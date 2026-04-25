import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import anyio
import mcp.types as types
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.message import SessionMessage


PROJECT_ROOT = Path(__file__).resolve().parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
SERVER_STDERR: list[str] = []


@asynccontextmanager
async def popen_stdio_client(
    server: StdioServerParameters,
) -> AsyncIterator[tuple[Any, Any]]:
    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    process = subprocess.Popen(
        [server.command, *server.args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=server.cwd,
        env={**os.environ, **(server.env or {})},
        bufsize=0,
    )

    async def stdout_reader() -> None:
        assert process.stdout is not None
        async with read_stream_writer:
            while True:
                line = await anyio.to_thread.run_sync(process.stdout.readline)
                if not line:
                    break
                message = types.JSONRPCMessage.model_validate_json(
                    line.decode(server.encoding, errors=server.encoding_error_handler)
                )
                await read_stream_writer.send(SessionMessage(message))

    async def stderr_reader() -> None:
        assert process.stderr is not None
        while True:
            line = await anyio.to_thread.run_sync(process.stderr.readline)
            if not line:
                break
            SERVER_STDERR.append(
                line.decode(server.encoding, errors="replace").rstrip()
            )

    async def stdin_writer() -> None:
        assert process.stdin is not None
        async with write_stream_reader:
            async for session_message in write_stream_reader:
                payload = session_message.message.model_dump_json(
                    by_alias=True,
                    exclude_none=True,
                )

                def write_payload() -> None:
                    assert process.stdin is not None
                    process.stdin.write((payload + "\n").encode(server.encoding))
                    process.stdin.flush()

                await anyio.to_thread.run_sync(write_payload)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(stdout_reader)
        task_group.start_soon(stdin_writer)
        task_group.start_soon(stderr_reader)
        try:
            yield read_stream, write_stream
        finally:
            if process.stdin:
                process.stdin.close()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.terminate()
            task_group.cancel_scope.cancel()
            await read_stream.aclose()
            await write_stream.aclose()


def print_section(title: str) -> None:
    width = 72
    print()
    print("=" * width)
    print(title)
    print("=" * width)


def extract_tool_text(result: Any) -> str:
    parts: list[str] = []
    for item in result.content:
        text = getattr(item, "text", None)
        if text is not None:
            parts.append(text)
    return "\n".join(parts).strip()


def parse_tool_json(result: Any) -> Any:
    text = extract_tool_text(result)
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def shorten(value: Any, limit: int = 52) -> str:
    if value is None:
        text = "-"
    elif isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)

    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def print_tables(tables: Any) -> None:
    print_section("TABLES FROM OPENMETADATA")
    if not tables:
        print("No tables returned.")
        return
    if not isinstance(tables, list):
        print(json.dumps(tables, indent=2, ensure_ascii=False))
        return

    print(f"{'#':<4} {'Name':<52} {'Owner':<20} Description")
    print("-" * 100)
    for index, table in enumerate(tables, start=1):
        print(
            f"{index:<4} "
            f"{shorten(table.get('name'), 52):<52} "
            f"{shorten(table.get('owner'), 20):<20} "
            f"{shorten(table.get('description'), 24)}"
        )


def print_pipelines(pipelines: Any) -> None:
    print_section("INGESTION PIPELINES FROM OPENMETADATA")
    if not pipelines:
        print("No pipelines returned.")
        return
    if not isinstance(pipelines, list):
        print(json.dumps(pipelines, indent=2, ensure_ascii=False))
        return

    print(f"{'#':<4} {'Name':<36} {'ID':<38} Status")
    print("-" * 100)
    for index, pipeline in enumerate(pipelines, start=1):
        print(
            f"{index:<4} "
            f"{shorten(pipeline.get('name'), 36):<36} "
            f"{shorten(pipeline.get('id'), 38):<38} "
            f"{shorten(pipeline.get('status'), 20)}"
        )


def print_quality_summary(summary: Any) -> None:
    print_section("DATA QUALITY FROM OPENMETADATA")
    if not summary:
        print("No data quality summary returned.")
        return
    if not isinstance(summary, dict):
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    print(f"Total tests: {summary.get('total_tests', 0)}")
    print(f"Status counts: {shorten(summary.get('status_counts', {}), 80)}")

    failed_tests = summary.get("failed_tests", [])
    if failed_tests:
        print()
        print("Failed tests:")
        for index, test_case in enumerate(failed_tests, start=1):
            print(
                f"{index:<4} "
                f"{shorten(test_case.get('name'), 42):<42} "
                f"{shorten(test_case.get('entity'), 42):<42} "
                f"{shorten(test_case.get('status'), 14)}"
            )
        return

    recent_tests = summary.get("recent_tests", [])
    if not recent_tests:
        print("No data quality tests returned.")
        return

    print()
    print("Recent tests:")
    print(f"{'#':<4} {'Name':<42} {'Entity':<42} Status")
    print("-" * 100)
    for index, test_case in enumerate(recent_tests[:10], start=1):
        print(
            f"{index:<4} "
            f"{shorten(test_case.get('name'), 42):<42} "
            f"{shorten(test_case.get('entity'), 42):<42} "
            f"{shorten(test_case.get('status'), 14)}"
        )


async def call_json_tool(
    session: ClientSession,
    tool_name: str,
    arguments: dict[str, Any] | None = None,
) -> Any:
    result = await session.call_tool(tool_name, arguments=arguments or {})
    if result.isError:
        message = extract_tool_text(result) or "The server returned an MCP tool error."
        raise RuntimeError(f"{tool_name} failed: {message}")
    return parse_tool_json(result)


def print_tool_health(results: dict[str, Any]) -> None:
    print_section("MCP TOOL HEALTH")
    for tool_name, value in results.items():
        if isinstance(value, list):
            detail = f"{len(value)} records"
        elif isinstance(value, dict):
            detail = f"{len(value)} top-level fields"
        else:
            detail = shorten(value, 70)
        print(f"[OK] {tool_name}: {detail}")


async def main() -> int:
    python_executable = str(VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable))
    server = StdioServerParameters(
        command=python_executable,
        args=["server.py"],
        cwd=str(PROJECT_ROOT),
    )

    try:
        try:
            transport = stdio_client(server)
            read_stream, write_stream = await transport.__aenter__()
        except PermissionError:
            transport = popen_stdio_client(server)
            read_stream, write_stream = await transport.__aenter__()

        try:
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()

                tools = await session.list_tools()
                available = {tool.name for tool in tools.tools}
                required = {
                    "ask_agent",
                    "get_quality_summary",
                    "get_table_lineage",
                    "list_pipelines",
                    "list_quality_failures",
                    "list_quality_tests",
                    "list_tables",
                    "list_undocumented_tables",
                    "list_unowned_tables",
                    "trigger_pipeline",
                }
                missing = sorted(required - available)
                if missing:
                    print("MCP server is running, but expected tools are missing:")
                    print(", ".join(missing))
                    print(f"Available tools: {', '.join(sorted(available))}")
                    return 1

                print_section("OPENMETADATA WORKFLOW AGENT MCP DEMO")
                print("Connected to FastMCP server over stdio.")
                print(f"Available tools: {', '.join(sorted(available))}")

                tables = await call_json_tool(session, "list_tables")
                print_tables(tables)

                unowned_tables = await call_json_tool(session, "list_unowned_tables")
                undocumented_tables = await call_json_tool(
                    session, "list_undocumented_tables"
                )

                pipelines = await call_json_tool(session, "list_pipelines")
                print_pipelines(pipelines)

                quality_summary = await call_json_tool(session, "get_quality_summary")
                print_quality_summary(quality_summary)
                quality_tests = await call_json_tool(
                    session,
                    "list_quality_tests",
                    {"status": "Unprocessed", "limit": 5},
                )
                quality_failures = await call_json_tool(session, "list_quality_failures")

                lineage = {}
                if isinstance(tables, list) and tables:
                    table_name = tables[0].get("name")
                    if table_name:
                        lineage = await call_json_tool(
                            session,
                            "get_table_lineage",
                            {"table_fqn": table_name},
                        )

                print_tool_health(
                    {
                        "list_tables": tables,
                        "list_unowned_tables": unowned_tables,
                        "list_undocumented_tables": undocumented_tables,
                        "list_pipelines": pipelines,
                        "get_quality_summary": quality_summary,
                        "list_quality_tests": quality_tests,
                        "list_quality_failures": quality_failures,
                        "get_table_lineage": lineage,
                        "trigger_pipeline": "registered; not called by demo to avoid starting a pipeline",
                        "ask_agent": "registered; use for natural-language questions",
                    }
                )

                print_section("DONE")
                print("MCP demo completed successfully.")
                return 0
        finally:
            await transport.__aexit__(None, None, None)
    except FileNotFoundError as exc:
        print("Could not start the MCP server process.")
        print(f"Details: {exc}")
        return 1
    except Exception as exc:
        print("Could not connect to or call the MCP server cleanly.")
        print("Make sure dependencies are installed and OpenMetadata settings are valid.")
        if SERVER_STDERR:
            print("Server error:")
            print("\n".join(SERVER_STDERR[-8:]))
        else:
            print(f"Details: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(anyio.run(main))
