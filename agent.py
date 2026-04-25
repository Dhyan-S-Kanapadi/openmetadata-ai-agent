import json
from typing import Annotated

from langchain_cerebras import ChatCerebras
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

import tools
from config import CEREBRAS_API_KEY


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


@tool
def get_tables_tool() -> str:
    """Get all tables from OpenMetadata platform"""
    result = tools.get_tables()
    return json.dumps(result)


@tool
def get_unowned_tables_tool() -> str:
    """Get all tables that have no owner assigned in OpenMetadata"""
    result = tools.get_unowned_tables()
    return json.dumps(result)


@tool
def get_undocumented_tables_tool() -> str:
    """Get all tables that have no description in OpenMetadata"""
    result = tools.get_undocumented_tables()
    return json.dumps(result)


@tool
def get_pipelines_tool() -> str:
    """Get all ingestion pipelines and their current status"""
    result = tools.get_pipelines()
    return json.dumps(result)


@tool
def trigger_pipeline_tool(pipeline_id: str) -> str:
    """Trigger an ingestion pipeline by its ID in OpenMetadata"""
    result = tools.trigger_pipeline(pipeline_id)
    return json.dumps(result)


@tool
def get_quality_failures_tool() -> str:
    """Get all data quality test failures from OpenMetadata"""
    result = tools.get_quality_failures()
    return json.dumps(result)


@tool
def get_quality_tests_tool() -> str:
    """Get recent data quality test cases and their current status from OpenMetadata"""
    result = tools.get_quality_tests()
    return json.dumps(result)


@tool
def get_quality_summary_tool() -> str:
    """Get a summary of OpenMetadata data quality test coverage and status counts"""
    result = tools.get_quality_summary()
    return json.dumps(result)


@tool
def get_lineage_tool(
    table_fqn: str,
    upstream_depth: int = 2,
    downstream_depth: int = 2,
) -> str:
    """Get lineage of a table by its fully qualified name from OpenMetadata"""
    result = tools.get_lineage(
        table_fqn,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth,
    )
    return json.dumps(result)


TOOLS = [
    get_tables_tool,
    get_unowned_tables_tool,
    get_undocumented_tables_tool,
    get_pipelines_tool,
    trigger_pipeline_tool,
    get_quality_failures_tool,
    get_quality_tests_tool,
    get_quality_summary_tool,
    get_lineage_tool,
]

tool_map = {tool_obj.name: tool_obj for tool_obj in TOOLS}

llm = ChatCerebras(
    api_key=CEREBRAS_API_KEY,
    model="llama3.1-8b",
)
llm_with_tools = llm.bind_tools(TOOLS)

SYSTEM_MESSAGE = SystemMessage(
    content=(
        "You are an intelligent OpenMetadata assistant. "
        "You help data teams manage their data platform by "
        "querying pipelines, data quality, lineage, and governance. "
        "Use the available tools to answer questions accurately. "
        "For multi-topic questions, call every relevant read-only tool before "
        "summarizing. For example, a health summary should check tables, "
        "pipelines, and data quality instead of answering from one category. "
        "Always provide clear and concise answers in plain text."
    )
)


def _extract_text_from_message(message: AIMessage) -> str | None:
    if isinstance(message.content, str):
        content = message.content.strip()
        return content or None

    if isinstance(message.content, list):
        text_parts = []
        for block in message.content:
            if isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text", "").strip()
                if text:
                    text_parts.append(text)
        if text_parts:
            return "\n".join(text_parts)

    return None


def _safe_json_loads(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _parse_tool_calls_from_ai_message(message: AIMessage) -> list[dict]:
    if getattr(message, "tool_calls", None):
        return list(message.tool_calls)

    raw_function_call = message.additional_kwargs.get("function_call")
    if raw_function_call:
        raw_args = _safe_json_loads(raw_function_call.get("arguments", {}))
        return [
            {
                "id": message.id or "function_call",
                "name": raw_function_call["name"],
                "args": raw_args if isinstance(raw_args, dict) else {},
            }
        ]

    raw_text = _extract_text_from_message(message)
    if not raw_text:
        return []

    payload = _safe_json_loads(raw_text)
    if not isinstance(payload, dict):
        return []
    if payload.get("type") != "function" or "name" not in payload:
        return []

    arguments = _safe_json_loads(payload.get("arguments", {}))
    return [
        {
            "id": payload.get("id") or message.id or payload["name"],
            "name": payload["name"],
            "args": arguments if isinstance(arguments, dict) else {},
        }
    ]


def _execute_tool_calls(tool_calls: list[dict]) -> list[ToolMessage]:
    tool_messages = []
    for tool_call in tool_calls:
        tool_name = tool_call["name"]
        tool_args = tool_call.get("args", {})

        try:
            result = tool_map[tool_name].invoke(tool_args)
            status = "success"
        except Exception as exc:
            result = f"Error calling tool: {exc}"
            status = "error"

        tool_messages.append(
            ToolMessage(
                content=str(result),
                tool_call_id=tool_call["id"],
                status=status,
            )
        )

    return tool_messages


def _extract_final_answer(messages: list) -> str | None:
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        if _parse_tool_calls_from_ai_message(message):
            continue
        text = _extract_text_from_message(message)
        if text:
            return text
    return None


def call_llm(state: AgentState):
    messages = [SYSTEM_MESSAGE] + state["messages"]
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def call_tools(state: AgentState):
    last_message = state["messages"][-1]
    tool_calls = _parse_tool_calls_from_ai_message(last_message)
    return {"messages": _execute_tool_calls(tool_calls)}


def should_continue(state: AgentState):
    last_message = state["messages"][-1]
    if isinstance(last_message, AIMessage) and _parse_tool_calls_from_ai_message(last_message):
        return "call_tools"
    return END


def build_agent():
    graph = StateGraph(AgentState)
    graph.add_node("call_llm", call_llm)
    graph.add_node("call_tools", call_tools)
    graph.set_entry_point("call_llm")
    graph.add_conditional_edges("call_llm", should_continue)
    graph.add_edge("call_tools", "call_llm")
    return graph.compile()


agent = build_agent()


def run_agent(user_message: str) -> str:
    result = agent.invoke(
        {
            "messages": [HumanMessage(content=user_message)],
        }
    )
    messages = list(result["messages"])

    final_answer = _extract_final_answer(messages)
    if final_answer:
        return final_answer

    # Fallback for providers that return the tool request as plain JSON text.
    for _ in range(5):
        last_message = messages[-1]
        if not isinstance(last_message, AIMessage):
            break

        tool_calls = _parse_tool_calls_from_ai_message(last_message)
        if not tool_calls:
            break

        messages.extend(_execute_tool_calls(tool_calls))
        follow_up = llm_with_tools.invoke([SYSTEM_MESSAGE] + messages)
        messages.append(follow_up)

        final_answer = _extract_final_answer(messages)
        if final_answer:
            return final_answer

    return "Sorry I could not get an answer. Please try again."
