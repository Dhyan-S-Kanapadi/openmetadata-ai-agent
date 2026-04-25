import json

import tools
from agent import run_agent
from mcp.server.fastmcp import FastMCP


mcp = FastMCP(
    name="OpenMetadata Workflow Agent",
    instructions=(
        "An MCP server that lets you manage OpenMetadata through natural "
        "language using Cerebras Llama and LangGraph"
    ),
)


@mcp.tool()
def ask_agent(question: str) -> str:
    """
    Ask the OpenMetadata AI agent anything about your data platform.
    Examples:
    - Which tables have no owners?
    - Show data quality failures
    - What is the lineage of orders table?
    - Trigger the Snowflake pipeline
    """
    return run_agent(question)


@mcp.tool()
def list_tables() -> str:
    """Get all tables from OpenMetadata."""
    return json.dumps(tools.get_tables())


@mcp.tool()
def list_unowned_tables() -> str:
    """Get all tables with no owner assigned."""
    return json.dumps(tools.get_unowned_tables())


@mcp.tool()
def list_undocumented_tables() -> str:
    """Get all tables with no description."""
    return json.dumps(tools.get_undocumented_tables())


@mcp.tool()
def list_pipelines() -> str:
    """Get all ingestion pipelines and their status."""
    return json.dumps(tools.get_pipelines())


@mcp.tool()
def trigger_pipeline(pipeline_id: str) -> str:
    """
    Trigger an OpenMetadata ingestion pipeline by ID.
    Args:
        pipeline_id: The ingestion pipeline ID returned by list_pipelines.
    """
    return json.dumps(tools.trigger_pipeline(pipeline_id))


@mcp.tool()
def list_quality_failures() -> str:
    """Get all failed data quality tests."""
    return json.dumps(tools.get_quality_failures())


@mcp.tool()
def list_quality_tests(status: str | None = None, limit: int = 50) -> str:
    """
    Get recent data quality test cases from OpenMetadata.
    Args:
        status: Optional status filter such as Failed, Success, or Unprocessed.
        limit: Maximum number of test cases to return.
    """
    return json.dumps(tools.get_quality_tests(limit=limit, status=status))


@mcp.tool()
def get_quality_summary(limit: int = 50) -> str:
    """
    Get data quality test coverage, status counts, failed tests, and recent tests.
    Args:
        limit: Maximum number of recent test cases to include.
    """
    return json.dumps(tools.get_quality_summary(limit=limit))


@mcp.tool()
def get_table_lineage(
    table_fqn: str,
    upstream_depth: int = 2,
    downstream_depth: int = 2,
) -> str:
    """
    Get lineage of a specific table.
    Args:
        table_fqn: Fully qualified table name, for example
            sample_data.ecommerce_db.shopify.orders.
        upstream_depth: Number of upstream hops to fetch, from 0 to 5.
        downstream_depth: Number of downstream hops to fetch, from 0 to 5.
    """
    return json.dumps(
        tools.get_lineage(
            table_fqn,
            upstream_depth=upstream_depth,
            downstream_depth=downstream_depth,
        )
    )


if __name__ == "__main__":
    mcp.run()
