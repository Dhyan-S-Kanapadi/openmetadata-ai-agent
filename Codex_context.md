# DataSentinel Project Context

This file is the working context for Codex and teammates. It reflects the current repository state after upgrading the hackathon project from OpenMetadata Workflow Agent toward DataSentinel.

## Project Identity

- Current product name: DataSentinel
- Repository name: `openmetadata-workflow-agent`
- Hackathon origin: Back to the Metadata, WeMakeDevs x OpenMetadata
- Track: T-01 MCP Ecosystem and AI Agents
- Team: AryaBit
- Development OS: Windows
- Primary runtime: Python with project virtual environment at `.venv`

## Current Goal

DataSentinel should be both:

- a conversational OpenMetadata workflow agent
- an autonomous data platform monitoring engine

The conversational agent lets users ask natural-language questions about OpenMetadata. The monitoring engine scans OpenMetadata on its own, calculates platform health, detects anomalies, stores findings, and can trigger failed pipelines.

## Current Architecture

```text
User / Scheduler / MCP Client
        |
        +--> app.py ----> agent.py ----+
        |                              |
        +--> server.py ----------------+--> tools.py --> OpenMetadata API
        |                              |
        +--> monitor.py ---------------+
```

## File Responsibilities

| File | Responsibility |
| --- | --- |
| `config.py` | Loads environment variables from `.env` |
| `tools.py` | OpenMetadata API wrapper functions |
| `agent.py` | LangGraph + Cerebras natural-language agent |
| `app.py` | Streamlit DataSentinel dashboard with monitor controls and assistant tab |
| `server.py` | FastMCP server exposing agent and direct OpenMetadata tools |
| `monitor.py` | DataSentinel autonomous monitoring engine |
| `policy.py` | Decides auto-fix vs escalation behavior |
| `notifier.py` | Sends customer alerts through webhook/email channels |
| `worker.py` | Long-running scheduler process for autonomous monitoring |
| `test.py` | OpenMetadata connection smoke test |
| `requirements.txt` | Project dependencies |
| `data/` | Generated monitoring reports |

## Environment Variables

Required for the agent:

```env
CEREBRAS_API_KEY=your-cerebras-api-key
```

Required for OpenMetadata:

```env
OPENMETADATA_URL=https://sandbox.open-metadata.org/api/v1
```

Authentication options:

```env
OPENMETADATA_JWT_TOKEN=your-openmetadata-jwt-token
```

or:

```env
OPENMETADATA_EMAIL=your-email
OPENMETADATA_PASSWORD=your-password
```

Optional customer alerting:

```env
DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url
DATASENTINEL_EMAIL_TO=customer@example.com
DATASENTINEL_EMAIL_HOST=smtp.example.com
DATASENTINEL_EMAIL_PORT=587
DATASENTINEL_EMAIL_USER=your-smtp-user
DATASENTINEL_EMAIL_PASSWORD=your-smtp-password
DATASENTINEL_EMAIL_FROM=datasentinel@example.com
```

Do not commit `.env`.

## Dependency List

Current `requirements.txt`:

```text
requests
python-dotenv
streamlit
mcp
langgraph
langchain-core
langchain-cerebras
APScheduler
```

## OpenMetadata Tool Surface

`tools.py` currently provides:

- `get_tables(limit=20)`
- `get_unowned_tables()`
- `get_undocumented_tables()`
- `get_pipelines()`
- `trigger_pipeline(pipeline_id)`
- `get_quality_failures()`
- `get_lineage(table_fqn)`

The tools support JWT auth through `OPENMETADATA_JWT_TOKEN`, or email/password login when supported by the OpenMetadata server.

## Conversational Agent Status

`agent.py` uses:

- `ChatCerebras`
- model `llama3.1-8b`
- LangGraph `StateGraph`
- LangChain tool wrappers around `tools.py`

The previous raw tool-call JSON issue has been addressed. The agent now includes parsing helpers for provider-specific tool call formats and extracts the final natural-language `AIMessage` content.

Main public function:

```python
from agent import run_agent

answer = run_agent("Which tables have no owners?")
```

## DataSentinel Monitoring Status

`monitor.py` is implemented and verified. It now uses LangGraph, matching the
architecture style used by `agent.py`.

Main public function:

```python
from monitor import run_monitoring_cycle

report = run_monitoring_cycle()
```

It performs:

- OpenMetadata scanning through existing `tools.py` functions
- health score calculation from `0` to `100`
- anomaly detection with `critical`, `warning`, and `info` severities
- autonomous failed-pipeline trigger actions
- policy-based auto-fix vs escalation decisions
- customer notifications through webhook or email
- timestamped report persistence
- APScheduler-compatible scheduled execution

Monitor graph nodes:

```text
initialize
scan_openmetadata
detect_anomalies
autonomous_actions
score_health
decide_policy
notify_customer
build_report
persist_report
```

Default output files:

```text
data/monitor_findings.jsonl
data/monitor_latest.json
```

Scheduler functions:

```python
from monitor import create_scheduler, start_scheduler

scheduler = create_scheduler(interval_minutes=15)
scheduler = start_scheduler(interval_minutes=15)
```

Important behavior:

- Failed quality tests are `critical`.
- Failed pipelines are `critical`.
- Unowned tables are `warning`.
- Monitoring scan errors are `warning`.
- Undocumented tables are `info`.
- Empty inventory is `info` only when the table scan actually succeeds and returns no tables.
- If `get_tables()` fails, the monitor records a scan error instead of pretending the platform has no tables.
- Successful failed-pipeline retriggers are recorded as autonomous remediation.
- Data quality failures require manual intervention and customer notification.
- Failed, skipped, or unknown remediation actions require customer notification.
- If no notification channel is configured, the notification action is recorded as `skipped`.

## Autonomous Worker

Run DataSentinel continuously:

```powershell
python worker.py --interval-minutes 1
```

Useful worker flags:

```text
--interval-minutes N   Run every N minutes
--no-immediate-run     Wait until the first scheduled interval
--no-auto-trigger      Disable failed-pipeline remediation
--no-notify            Disable customer notifications
```

## Health Scoring

`calculate_health_score()` starts from `100` and subtracts capped penalties:

- failed pipelines
- quality failures
- unowned table ratio
- undocumented table ratio
- scan errors

The result is clamped between `0` and `100`.

Report status mapping:

```text
0-49    critical
50-79   warning
80-100  healthy
```

## Verified Behavior

Verified with the project `.venv`:

- `monitor.py` compiles.
- `notifier.py`, `policy.py`, and `worker.py` compile.
- scheduler creation works.
- clean mocked platform returns `100 healthy`.
- degraded mocked platform returns expected warning score and findings.
- failed pipeline action calls `tools.trigger_pipeline()`.
- successful failed-pipeline remediation creates an informational notification decision.
- critical data quality failure creates a manual-intervention notification decision.
- webhook notification success is recorded as a `notify_customer` action.
- scan-error case records one monitoring warning and avoids misleading inventory findings.
- reports are written to JSONL and latest JSON.

## How To Run

Activate the virtual environment:

```powershell
.venv\Scripts\activate
```

Run Streamlit:

```powershell
streamlit run app.py
```

The UI is monitoring-first:

- Monitor tab: health score, findings, actions, raw report, Safe Scan, Live Autonomous Scan
- Operations tab: worker commands and customer alert configuration
- Assistant tab: legacy natural-language OpenMetadata investigation

Run MCP server:

```powershell
python server.py
```

Run OpenMetadata connection test:

```powershell
python test.py
```

Run one monitoring cycle:

```powershell
python monitor.py
```

Run the autonomous worker:

```powershell
python worker.py --interval-minutes 1
```

## Development Rules

- Do not commit `.env`.
- Keep `tools.py` as the shared OpenMetadata access layer.
- Keep autonomous monitoring logic in `monitor.py`.
- Keep notification delivery logic in `notifier.py`.
- Keep auto-fix and escalation decisions in `policy.py`.
- Keep conversational logic in `agent.py`.
- Generated files in `data/` are runtime artifacts.
- Prefer adding tests or smoke checks before changing monitor behavior.

## Current Project State

Completed:

- OpenMetadata API wrapper functions
- Streamlit DataSentinel dashboard with assistant tab
- LangGraph + Cerebras agent
- FastMCP server
- DataSentinel `monitor.py`
- Policy layer for auto-fix vs manual escalation
- Customer notifier with webhook/email support
- Long-running `worker.py`
- APScheduler integration in `monitor.py`
- `APScheduler` added to dependencies
- Markdown documentation refreshed for the DataSentinel upgrade

Likely next steps:

- Add a Streamlit dashboard view for `data/monitor_latest.json`
- Add MCP tools for monitoring reports
- Add unit tests for `monitor.py`
- Add richer remediation actions for owner assignment and documentation drafts
