# DataSentinel

DataSentinel is an autonomous OpenMetadata monitoring and workflow agent for data teams. It combines a natural-language assistant with a production-style monitoring engine that scans platform health, detects governance and quality issues, and can take autonomous remediation actions such as retriggering failed ingestion pipelines.

The project started as `openmetadata-workflow-agent` for the Back to the Metadata hackathon and is being upgraded into DataSentinel.

## What It Does

- Monitors OpenMetadata tables, ownership, documentation, ingestion pipelines, quality failures, and lineage.
- Calculates a deterministic platform health score from `0` to `100`.
- Categorizes anomalies as `critical`, `warning`, or `info`.
- Triggers failed ingestion pipelines automatically when configured.
- Uses a policy layer to decide what can be fixed safely and what must be escalated.
- Sends customer notifications through webhook or email when manual intervention is needed.
- Stores timestamped monitoring reports in JSONL and latest-report JSON format.
- Provides a Streamlit DataSentinel dashboard with monitoring controls, report history, and a secondary assistant tab.
- Exposes an MCP server for compatible clients.
- Can run monitoring manually or on a schedule through APScheduler.

## Architecture

```text
Streamlit UI / MCP Client / Manual Python Call
        |
        v
agent.py / monitor.py
        |
        v
tools.py
        |
        v
OpenMetadata API
```

The project has two main paths:

- Conversational workflow path: `app.py` -> `agent.py` -> `tools.py` -> OpenMetadata.
- Autonomous monitoring path: `monitor.py` -> `tools.py` -> OpenMetadata.

## Key Files

```text
openmetadata-workflow-agent/
+-- app.py                 Streamlit DataSentinel dashboard and assistant
+-- agent.py               LangGraph + Cerebras natural-language agent
+-- monitor.py             DataSentinel autonomous monitoring engine
+-- policy.py              Auto-fix and escalation decision policy
+-- notifier.py            Customer webhook/email notification helpers
+-- worker.py              Long-running scheduled monitoring process
+-- tools.py               OpenMetadata API functions
+-- server.py              FastMCP server
+-- config.py              Environment-based configuration
+-- test.py                OpenMetadata connection smoke test
+-- requirements.txt       Python dependencies
+-- data/                  Generated monitoring reports
+-- .env                   Local secrets, not committed
+-- .gitignore
```

## Monitoring Engine

`monitor.py` is the DataSentinel monitoring engine. Internally it is a LangGraph
workflow with nodes for scanning, anomaly detection, remediation, policy,
notification, report building, and persistence. It can be called directly:

```python
from monitor import run_monitoring_cycle

report = run_monitoring_cycle()
print(report.health_score)
print(report.status)
```

By default it writes:

```text
data/monitor_findings.jsonl
data/monitor_latest.json
```

The monitoring engine checks:

- unowned tables
- undocumented tables
- failed data quality tests
- failed ingestion pipelines
- OpenMetadata scan errors

Each run follows this lifecycle:

```text
Detect -> Classify -> Decide -> Act -> Notify -> Record
```

Severity mapping:

```text
critical  failed quality tests, failed pipelines
warning   unowned tables, monitoring scan errors
info      undocumented tables, empty table inventory
```

Failed pipelines are retriggered automatically unless disabled:

```python
from monitor import run_monitoring_cycle

report = run_monitoring_cycle(trigger_failed_pipelines=False)
```

Policy behavior:

```text
Auto-fixable: failed ingestion pipelines
Manual review: data quality failures, monitoring scan failures, failed remediation
Informational: successful autonomous remediation summaries
```

## Scheduled Monitoring

DataSentinel can run automatically with APScheduler:

```python
from monitor import start_scheduler

scheduler = start_scheduler(interval_minutes=15)
```

Or create the scheduler without starting it:

```python
from monitor import create_scheduler

scheduler = create_scheduler(interval_minutes=15)
```

The project `.venv` has APScheduler available, and `requirements.txt` includes it for fresh installs.

For a real always-on process, run the worker:

```powershell
python worker.py --interval-minutes 1
```

The worker keeps the scheduler alive until stopped with `Ctrl+C`.

## Customer Notifications

DataSentinel does not require customers to keep refreshing the app. When policy decides an issue needs attention, `notifier.py` can send alerts through configured channels.

Webhook alerts:

```env
DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url
```

Email alerts:

```env
DATASENTINEL_EMAIL_TO=customer@example.com
DATASENTINEL_EMAIL_HOST=smtp.example.com
DATASENTINEL_EMAIL_PORT=587
DATASENTINEL_EMAIL_USER=your-smtp-user
DATASENTINEL_EMAIL_PASSWORD=your-smtp-password
DATASENTINEL_EMAIL_FROM=datasentinel@example.com
```

If no notification channel is configured, the alert attempt is recorded as `skipped` in the monitoring report instead of crashing the monitor.

## Agent Tools

The natural-language agent can use these OpenMetadata functions:

| Tool | Purpose |
| --- | --- |
| `get_tables` | Fetch tables from OpenMetadata |
| `get_unowned_tables` | Find tables with no owner |
| `get_undocumented_tables` | Find tables with no description |
| `get_pipelines` | List ingestion pipelines and status |
| `trigger_pipeline` | Trigger a pipeline by ID |
| `get_quality_failures` | Fetch failed data quality test cases |
| `get_lineage` | Fetch upstream and downstream lineage |

## Environment Variables

Create a `.env` file in the project root:

```env
CEREBRAS_API_KEY=your-cerebras-api-key
OPENMETADATA_URL=https://sandbox.open-metadata.org/api/v1

# Use either JWT auth:
OPENMETADATA_JWT_TOKEN=your-openmetadata-jwt-token

# Or email/password auth if your OpenMetadata server allows it:
OPENMETADATA_EMAIL=your-email
OPENMETADATA_PASSWORD=your-password

# Optional customer alerting:
DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url
```

Do not commit `.env`.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run

Run the Streamlit dashboard:

```powershell
streamlit run app.py
```

The Streamlit app opens on the Monitor tab. Use **Safe Scan** to inspect the
platform without triggering pipelines or sending notifications, and use **Live
Autonomous Scan** when you want DataSentinel to apply configured remediation and
alerting behavior. The old natural-language assistant is still available under
the Assistant tab for investigation.

Run the MCP server:

```powershell
python server.py
```

Run the OpenMetadata connection smoke test:

```powershell
python test.py
```

Run one DataSentinel monitoring cycle:

```powershell
python monitor.py
```

Run the autonomous worker every minute:

```powershell
python worker.py --interval-minutes 1
```

## Example Questions

```text
Which tables have no owners?
Which tables are undocumented?
Show data quality failures.
Show all ingestion pipelines.
What is the lineage of sample_data.ecommerce_db.shopify.orders?
Trigger pipeline <pipeline-id>.
```

## Verification Status

Current verified behavior:

- `monitor.py` compiles successfully.
- Manual monitoring works through `run_monitoring_cycle()`.
- APScheduler job creation works in the project `.venv`.
- Health scoring works for clean, degraded, and scan-error cases.
- Failed pipeline detection and autonomous trigger behavior work in mocked verification.
- Policy decisions separate auto-fixed issues from manual-intervention issues.
- Webhook notification success and skipped-notification behavior are verified with mocks.
- Monitoring reports are persisted with timestamps.
- The agent has robust tool-call parsing and final-answer extraction.

## Hackathon Context

- Event: Back to the Metadata, WeMakeDevs x OpenMetadata
- Track: T-01 MCP Ecosystem and AI Agents
- Team: AryaBit
- Core sponsors used: OpenMetadata, Cerebras, MCP

## Security

- Secrets are loaded from `.env`.
- `.env` and `.venv/` are ignored by Git.
- Generated `data/*.json` and `data/*.jsonl` monitoring reports are ignored by Git.
- No API keys should be hardcoded in source files or documentation.
