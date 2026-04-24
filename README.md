# DataSentinel

**An autonomous OpenMetadata monitoring and workflow agent for data teams.**

DataSentinel combines a natural-language assistant with a production-grade monitoring engine that scans platform health, detects anomalies, applies safe remediation policies, and sends alerts—all autonomously. It originated as `openmetadata-workflow-agent` for the Back to the Metadata hackathon and is now evolved into DataSentinel.

## Project Overview

- **Repository**: Dhyan-S-Kanapadi/DataSentinel
- **Language**: Python (100%)
- **Created**: April 18, 2026
- **Status**: Active Development
- **License**: Not specified
- **Visibility**: Public
- **Size**: 13 KB

## What It Does

- **Monitors OpenMetadata** for tables, ownership, documentation, ingestion pipelines, quality failures, and lineage.
- **Calculates Platform Health**: Generates a deterministic health score from 0 to 100.
- **Categorizes Anomalies**: Classifies issues as `critical`, `warning`, or `info`.
- **Auto-Remediation**: Automatically triggers failed ingestion pipelines when configured.
- **Policy Layer**: Decides what can be fixed safely and what must be escalated to teams.
- **Customer Notifications**: Sends alerts through webhook or email when manual intervention is needed.
- **Report Storage**: Stores timestamped monitoring reports in JSONL and latest-report JSON formats.
- **Streamlit Dashboard**: Provides a comprehensive monitoring interface with controls, report history, and assistant capabilities.
- **MCP Server**: Exposes an MCP (Model Context Protocol) server for compatible clients.
- **Flexible Scheduling**: Runs monitoring manually or on a schedule via APScheduler.

## Architecture

```
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

### Two Main Execution Paths

1. **Conversational Workflow**: `app.py` → `agent.py` → `tools.py` → OpenMetadata
2. **Autonomous Monitoring**: `monitor.py` → `tools.py` → OpenMetadata

## Key Files & Structure

```
DataSentinel/
├── app.py                  # Streamlit DataSentinel dashboard and assistant
├── agent.py                # LangGraph + Cerebras natural-language agent
├── monitor.py              # DataSentinel autonomous monitoring engine
├── policy.py               # Auto-fix and escalation decision policy layer
├── notifier.py             # Customer webhook/email notification helpers
├── worker.py               # Long-running scheduled monitoring process
├── tools.py                # OpenMetadata API functions
├── server.py               # FastMCP server for compatible clients
├── config.py               # Environment-based configuration
├── test.py                 # OpenMetadata connection smoke test
├── requirements.txt        # Python dependencies
├── data/                   # Generated monitoring reports (gitignored)
├── .env                    # Local secrets (not committed)
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## Monitoring Engine

`monitor.py` is the core autonomous monitoring engine. It's built on LangGraph with nodes for scanning, anomaly detection, remediation, policy evaluation, notification, report building, and persistence.

### Basic Usage

```python
from monitor import run_monitoring_cycle

report = run_monitoring_cycle()
print(report.health_score)
print(report.status)
```

### Monitoring Checks

The engine checks for:
- Unowned tables
- Undocumented tables
- Failed data quality tests
- Failed ingestion pipelines
- OpenMetadata scan errors

### Monitoring Lifecycle

```
Detect → Classify → Decide → Act → Notify → Record
```

### Severity Mapping

| Severity | Issues |
|----------|--------|
| **critical** | Failed quality tests, failed pipelines |
| **warning** | Unowned tables, monitoring scan errors |
| **info** | Undocumented tables, empty inventory |

### Policy Behavior

| Category | Action |
|----------|--------|
| **Auto-fixable** | Failed ingestion pipelines |
| **Manual review** | Data quality failures, monitoring scan failures, failed remediation |
| **Informational** | Successful autonomous remediation summaries |

### Pipeline Auto-Trigger

Failed pipelines are retriggered automatically by default:

```python
from monitor import run_monitoring_cycle

# Disable auto-trigger if needed
report = run_monitoring_cycle(trigger_failed_pipelines=False)
```

## Scheduled Monitoring

### One-Time Scheduler Creation

```python
from monitor import create_scheduler

scheduler = create_scheduler(interval_minutes=15)
```

### Start Scheduler Immediately

```python
from monitor import start_scheduler

scheduler = start_scheduler(interval_minutes=15)
```

### Background Worker Process

For production always-on monitoring:

```bash
python worker.py --interval-minutes 1
```

Stop with `Ctrl+C`.

## Customer Notifications

DataSentinel can alert teams through configured notification channels when policy escalates issues.

### Webhook Alerts (Slack, Discord, etc.)

```env
DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url
```

### Email Alerts

```env
DATASENTINEL_EMAIL_TO=customer@example.com
DATASENTINEL_EMAIL_HOST=smtp.example.com
DATASENTINEL_EMAIL_PORT=587
DATASENTINEL_EMAIL_USER=your-smtp-user
DATASENTINEL_EMAIL_PASSWORD=your-smtp-password
DATASENTINEL_EMAIL_FROM=datasentinel@example.com
```

If no notification channel is configured, alert attempts are recorded as `skipped` in the monitoring report instead of crashing.

## Agent Tools

The natural-language agent has access to these OpenMetadata functions:

| Tool | Purpose |
|------|---------|
| `get_tables` | Fetch tables from OpenMetadata |
| `get_unowned_tables` | Find tables with no owner |
| `get_undocumented_tables` | Find tables with no description |
| `get_pipelines` | List ingestion pipelines and status |
| `trigger_pipeline` | Trigger a pipeline by ID |
| `get_quality_failures` | Fetch failed data quality test cases |
| `get_lineage` | Fetch upstream and downstream lineage |

## Environment Variables

Create a `.env` file in the project root with the following:

```env
# Cerebras AI API
CEREBRAS_API_KEY=your-cerebras-api-key

# OpenMetadata Connection
OPENMETADATA_URL=https://sandbox.open-metadata.org/api/v1

# Authentication: Use either JWT or email/password
OPENMETADATA_JWT_TOKEN=your-openmetadata-jwt-token
# OR
OPENMETADATA_EMAIL=your-email
OPENMETADATA_PASSWORD=your-password

# Optional: Customer Alerting
DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url
```

**Important**: Do not commit `.env` to version control.

## Setup Instructions

### 1. Create Virtual Environment

```bash
python -m venv .venv
```

### 2. Activate Virtual Environment

**Windows:**
```bash
.venv\Scripts\activate
```

**Linux/macOS:**
```bash
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## Running DataSentinel

### Run Streamlit Dashboard

```bash
streamlit run app.py
```

Opens the Monitor tab by default. Use:
- **Safe Scan** to inspect without triggering pipelines or sending notifications
- **Live Autonomous Scan** to apply configured remediation and alerting

The Assistant tab provides the legacy natural-language assistant for investigation.

### Run MCP Server

```bash
python server.py
```

### Test OpenMetadata Connection

```bash
python test.py
```

### Run Single Monitoring Cycle

```bash
python monitor.py
```

### Run Background Worker (Every Minute)

```bash
python worker.py --interval-minutes 1
```

## Example Questions for Agent

```
Which tables have no owners?
Which tables are undocumented?
Show data quality failures.
Show all ingestion pipelines.
What is the lineage of sample_data.ecommerce_db.shopify.orders?
Trigger pipeline <pipeline-id>.
```

## Output Files

Monitoring reports are generated in the `data/` directory:

- `data/monitor_findings.jsonl` – Timestamped monitoring reports
- `data/monitor_latest.json` – Latest monitoring report snapshot

Both are gitignored to prevent leaking sensitive data.

## Verification Status

Current verified capabilities:

✅ `monitor.py` compiles successfully
✅ Manual monitoring works via `run_monitoring_cycle()`
✅ APScheduler job creation in project `.venv`
✅ Health scoring for clean, degraded, and scan-error cases
✅ Failed pipeline detection and autonomous trigger behavior (mocked verification)
✅ Policy decisions separate auto-fixed from manual-intervention issues
✅ Webhook notification success and skipped-notification behavior (mocked)
✅ Monitoring reports persisted with timestamps
✅ Agent has robust tool-call parsing and final-answer extraction

## Hackathon Context

- **Event**: Back to the Metadata, WeMakeDevs x OpenMetadata
- **Track**: T-01 MCP Ecosystem and AI Agents
- **Team**: AryaBit
- **Core Sponsors**: OpenMetadata, Cerebras, MCP

## Security Best Practices

- Secrets are loaded from `.env` file
- `.env` and `.venv/` are gitignored
- Generated `data/*.json` and `data/*.jsonl` reports are gitignored
- No API keys are hardcoded in source files or documentation
- Always keep credentials secure and never share `.env` files

## Contributing

Contributions are welcome! Feel free to fork, create feature branches, and submit pull requests to improve DataSentinel.

## Support

For issues, questions, or feature requests, please open a GitHub issue in this repository.

---

**Last Updated**: April 24, 2026
**Repository**: [https://github.com/Dhyan-S-Kanapadi/DataSentinel](https://github.com/Dhyan-S-Kanapadi/DataSentinel)