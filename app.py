import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import streamlit as st

from agent import run_agent
from monitor import DEFAULT_LATEST_REPORT_PATH, run_monitoring_cycle


st.set_page_config(
    page_title="DataSentinel",
    page_icon="DS",
    layout="wide",
)


STATUS_COLORS = {
    "healthy": "#0f8a5f",
    "warning": "#b7791f",
    "critical": "#c53030",
    "unknown": "#4a5568",
}


def load_latest_report() -> dict[str, Any] | None:
    report_path = Path(DEFAULT_LATEST_REPORT_PATH)
    if not report_path.exists():
        return None
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        st.warning("Latest monitoring report could not be parsed.")
        return None


def normalize(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, list):
        return [normalize(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    return value


def render_status_pill(status: str) -> None:
    color = STATUS_COLORS.get(status, STATUS_COLORS["unknown"])
    st.markdown(
        f"""
        <div style="
            display:inline-block;
            padding:6px 12px;
            border-radius:6px;
            background:{color};
            color:white;
            font-size:13px;
            font-weight:700;
            text-transform:uppercase;
        ">{status}</div>
        """,
        unsafe_allow_html=True,
    )


def run_scan(*, trigger_failed_pipelines: bool, notify_customer: bool) -> None:
    with st.spinner("DataSentinel is scanning OpenMetadata..."):
        report = run_monitoring_cycle(
            trigger_failed_pipelines=trigger_failed_pipelines,
            notify_customer=notify_customer,
        )
    st.session_state.latest_report = normalize(report)
    st.success("Monitoring cycle completed.")


def render_metric_row(report: dict[str, Any]) -> None:
    summary = report.get("summary", {})
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Health Score", f"{report.get('health_score', 'NA')}/100")
    col2.metric("Status", str(report.get("status", "unknown")).title())
    col3.metric("Critical", summary.get("critical_findings", 0))
    col4.metric("Warnings", summary.get("warning_findings", 0))
    col5.metric("Actions", summary.get("actions_taken", 0))

    col6, col7, col8, col9 = st.columns(4)
    col6.metric("Tables", summary.get("tables_scanned", 0))
    col7.metric("Unowned", summary.get("unowned_tables", 0))
    col8.metric("Undocumented", summary.get("undocumented_tables", 0))
    col9.metric("Failed Pipelines", summary.get("failed_pipelines", 0))


def render_findings(report: dict[str, Any]) -> None:
    findings = report.get("findings", [])
    if not findings:
        st.info("No findings in the latest report.")
        return

    rows = [
        {
            "severity": finding.get("severity"),
            "category": finding.get("category"),
            "title": finding.get("title"),
            "entity": finding.get("entity"),
            "timestamp": finding.get("timestamp"),
        }
        for finding in findings
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)


def render_actions(report: dict[str, Any]) -> None:
    actions = report.get("actions", [])
    if not actions:
        st.info("No autonomous actions were taken in the latest report.")
        return

    rows = [
        {
            "type": action.get("action_type"),
            "target": action.get("target"),
            "status": action.get("status"),
            "message": action.get("message"),
            "timestamp": action.get("timestamp"),
        }
        for action in actions
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)


def render_dashboard() -> None:
    st.header("Autonomous Monitor")
    st.caption("DataSentinel fixes safe issues, escalates risky ones, and stores every run.")

    report = st.session_state.get("latest_report") or load_latest_report()
    if report:
        st.session_state.latest_report = report

    left, right = st.columns([2, 1])
    with left:
        if report:
            status = str(report.get("status", "unknown"))
            render_status_pill(status)
            st.write(f"Last run: `{report.get('timestamp', 'unknown')}`")
            render_metric_row(report)
        else:
            st.info("No monitoring report found yet. Run a scan to create one.")

    with right:
        st.subheader("Run A Scan")
        st.caption("Use safe mode first. Live mode may trigger failed pipelines and send configured alerts.")
        if st.button("Safe Scan", type="primary", use_container_width=True):
            run_scan(trigger_failed_pipelines=False, notify_customer=False)
            st.rerun()
        if st.button("Live Autonomous Scan", use_container_width=True):
            run_scan(trigger_failed_pipelines=True, notify_customer=True)
            st.rerun()

    if not report:
        return

    summary = report.get("summary", {})
    st.divider()
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Manual Intervention",
        "Required" if summary.get("manual_intervention_required") else "Not Required",
    )
    col2.metric(
        "Notification",
        "Required" if summary.get("notification_required") else "Not Required",
    )
    col3.metric("Notification Urgency", str(summary.get("notification_urgency", "info")).title())

    findings_tab, actions_tab, report_tab = st.tabs(["Findings", "Actions", "Raw Report"])
    with findings_tab:
        render_findings(report)
    with actions_tab:
        render_actions(report)
    with report_tab:
        st.json(report)


def render_operations() -> None:
    st.header("Operations")
    st.caption("Run DataSentinel continuously from a terminal for true autonomous monitoring.")

    st.code("python worker.py --interval-minutes 1", language="powershell")
    st.write("Safe testing mode:")
    st.code("python worker.py --interval-minutes 1 --no-auto-trigger --no-notify", language="powershell")

    st.subheader("Customer Alerts")
    st.write("Configure at least one channel in `.env` to notify customers automatically.")
    st.code(
        """DATASENTINEL_WEBHOOK_URL=https://your-slack-or-discord-webhook-url

# Optional email:
DATASENTINEL_EMAIL_TO=customer@example.com
DATASENTINEL_EMAIL_HOST=smtp.example.com
DATASENTINEL_EMAIL_PORT=587
DATASENTINEL_EMAIL_USER=your-smtp-user
DATASENTINEL_EMAIL_PASSWORD=your-smtp-password
DATASENTINEL_EMAIL_FROM=datasentinel@example.com""",
        language="dotenv",
    )


def render_assistant() -> None:
    st.header("OpenMetadata Assistant")
    st.caption("Ask direct questions when you want an interactive investigation.")

    suggestions = [
        "Which tables have no owners?",
        "Which tables are undocumented?",
        "Show data quality failures",
        "Show all ingestion pipelines",
    ]
    cols = st.columns(4)
    for index, suggestion in enumerate(suggestions):
        if cols[index].button(suggestion, use_container_width=True):
            st.session_state.suggested = suggestion

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    prompt = st.session_state.pop("suggested", None)
    prompt = prompt or st.chat_input("Ask anything about your data platform...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = run_agent(prompt)
            st.markdown(response)

        st.session_state.messages.append({"role": "assistant", "content": response})


st.title("DataSentinel")
st.caption("Autonomous OpenMetadata monitoring, safe remediation, and human escalation.")

dashboard_tab, operations_tab, assistant_tab = st.tabs(
    ["Monitor", "Operations", "Assistant"]
)

with dashboard_tab:
    render_dashboard()

with operations_tab:
    render_operations()

with assistant_tab:
    render_assistant()
