"""
DataSentinel autonomous monitoring engine.

This module scans OpenMetadata through the existing ``tools.py`` functions,
scores platform health, records findings, and can be invoked manually or by
APScheduler. It intentionally does not modify or depend on agent behavior.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypedDict

from langgraph.graph import END, StateGraph
import notifier
import policy
import tools


LOGGER = logging.getLogger(__name__)

DEFAULT_FINDINGS_PATH = Path("data") / "monitor_findings.jsonl"
DEFAULT_LATEST_REPORT_PATH = Path("data") / "monitor_latest.json"

CRITICAL = "critical"
WARNING = "warning"
INFO = "info"
_MONITOR_GRAPH = None


@dataclass(frozen=True)
class Finding:
    """A single DataSentinel observation from a monitoring run."""

    severity: str
    category: str
    title: str
    description: str
    entity: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: utc_now_iso())


@dataclass(frozen=True)
class AutonomousAction:
    """An action DataSentinel attempted without a human prompt."""

    action_type: str
    target: str
    status: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: utc_now_iso())


@dataclass(frozen=True)
class MonitorReport:
    """Complete result of one monitoring cycle."""

    run_id: str
    timestamp: str
    health_score: int
    status: str
    summary: dict[str, Any]
    findings: list[Finding]
    actions: list[AutonomousAction]
    errors: list[str] = field(default_factory=list)


class MonitorState(TypedDict, total=False):
    """LangGraph state for one DataSentinel monitoring cycle."""

    trigger_failed_pipelines: bool
    notify_customer: bool
    findings_path: str | Path
    latest_report_path: str | Path | None
    timestamp: str
    run_id: str
    tables: list[dict[str, Any]]
    unowned_tables: list[dict[str, Any]]
    undocumented_tables: list[dict[str, Any]]
    pipelines: list[dict[str, Any]]
    quality_failures: list[dict[str, Any]]
    failed_pipelines: list[dict[str, Any]]
    findings: list[Finding]
    actions: list[AutonomousAction]
    errors: list[str]
    health_score: int
    status: str
    escalation_decision: policy.EscalationDecision
    notification_results: list[notifier.NotificationResult]
    report: MonitorReport


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp with second precision."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_monitoring_cycle(
    *,
    trigger_failed_pipelines: bool = True,
    notify_customer: bool = True,
    findings_path: str | Path = DEFAULT_FINDINGS_PATH,
    latest_report_path: str | Path | None = DEFAULT_LATEST_REPORT_PATH,
) -> MonitorReport:
    """
    Run one complete DataSentinel monitoring pass.

    This is the primary function to call manually, from Streamlit, from tests,
    or from an APScheduler job.
    """

    result = _get_monitor_graph().invoke(
        {
            "trigger_failed_pipelines": trigger_failed_pipelines,
            "notify_customer": notify_customer,
            "findings_path": findings_path,
            "latest_report_path": latest_report_path,
        }
    )
    report = result["report"]
    LOGGER.info(
        "DataSentinel run %s finished: score=%s findings=%s actions=%s",
        report.run_id,
        report.health_score,
        len(report.findings),
        len(report.actions),
    )
    return report


def calculate_health_score(
    *,
    total_tables: int,
    unowned_count: int,
    undocumented_count: int,
    failed_pipeline_count: int,
    quality_failure_count: int,
    scan_error_count: int = 0,
) -> int:
    """
    Calculate a deterministic 0-100 platform health score.

    The score weighs operational failures most heavily, then data quality, then
    governance coverage. Penalties are capped so one noisy class does not hide
    every other signal.
    """

    table_denominator = max(total_tables, 1)
    unowned_ratio = unowned_count / table_denominator
    undocumented_ratio = undocumented_count / table_denominator

    penalty = 0
    penalty += min(45, failed_pipeline_count * 15)
    penalty += min(40, quality_failure_count * 25)
    penalty += min(12, round(unowned_ratio * 20))
    penalty += min(10, round(undocumented_ratio * 16))
    penalty += min(30, scan_error_count * 25)

    return max(0, min(100, 100 - penalty))


def store_report(
    report: MonitorReport,
    *,
    findings_path: str | Path = DEFAULT_FINDINGS_PATH,
    latest_report_path: str | Path | None = DEFAULT_LATEST_REPORT_PATH,
) -> None:
    """Persist the monitoring report as JSONL and optionally update latest JSON."""

    report_dict = _report_to_dict(report)
    findings_file = Path(findings_path)
    findings_file.parent.mkdir(parents=True, exist_ok=True)

    with findings_file.open("a", encoding="utf-8") as file_obj:
        file_obj.write(json.dumps(report_dict, ensure_ascii=False) + "\n")

    if latest_report_path is not None:
        latest_file = Path(latest_report_path)
        latest_file.parent.mkdir(parents=True, exist_ok=True)
        latest_file.write_text(
            json.dumps(report_dict, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def create_scheduler(
    *,
    interval_minutes: int = 15,
    trigger_failed_pipelines: bool = True,
    notify_customer: bool = True,
    findings_path: str | Path = DEFAULT_FINDINGS_PATH,
    latest_report_path: str | Path | None = DEFAULT_LATEST_REPORT_PATH,
):
    """
    Create a BackgroundScheduler with DataSentinel registered as an interval job.

    APScheduler is imported lazily so the engine remains manually callable even
    when the optional scheduler dependency has not been installed yet.
    """

    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be greater than 0")

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError as exc:
        raise RuntimeError(
            "APScheduler is required for automatic monitoring. "
            "Install it with: pip install APScheduler"
        ) from exc

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        run_monitoring_cycle,
        trigger="interval",
        minutes=interval_minutes,
        kwargs={
            "trigger_failed_pipelines": trigger_failed_pipelines,
            "notify_customer": notify_customer,
            "findings_path": findings_path,
            "latest_report_path": latest_report_path,
        },
        id="datasentinel_monitoring_cycle",
        name="DataSentinel OpenMetadata monitoring cycle",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    return scheduler


def start_scheduler(
    *,
    interval_minutes: int = 15,
    trigger_failed_pipelines: bool = True,
    notify_customer: bool = True,
    run_immediately: bool = True,
    findings_path: str | Path = DEFAULT_FINDINGS_PATH,
    latest_report_path: str | Path | None = DEFAULT_LATEST_REPORT_PATH,
):
    """Start APScheduler and optionally run an immediate first scan."""

    scheduler = create_scheduler(
        interval_minutes=interval_minutes,
        trigger_failed_pipelines=trigger_failed_pipelines,
        notify_customer=notify_customer,
        findings_path=findings_path,
        latest_report_path=latest_report_path,
    )
    scheduler.start()
    if run_immediately:
        run_monitoring_cycle(
            trigger_failed_pipelines=trigger_failed_pipelines,
            notify_customer=notify_customer,
            findings_path=findings_path,
            latest_report_path=latest_report_path,
        )
    return scheduler


def build_monitor_graph():
    """Build the LangGraph workflow used by the DataSentinel monitor."""

    graph = StateGraph(MonitorState)
    graph.add_node("initialize", _initialize_run)
    graph.add_node("scan_openmetadata", _scan_openmetadata)
    graph.add_node("detect_anomalies", _detect_anomalies)
    graph.add_node("autonomous_actions", _run_autonomous_actions)
    graph.add_node("score_health", _score_health)
    graph.add_node("decide_policy", _decide_policy)
    graph.add_node("notify_customer", _notify_customer_node)
    graph.add_node("build_report", _build_report)
    graph.add_node("persist_report", _persist_report)

    graph.set_entry_point("initialize")
    graph.add_edge("initialize", "scan_openmetadata")
    graph.add_edge("scan_openmetadata", "detect_anomalies")
    graph.add_edge("detect_anomalies", "autonomous_actions")
    graph.add_edge("autonomous_actions", "score_health")
    graph.add_edge("score_health", "decide_policy")
    graph.add_edge("decide_policy", "notify_customer")
    graph.add_edge("notify_customer", "build_report")
    graph.add_edge("build_report", "persist_report")
    graph.add_edge("persist_report", END)
    return graph.compile()


def _get_monitor_graph():
    global _MONITOR_GRAPH
    if _MONITOR_GRAPH is None:
        _MONITOR_GRAPH = build_monitor_graph()
    return _MONITOR_GRAPH


def _initialize_run(state: MonitorState) -> MonitorState:
    timestamp = utc_now_iso()
    return {
        **state,
        "timestamp": timestamp,
        "run_id": timestamp.replace(":", "").replace("+", "Z"),
        "findings": [],
        "actions": [],
        "errors": [],
        "notification_results": [],
    }


def _scan_openmetadata(state: MonitorState) -> MonitorState:
    errors = list(state.get("errors", []))
    tables = _safe_call("get_tables", lambda: tools.get_tables(limit=50), errors, [])
    unowned_tables = _safe_call(
        "get_unowned_tables", tools.get_unowned_tables, errors, []
    )
    undocumented_tables = _safe_call(
        "get_undocumented_tables", tools.get_undocumented_tables, errors, []
    )
    pipelines = _safe_call("get_pipelines", tools.get_pipelines, errors, [])
    quality_failures = _safe_call(
        "get_quality_failures", tools.get_quality_failures, errors, []
    )

    return {
        **state,
        "tables": tables,
        "unowned_tables": unowned_tables,
        "undocumented_tables": undocumented_tables,
        "pipelines": pipelines,
        "quality_failures": quality_failures,
        "errors": errors,
    }


def _detect_anomalies(state: MonitorState) -> MonitorState:
    errors = list(state.get("errors", []))
    findings = list(state.get("findings", []))
    tables = state.get("tables", [])
    unowned_tables = state.get("unowned_tables", [])
    undocumented_tables = state.get("undocumented_tables", [])
    pipelines = state.get("pipelines", [])
    quality_failures = state.get("quality_failures", [])
    table_scan_failed = _has_scan_error(errors, "get_tables")

    findings.extend(
        _find_governance_anomalies(
            tables,
            unowned_tables,
            undocumented_tables,
            table_scan_failed=table_scan_failed,
        )
    )
    findings.extend(_find_quality_anomalies(quality_failures))
    failed_pipelines = _find_failed_pipelines(pipelines)
    findings.extend(_find_pipeline_anomalies(failed_pipelines))

    if errors:
        findings.append(
            Finding(
                severity=WARNING,
                category="monitoring",
                title="Monitoring scan completed with errors",
                description="One or more OpenMetadata checks failed during the scan.",
                metadata={"errors": errors},
            )
        )

    return {
        **state,
        "failed_pipelines": failed_pipelines,
        "findings": findings,
    }


def _run_autonomous_actions(state: MonitorState) -> MonitorState:
    actions = list(state.get("actions", []))
    if state.get("trigger_failed_pipelines", True):
        actions.extend(_trigger_failed_pipelines(state.get("failed_pipelines", [])))
    return {**state, "actions": actions}


def _score_health(state: MonitorState) -> MonitorState:
    health_score = calculate_health_score(
        total_tables=len(state.get("tables", [])),
        unowned_count=len(state.get("unowned_tables", [])),
        undocumented_count=len(state.get("undocumented_tables", [])),
        failed_pipeline_count=len(state.get("failed_pipelines", [])),
        quality_failure_count=len(state.get("quality_failures", [])),
        scan_error_count=len(state.get("errors", [])),
    )
    return {
        **state,
        "health_score": health_score,
        "status": _health_status(health_score),
    }


def _decide_policy(state: MonitorState) -> MonitorState:
    decision_report = MonitorReport(
        run_id=state["run_id"],
        timestamp=state["timestamp"],
        health_score=state["health_score"],
        status=state["status"],
        summary={},
        findings=state.get("findings", []),
        actions=list(state.get("actions", [])),
        errors=state.get("errors", []),
    )
    return {
        **state,
        "escalation_decision": policy.decide_escalation(decision_report),
    }


def _notify_customer_node(state: MonitorState) -> MonitorState:
    decision = state["escalation_decision"]
    notification_results: list[notifier.NotificationResult] = []
    actions = list(state.get("actions", []))

    if state.get("notify_customer", True) and decision.should_notify:
        notification_report = MonitorReport(
            run_id=state["run_id"],
            timestamp=state["timestamp"],
            health_score=state["health_score"],
            status=state["status"],
            summary={},
            findings=state.get("findings", []),
            actions=actions,
            errors=state.get("errors", []),
        )
        notification_results = notifier.notify_customer(notification_report, decision)
        actions.extend(_notification_results_to_actions(notification_results, decision))

    return {
        **state,
        "notification_results": notification_results,
        "actions": actions,
    }


def _build_report(state: MonitorState) -> MonitorState:
    findings = state.get("findings", [])
    actions = state.get("actions", [])
    notification_results = state.get("notification_results", [])
    decision = state["escalation_decision"]

    report = MonitorReport(
        run_id=state["run_id"],
        timestamp=state["timestamp"],
        health_score=state["health_score"],
        status=state["status"],
        summary={
            "workflow_engine": "langgraph",
            "tables_scanned": len(state.get("tables", [])),
            "unowned_tables": len(state.get("unowned_tables", [])),
            "undocumented_tables": len(state.get("undocumented_tables", [])),
            "pipelines_scanned": len(state.get("pipelines", [])),
            "failed_pipelines": len(state.get("failed_pipelines", [])),
            "quality_failures": len(state.get("quality_failures", [])),
            "critical_findings": _count_severity(findings, CRITICAL),
            "warning_findings": _count_severity(findings, WARNING),
            "info_findings": _count_severity(findings, INFO),
            "actions_taken": len(actions),
            "manual_intervention_required": decision.manual_intervention_required,
            "notification_required": decision.should_notify,
            "notification_urgency": decision.urgency,
            "notifications_attempted": len(notification_results),
            "notifications_successful": sum(
                1 for result in notification_results if result.status == "success"
            ),
        },
        findings=findings,
        actions=actions,
        errors=state.get("errors", []),
    )
    return {**state, "report": report}


def _persist_report(state: MonitorState) -> MonitorState:
    store_report(
        state["report"],
        findings_path=state.get("findings_path", DEFAULT_FINDINGS_PATH),
        latest_report_path=state.get("latest_report_path", DEFAULT_LATEST_REPORT_PATH),
    )
    return state


def _safe_call(
    name: str,
    func: Callable[[], Any],
    errors: list[str],
    fallback: Any,
) -> Any:
    try:
        return func()
    except Exception as exc:  # noqa: BLE001 - monitoring must continue.
        message = f"{name} failed: {exc}"
        LOGGER.warning(message)
        errors.append(message)
        return fallback


def _find_governance_anomalies(
    tables: list[dict[str, Any]],
    unowned_tables: list[dict[str, Any]],
    undocumented_tables: list[dict[str, Any]],
    *,
    table_scan_failed: bool = False,
) -> list[Finding]:
    findings: list[Finding] = []

    if not tables:
        if table_scan_failed:
            return findings
        findings.append(
            Finding(
                severity=INFO,
                category="inventory",
                title="No tables returned by OpenMetadata",
                description="The table scan returned zero assets.",
            )
        )
        return findings

    for table in unowned_tables:
        findings.append(
            Finding(
                severity=WARNING,
                category="governance",
                title="Table has no owner",
                description="Ownership is missing, which can slow incident response and stewardship workflows.",
                entity=table.get("name"),
                metadata={"table": table},
            )
        )

    for table in undocumented_tables:
        findings.append(
            Finding(
                severity=INFO,
                category="governance",
                title="Table has no description",
                description="Documentation is missing, which makes discovery and trust harder for consumers.",
                entity=table.get("name"),
                metadata={"table": table},
            )
        )

    return findings


def _find_quality_anomalies(quality_failures: list[dict[str, Any]]) -> list[Finding]:
    findings: list[Finding] = []
    for failure in quality_failures:
        findings.append(
            Finding(
                severity=CRITICAL,
                category="data_quality",
                title="Data quality test failed",
                description="A failed OpenMetadata test case was detected.",
                entity=failure.get("name") or failure.get("table"),
                metadata={"failure": failure},
            )
        )
    return findings


def _find_failed_pipelines(pipelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [pipeline for pipeline in pipelines if _pipeline_looks_failed(pipeline)]


def _find_pipeline_anomalies(failed_pipelines: list[dict[str, Any]]) -> list[Finding]:
    findings: list[Finding] = []
    for pipeline in failed_pipelines:
        findings.append(
            Finding(
                severity=CRITICAL,
                category="pipeline",
                title="Ingestion pipeline appears failed",
                description="Pipeline status contains a failed or error state.",
                entity=pipeline.get("name") or pipeline.get("id"),
                metadata={"pipeline": pipeline},
            )
        )
    return findings


def _trigger_failed_pipelines(
    failed_pipelines: list[dict[str, Any]],
) -> list[AutonomousAction]:
    actions: list[AutonomousAction] = []
    for pipeline in failed_pipelines:
        pipeline_id = pipeline.get("id")
        target = pipeline.get("name") or pipeline_id or "unknown pipeline"

        if not pipeline_id:
            actions.append(
                AutonomousAction(
                    action_type="trigger_pipeline",
                    target=target,
                    status="skipped",
                    message="Pipeline could not be triggered because it has no ID.",
                    metadata={"pipeline": pipeline},
                )
            )
            continue

        try:
            result = tools.trigger_pipeline(pipeline_id)
            status = "success" if "success" in str(result).lower() else "unknown"
            actions.append(
                AutonomousAction(
                    action_type="trigger_pipeline",
                    target=target,
                    status=status,
                    message=str(result),
                    metadata={"pipeline_id": pipeline_id},
                )
            )
        except Exception as exc:  # noqa: BLE001 - action failure is part of report.
            actions.append(
                AutonomousAction(
                    action_type="trigger_pipeline",
                    target=target,
                    status="failed",
                    message=str(exc),
                    metadata={"pipeline_id": pipeline_id},
                )
            )

    return actions


def _notification_results_to_actions(
    results: list[notifier.NotificationResult],
    decision: policy.EscalationDecision,
) -> list[AutonomousAction]:
    actions: list[AutonomousAction] = []
    for result in results:
        actions.append(
            AutonomousAction(
                action_type="notify_customer",
                target=result.channel,
                status=result.status,
                message=result.message,
                metadata={
                    "urgency": decision.urgency,
                    "manual_intervention_required": (
                        decision.manual_intervention_required
                    ),
                    "reason": decision.reason,
                    **result.metadata,
                },
            )
        )
    return actions


def _pipeline_looks_failed(pipeline: dict[str, Any]) -> bool:
    status_payload = pipeline.get("status")
    flattened_status = " ".join(_flatten_status_values(status_payload)).lower()
    failure_tokens = ("failed", "failure", "error", "killed", "timeout", "timed_out")
    return any(token in flattened_status for token in failure_tokens)


def _flatten_status_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, bool | int | float):
        return [str(value)]
    if isinstance(value, dict):
        values: list[str] = []
        for key, item in value.items():
            values.append(str(key))
            values.extend(_flatten_status_values(item))
        return values
    if isinstance(value, list | tuple | set):
        values = []
        for item in value:
            values.extend(_flatten_status_values(item))
        return values
    return [str(value)]


def _health_status(score: int) -> str:
    if score < 50:
        return CRITICAL
    if score < 80:
        return WARNING
    return "healthy"


def _count_severity(findings: list[Finding], severity: str) -> int:
    return sum(1 for finding in findings if finding.severity == severity)


def _has_scan_error(errors: list[str], tool_name: str) -> bool:
    return any(error.startswith(f"{tool_name} failed:") for error in errors)


def _report_to_dict(report: MonitorReport) -> dict[str, Any]:
    return {
        **asdict(report),
        "findings": [asdict(finding) for finding in report.findings],
        "actions": [asdict(action) for action in report.actions],
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    report = run_monitoring_cycle()
    print(json.dumps(_report_to_dict(report), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
