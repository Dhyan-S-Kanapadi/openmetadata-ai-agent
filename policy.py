"""
DataSentinel decision policy.

The policy layer decides which findings are safe for autonomous handling and
which findings need customer notification or manual intervention.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


CRITICAL = "critical"
WARNING = "warning"
INFO = "info"

AUTO_FIXABLE_CATEGORIES = {"pipeline"}
MANUAL_INTERVENTION_CATEGORIES = {"data_quality", "monitoring"}
ACTION_PROBLEM_STATUSES = {"failed", "skipped", "unknown"}


@dataclass(frozen=True)
class EscalationDecision:
    """Outcome of applying DataSentinel policy to one monitor report."""

    should_notify: bool
    urgency: str
    manual_intervention_required: bool
    reason: str
    notify_findings: list[Any] = field(default_factory=list)
    notify_actions: list[Any] = field(default_factory=list)


def is_auto_fixable(finding: Any) -> bool:
    """Return whether a finding category is safe for autonomous remediation."""

    return _get(finding, "category") in AUTO_FIXABLE_CATEGORIES


def decide_escalation(report: Any) -> EscalationDecision:
    """
    Decide whether a report should notify the customer.

    DataSentinel only escalates when there is something meaningful for a human
    to know: critical issues that were not safely resolved, failed autonomous
    actions, scan errors, or a critical overall health state.
    """

    findings = list(_get(report, "findings", []))
    actions = list(_get(report, "actions", []))
    errors = list(_get(report, "errors", []))
    status = str(_get(report, "status", "")).lower()

    problematic_actions = [
        action
        for action in actions
        if str(_get(action, "status", "")).lower() in ACTION_PROBLEM_STATUSES
    ]
    critical_findings = [
        finding
        for finding in findings
        if str(_get(finding, "severity", "")).lower() == CRITICAL
    ]
    manual_findings = [
        finding
        for finding in critical_findings
        if _get(finding, "category") in MANUAL_INTERVENTION_CATEGORIES
    ]
    unresolved_auto_fixable = [
        finding
        for finding in critical_findings
        if is_auto_fixable(finding) and not _has_successful_action_for_finding(finding, actions)
    ]

    if problematic_actions:
        return EscalationDecision(
            should_notify=True,
            urgency=CRITICAL,
            manual_intervention_required=True,
            reason="An autonomous remediation action did not complete successfully.",
            notify_findings=critical_findings,
            notify_actions=problematic_actions,
        )

    if manual_findings:
        return EscalationDecision(
            should_notify=True,
            urgency=CRITICAL,
            manual_intervention_required=True,
            reason="Critical findings require human review.",
            notify_findings=manual_findings,
            notify_actions=actions,
        )

    if unresolved_auto_fixable:
        return EscalationDecision(
            should_notify=True,
            urgency=CRITICAL,
            manual_intervention_required=True,
            reason="A critical auto-fixable finding was not remediated.",
            notify_findings=unresolved_auto_fixable,
            notify_actions=actions,
        )

    if errors:
        return EscalationDecision(
            should_notify=True,
            urgency=WARNING,
            manual_intervention_required=True,
            reason="Monitoring scan errors need operator attention.",
            notify_findings=[
                finding for finding in findings if _get(finding, "category") == "monitoring"
            ],
            notify_actions=actions,
        )

    if status == CRITICAL:
        return EscalationDecision(
            should_notify=True,
            urgency=CRITICAL,
            manual_intervention_required=True,
            reason="Overall platform health is critical.",
            notify_findings=critical_findings,
            notify_actions=actions,
        )

    successful_actions = [
        action for action in actions if str(_get(action, "status", "")).lower() == "success"
    ]
    if successful_actions:
        return EscalationDecision(
            should_notify=True,
            urgency=INFO,
            manual_intervention_required=False,
            reason="DataSentinel completed autonomous remediation.",
            notify_findings=critical_findings,
            notify_actions=successful_actions,
        )

    return EscalationDecision(
        should_notify=False,
        urgency=INFO,
        manual_intervention_required=False,
        reason="No customer notification required.",
    )


def _has_successful_action_for_finding(finding: Any, actions: list[Any]) -> bool:
    entity = _get(finding, "entity")
    category = _get(finding, "category")

    if category != "pipeline" or not entity:
        return False

    for action in actions:
        if str(_get(action, "status", "")).lower() != "success":
            continue
        if _get(action, "action_type") != "trigger_pipeline":
            continue
        if _get(action, "target") == entity:
            return True

    return False


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)
