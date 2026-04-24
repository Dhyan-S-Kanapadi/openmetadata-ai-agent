"""
Customer notification helpers for DataSentinel.

Supported channels are intentionally simple for a hackathon-friendly MVP:
- Generic webhook alerts through DATASENTINEL_WEBHOOK_URL.
- SMTP email alerts when DATASENTINEL_EMAIL_* settings are configured.

If no channel is configured, notifications are recorded as skipped instead of
raising an exception that would break monitoring.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotificationResult:
    """Result of attempting customer notification."""

    channel: str
    status: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)


def notify_customer(report: Any, decision: Any) -> list[NotificationResult]:
    """Send a DataSentinel notification through all configured channels."""

    if not getattr(decision, "should_notify", False):
        return [
            NotificationResult(
                channel="policy",
                status="skipped",
                message="Policy decided no notification was required.",
            )
        ]

    results: list[NotificationResult] = []
    webhook_url = os.getenv("DATASENTINEL_WEBHOOK_URL")
    email_to = os.getenv("DATASENTINEL_EMAIL_TO")

    if webhook_url:
        results.append(_send_webhook(webhook_url, report, decision))
    if email_to:
        results.append(_send_email(email_to, report, decision))

    if not results:
        results.append(
            NotificationResult(
                channel="none",
                status="skipped",
                message=(
                    "No notification channel configured. Set DATASENTINEL_WEBHOOK_URL "
                    "or DATASENTINEL_EMAIL_TO to enable customer alerts."
                ),
            )
        )

    return results


def build_alert_text(report: Any, decision: Any) -> str:
    """Create a compact customer-facing alert message."""

    lines = [
        f"DataSentinel {str(getattr(decision, 'urgency', 'info')).upper()} Alert",
        "",
        f"Health score: {getattr(report, 'health_score', 'unknown')}/100",
        f"Status: {getattr(report, 'status', 'unknown')}",
        f"Run ID: {getattr(report, 'run_id', 'unknown')}",
        f"Manual intervention required: {_yes_no(getattr(decision, 'manual_intervention_required', False))}",
        f"Reason: {getattr(decision, 'reason', 'No reason provided.')}",
    ]

    findings = list(getattr(decision, "notify_findings", []))
    if findings:
        lines.extend(["", "Findings:"])
        for finding in findings[:8]:
            lines.append(
                "- "
                f"{_get(finding, 'severity')}: {_get(finding, 'title')}"
                f"{_entity_suffix(_get(finding, 'entity'))}"
            )

    actions = list(getattr(decision, "notify_actions", []))
    if actions:
        lines.extend(["", "Autonomous actions:"])
        for action in actions[:8]:
            lines.append(
                "- "
                f"{_get(action, 'action_type')} on {_get(action, 'target')}: "
                f"{_get(action, 'status')} ({_get(action, 'message')})"
            )

    lines.extend(["", "Latest report: data/monitor_latest.json"])
    return "\n".join(lines)


def _send_webhook(url: str, report: Any, decision: Any) -> NotificationResult:
    payload = {
        "text": build_alert_text(report, decision),
        "datasentinel": {
            "run_id": getattr(report, "run_id", None),
            "health_score": getattr(report, "health_score", None),
            "status": getattr(report, "status", None),
            "urgency": getattr(decision, "urgency", None),
            "manual_intervention_required": getattr(
                decision, "manual_intervention_required", None
            ),
            "reason": getattr(decision, "reason", None),
        },
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code >= 400:
            return NotificationResult(
                channel="webhook",
                status="failed",
                message=f"Webhook returned HTTP {response.status_code}: {response.text}",
            )
        return NotificationResult(
            channel="webhook",
            status="success",
            message="Webhook notification sent.",
            metadata={"status_code": response.status_code},
        )
    except Exception as exc:  # noqa: BLE001 - alert failures are recorded.
        LOGGER.exception("Webhook notification failed")
        return NotificationResult(channel="webhook", status="failed", message=str(exc))


def _send_email(email_to: str, report: Any, decision: Any) -> NotificationResult:
    smtp_host = os.getenv("DATASENTINEL_EMAIL_HOST")
    smtp_port = int(os.getenv("DATASENTINEL_EMAIL_PORT", "587"))
    smtp_user = os.getenv("DATASENTINEL_EMAIL_USER")
    smtp_password = os.getenv("DATASENTINEL_EMAIL_PASSWORD")
    email_from = os.getenv("DATASENTINEL_EMAIL_FROM", smtp_user or "datasentinel@local")

    if not smtp_host:
        return NotificationResult(
            channel="email",
            status="skipped",
            message="DATASENTINEL_EMAIL_HOST is required for email alerts.",
        )

    message = EmailMessage()
    message["Subject"] = (
        f"DataSentinel {str(getattr(decision, 'urgency', 'info')).upper()} Alert"
    )
    message["From"] = email_from
    message["To"] = email_to
    message.set_content(build_alert_text(report, decision))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
            smtp.starttls()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(message)
        return NotificationResult(
            channel="email",
            status="success",
            message=f"Email notification sent to {email_to}.",
        )
    except Exception as exc:  # noqa: BLE001 - alert failures are recorded.
        LOGGER.exception("Email notification failed")
        return NotificationResult(channel="email", status="failed", message=str(exc))


def _entity_suffix(entity: Any) -> str:
    if not entity:
        return ""
    return f" [{entity}]"


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _get(value: Any, key: str, default: Any = "") -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def notification_results_to_json(results: list[NotificationResult]) -> str:
    """Small helper useful for debugging notification outcomes."""

    return json.dumps([result.__dict__ for result in results], indent=2)
