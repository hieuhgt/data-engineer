import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class AlertManager:
    """Send alerts via Slack webhook (or log if webhook not configured)."""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    def _post_slack(self, text: str):
        try:
            import requests
            resp = requests.post(self.webhook_url, json={"text": text}, timeout=5)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Slack alert failed: {e}")

    def alert(self, severity: str, message: str, context: Dict[str, Any] | None = None):
        emoji = {"critical": ":red_circle:", "warning": ":warning:", "info": ":information_source:"}.get(severity, "")
        full_message = f"{emoji} *[{severity.upper()}]* {message}"
        if context:
            full_message += f"\n```{context}```"

        logger.log(
            logging.ERROR if severity == "critical" else logging.WARNING,
            f"ALERT [{severity}]: {message} {context or ''}"
        )
        if self.webhook_url:
            self._post_slack(full_message)

    def sla_miss(self, dag_id: str, actual_hours: float, sla_hours: float = 4.0):
        self.alert(
            "critical",
            f"SLA miss on `{dag_id}`: took {actual_hours:.1f}h (limit {sla_hours}h)",
        )

    def quality_gate_failed(self, gate: str, pass_rate: float, threshold: float):
        self.alert(
            "warning" if pass_rate > 0.85 else "critical",
            f"Quality gate `{gate}` failed: {pass_rate:.1%} < threshold {threshold:.1%}",
        )

    def source_failure(self, failed_sources: List[str]):
        self.alert(
            "critical" if len(failed_sources) > 3 else "warning",
            f"{len(failed_sources)} source(s) failed to ingest: {failed_sources}",
        )
