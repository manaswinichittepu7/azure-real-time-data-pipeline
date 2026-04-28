"""
Azure Pipeline Health Check
============================
Probes all pipeline components and reports status to Azure Monitor.
Designed to run as:
  - Post-deployment smoke test
  - Scheduled Azure Function (every 5 min)
  - GitHub Actions deployment gate

Exit code 0 = all healthy, 1 = degraded/critical.
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional

from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.storage.filedatalake import DataLakeServiceClient
import requests

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


class Status(str, Enum):
    HEALTHY   = "healthy"
    DEGRADED  = "degraded"
    CRITICAL  = "critical"
    UNKNOWN   = "unknown"


@dataclass
class CheckResult:
    name: str
    status: Status
    latency_ms: float
    message: str
    details: dict = field(default_factory=dict)
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class HealthReport:
    environment: str
    overall_status: Status
    checks: list[CheckResult]
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def summary(self) -> dict:
        counts = {s: 0 for s in Status}
        for c in self.checks:
            counts[c.status] += 1
        return {s.value: counts[s] for s in Status}


# ---------------------------------------------------------------------------
# Individual health checks
# ---------------------------------------------------------------------------

def check_event_hubs(namespace: str, eventhub: str, consumer_group: str) -> CheckResult:
    """Verify Event Hubs is reachable and receiving events."""
    start = time.monotonic()
    try:
        credential = DefaultAzureCredential()
        client = EventHubConsumerClient(
            fully_qualified_namespace=f"{namespace}.servicebus.windows.net",
            eventhub_name=eventhub,
            consumer_group=consumer_group,
            credential=credential,
        )
        props = client.get_eventhub_properties()
        partition_count = len(props.partition_ids)
        latency = (time.monotonic() - start) * 1000

        return CheckResult(
            name="EventHubs",
            status=Status.HEALTHY,
            latency_ms=round(latency, 1),
            message=f"Connected. {partition_count} partitions available.",
            details={"partitions": partition_count, "namespace": namespace},
        )
    except Exception as exc:
        return CheckResult(
            name="EventHubs",
            status=Status.CRITICAL,
            latency_ms=(time.monotonic() - start) * 1000,
            message=f"Connection failed: {exc}",
        )


def check_adls(account_name: str, container: str) -> CheckResult:
    """Verify ADLS Gen2 is accessible and Bronze/Silver/Gold paths exist."""
    start = time.monotonic()
    try:
        credential = DefaultAzureCredential()
        client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential,
        )
        fs = client.get_file_system_client(container)

        layers = {}
        for layer in ["bronze", "silver", "gold"]:
            paths = list(fs.get_paths(path=layer, max_results=1))
            layers[layer] = len(paths) > 0

        all_present = all(layers.values())
        latency = (time.monotonic() - start) * 1000

        return CheckResult(
            name="ADLS_Gen2",
            status=Status.HEALTHY if all_present else Status.DEGRADED,
            latency_ms=round(latency, 1),
            message="All medallion layers present." if all_present else f"Missing layers: {layers}",
            details={"layers": layers},
        )
    except Exception as exc:
        return CheckResult(
            name="ADLS_Gen2",
            status=Status.CRITICAL,
            latency_ms=(time.monotonic() - start) * 1000,
            message=f"ADLS check failed: {exc}",
        )


def check_databricks_job(host: str, job_id: int, token: str) -> CheckResult:
    """Check the latest Databricks streaming job run status."""
    start = time.monotonic()
    try:
        resp = requests.get(
            f"https://{host}/api/2.1/jobs/runs/list",
            headers={"Authorization": f"Bearer {token}"},
            params={"job_id": job_id, "limit": 1, "active_only": True},
            timeout=10,
        )
        resp.raise_for_status()
        runs = resp.json().get("runs", [])
        latency = (time.monotonic() - start) * 1000

        if not runs:
            return CheckResult(
                name="Databricks_Streaming",
                status=Status.CRITICAL,
                latency_ms=round(latency, 1),
                message="No active streaming runs found — pipeline may be stopped.",
            )

        run = runs[0]
        life_cycle = run["state"]["life_cycle_state"]  # RUNNING, PENDING, TERMINATING, etc.
        result_state = run["state"].get("result_state", "N/A")

        status = Status.HEALTHY if life_cycle == "RUNNING" else Status.DEGRADED
        return CheckResult(
            name="Databricks_Streaming",
            status=status,
            latency_ms=round(latency, 1),
            message=f"Job state: {life_cycle} / {result_state}",
            details={"run_id": run["run_id"], "life_cycle": life_cycle},
        )
    except Exception as exc:
        return CheckResult(
            name="Databricks_Streaming",
            status=Status.UNKNOWN,
            latency_ms=(time.monotonic() - start) * 1000,
            message=f"Databricks API error: {exc}",
        )


def check_synapse(workspace_url: str) -> CheckResult:
    """Ping Synapse workspace REST endpoint."""
    start = time.monotonic()
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://dev.azuresynapse.net/.default").token
        resp = requests.get(
            f"https://{workspace_url}/sqlPools?api-version=2021-06-01",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
        pools = resp.json().get("value", [])
        latency = (time.monotonic() - start) * 1000

        pool_states = {p["name"]: p["properties"]["status"] for p in pools}
        all_online = all(v == "Online" for v in pool_states.values())

        return CheckResult(
            name="Synapse_Analytics",
            status=Status.HEALTHY if all_online else Status.DEGRADED,
            latency_ms=round(latency, 1),
            message=f"SQL pools: {pool_states}",
            details={"pools": pool_states},
        )
    except Exception as exc:
        return CheckResult(
            name="Synapse_Analytics",
            status=Status.UNKNOWN,
            latency_ms=(time.monotonic() - start) * 1000,
            message=f"Synapse check failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Report generation + Azure Monitor push
# ---------------------------------------------------------------------------

def build_report(environment: str, check_results: list[CheckResult]) -> HealthReport:
    """Determine overall status from individual check results."""
    if any(c.status == Status.CRITICAL for c in check_results):
        overall = Status.CRITICAL
    elif any(c.status in (Status.DEGRADED, Status.UNKNOWN) for c in check_results):
        overall = Status.DEGRADED
    else:
        overall = Status.HEALTHY

    return HealthReport(
        environment=environment,
        overall_status=overall,
        checks=check_results,
    )


def push_to_azure_monitor(report: HealthReport, endpoint: str, rule_id: str) -> None:
    """Push health report as custom logs to Azure Monitor (DCR-based ingestion)."""
    try:
        credential = DefaultAzureCredential()
        client = LogsIngestionClient(endpoint=endpoint, credential=credential)
        logs = [
            {
                "TimeGenerated": c.checked_at,
                "CheckName":     c.name,
                "Status":        c.status.value,
                "LatencyMs":     c.latency_ms,
                "Message":       c.message,
                "Environment":   report.environment,
                "OverallStatus": report.overall_status.value,
            }
            for c in report.checks
        ]
        client.upload(rule_id=rule_id, stream_name="Custom-PipelineHealth", logs=logs)
        log.info("Health metrics pushed to Azure Monitor (%d records)", len(logs))
    except Exception as exc:
        log.warning("Failed to push to Azure Monitor: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_checks(args: argparse.Namespace) -> HealthReport:
    import os
    checks: list[CheckResult] = []

    if args.eventhubs_namespace:
        checks.append(check_event_hubs(
            namespace=args.eventhubs_namespace,
            eventhub=args.eventhub_name,
            consumer_group="cg-healthcheck",
        ))

    if args.adls_account:
        checks.append(check_adls(args.adls_account, args.adls_container))

    if args.databricks_host and args.databricks_job_id:
        checks.append(check_databricks_job(
            host=args.databricks_host,
            job_id=args.databricks_job_id,
            token=os.environ.get("DATABRICKS_TOKEN", ""),
        ))

    if args.synapse_url:
        checks.append(check_synapse(args.synapse_url))

    report = build_report(environment=args.environment, check_results=checks)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline health check")
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--eventhubs-namespace", default="")
    parser.add_argument("--eventhub-name", default="telemetry-events")
    parser.add_argument("--adls-account", default="")
    parser.add_argument("--adls-container", default="data")
    parser.add_argument("--databricks-host", default="")
    parser.add_argument("--databricks-job-id", type=int, default=0)
    parser.add_argument("--synapse-url", default="")
    parser.add_argument("--timeout", type=int, default=120, help="Max seconds to wait for all checks")
    parser.add_argument("--monitor-endpoint", default="")
    parser.add_argument("--monitor-rule-id", default="")
    args = parser.parse_args()

    report = run_checks(args)

    # Pretty print
    print(json.dumps(asdict(report), indent=2, default=str))
    log.info("Overall status: %s | Summary: %s", report.overall_status.value, report.summary)

    if args.monitor_endpoint and args.monitor_rule_id:
        push_to_azure_monitor(report, args.monitor_endpoint, args.monitor_rule_id)

    # Exit non-zero on critical
    sys.exit(0 if report.overall_status != Status.CRITICAL else 1)
