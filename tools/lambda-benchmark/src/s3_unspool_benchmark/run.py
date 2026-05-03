#!/usr/bin/env python3
"""Run s3-unspool Lambda extraction benchmarks.

The harness updates one Lambda memory size at a time, then runs independent
benchmark samples. Live S3 benchmarks default to serial execution because each
sample can create tens of thousands of Tier-1 S3 requests.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import dataclasses
import json
import math
import os
import re
import shutil
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


DEFAULT_BUCKET = os.environ.get("S3_UNSPOOL_BENCHMARK_BUCKET", "your-benchmark-bucket")
DEFAULT_FIXTURE_PREFIX = "benchmarks/fixtures/2026-04-29"
DEFAULT_DESTINATION_PREFIX = "benchmarks/extract/2026-04-29"
SAFE_DESTINATION_PREFIX_ROOT = "benchmarks/extract/"
DEFAULT_MEMORIES = (128, 256, 512)
DEFAULT_FIXTURES = ("streaming",)
FIXTURE_METADATA = {
    "streaming": {
        "title": "Streaming artifact",
        "uncompressed": "4,506 MiB",
        "zip": "2,071 MiB ZIP",
        "files": "1,000 files",
    },
    "small": {
        "title": "Small artifact",
        "uncompressed": "10 MiB",
        "zip": "4.21 MiB ZIP",
        "files": "512 files",
    },
    "medium": {
        "title": "Medium artifact",
        "uncompressed": "500 MiB",
        "zip": "233.65 MiB ZIP",
        "files": "8,192 files",
    },
    "large": {
        "title": "Large artifact",
        "uncompressed": "3,072 MiB",
        "zip": "1,397 MiB ZIP",
        "files": "49,152 files",
    },
}
FIXTURE_MIX = "40% compressible / 40% incompressible / 20% mixed"
NUMBER_TYPES = (int, float)
BENCHMARK_SECTION_START = "<!-- s3-unspool-benchmark-results:start -->"
BENCHMARK_SECTION_END = "<!-- s3-unspool-benchmark-results:end -->"
REPORT_RE = re.compile(
    r"REPORT RequestId:\s+(?P<request_id>[^\s]+).*?"
    r"Duration:\s+(?P<duration_ms>[0-9.]+)\s+ms.*?"
    r"Billed Duration:\s+(?P<billed_duration_ms>[0-9]+)\s+ms.*?"
    r"Memory Size:\s+(?P<memory_size_mb>[0-9]+)\s+MB.*?"
    r"Max Memory Used:\s+(?P<max_memory_used_mb>[0-9]+)\s+MB",
    re.DOTALL,
)


@dataclasses.dataclass(frozen=True)
class BenchCase:
    fixture: str
    run: str
    catalog: str
    source_zip: str
    ignore_catalog: bool
    setup_zip: str | None

    @property
    def slug(self) -> str:
        return f"{self.fixture}-{self.run}-{self.catalog}"

    @property
    def label(self) -> str:
        if self.run == "full":
            return "full"
        return "update with catalog" if self.catalog == "enabled" else "update with no catalog"


@dataclasses.dataclass(frozen=True)
class SampleTask:
    memory_mb: int
    case: BenchCase
    sample: int
    run_id: str

    @property
    def destination_prefix(self) -> str:
        return (
            f"{DEFAULT_DESTINATION_PREFIX}/{self.run_id}/{self.memory_mb}mb/"
            f"{self.case.slug}/sample-{self.sample:02d}/"
        )

    @property
    def output_stem(self) -> str:
        return f"{self.memory_mb}mb-{self.case.slug}-sample-{self.sample:02d}"


def main() -> int:
    args = parse_args()
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (args.output_dir or Path("benchmark-results") / run_id).resolve()
    results_dir = output_dir / "results"
    setup_dir = output_dir / "setup"
    charts_dir = output_dir / "charts"
    results_dir.mkdir(parents=True, exist_ok=True)
    setup_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(
        connect_timeout=10,
        read_timeout=args.lambda_timeout + 60,
        retries={"max_attempts": args.aws_max_attempts, "mode": "standard"},
        max_pool_connections=max(10, args.max_workers + 4),
    )
    clients = AwsClients(
        cloudformation=session.client("cloudformation", config=config),
        lambda_client=session.client("lambda", config=config),
        s3=session.client("s3", config=config),
    )

    function_name = args.function_name or function_from_stack(clients, args.stack_name)
    cases = build_cases(parse_csv(args.fixtures), parse_csv(args.runs))
    memories = [int(value) for value in parse_csv(args.memories)]
    started_at = datetime.now(UTC)

    manifest = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "function_name": function_name,
        "stack_name": args.stack_name,
        "bucket": args.bucket,
        "fixture_prefix": args.fixture_prefix,
        "destination_prefix": args.destination_prefix,
        "memories": memories,
        "fixtures": parse_csv(args.fixtures),
        "runs": parse_csv(args.runs),
        "samples": args.samples,
        "max_workers": args.max_workers,
        "cleanup_before_run": args.cleanup_before_run,
        "diagnostics": args.diagnostics,
        "include_operations": args.include_operations,
    }
    write_json(output_dir / "manifest.json", manifest)

    if args.dry_run:
        print(json.dumps({"function_name": function_name, "cases": [case.__dict__ for case in cases]}, indent=2))
        return 0

    original_memory = current_lambda_memory(clients, function_name)
    all_results: list[dict[str, Any]] = []

    try:
        for memory in memories:
            print(f"==> Setting {function_name} memory to {memory} MB", flush=True)
            update_lambda_memory(clients, function_name, memory)
            tasks = [
                SampleTask(memory_mb=memory, case=case, sample=sample, run_id=run_id)
                for case in cases
                for sample in range(1, args.samples + 1)
            ]
            print(
                f"==> Running {len(tasks)} samples at {memory} MB with {args.max_workers} workers",
                flush=True,
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                future_to_task = {
                    executor.submit(run_sample, clients, args, function_name, task, results_dir, setup_dir): task
                    for task in tasks
                }
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001 - preserve failed samples in artifacts.
                        result = failed_task_result(args, task, exc)
                        write_json(results_dir / f"{task.output_stem}.json", result)
                    all_results.append(result)
                    status = "ok" if result.get("ok") else "failed"
                    duration = result.get("lambda_report", {}).get("duration_ms")
                    duration_text = f"{duration / 1000:.2f}s" if isinstance(duration, NUMBER_TYPES) else "n/a"
                    print(f"  {status:6} {task.output_stem} {duration_text}", flush=True)
    finally:
        if args.restore_memory and original_memory is not None:
            print(f"==> Restoring {function_name} memory to {original_memory} MB", flush=True)
            update_lambda_memory(clients, function_name, original_memory)

    finished_at = datetime.now(UTC)
    aggregate = aggregate_results(all_results)
    write_json(output_dir / "aggregate.json", aggregate)
    write_json(
        output_dir / "run.json",
        {
            **manifest,
            "finished_at": finished_at.isoformat(),
            "results": all_results,
            "aggregate": aggregate,
        },
    )

    chart_paths = write_charts(aggregate, charts_dir)
    report_markdown = render_markdown(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        output_dir=output_dir,
        function_name=function_name,
        aggregate=aggregate,
        chart_paths=chart_paths,
        chart_link_base=output_dir,
    )
    report_path = output_dir / "report.md"
    report_path.write_text(report_markdown)

    if args.results_md:
        update_results_markdown(
            args.results_md,
            render_markdown(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                output_dir=output_dir,
                function_name=function_name,
                aggregate=aggregate,
                chart_paths={},
                chart_link_base=args.results_md.parent.resolve(),
                redact_function_name=True,
            ),
        )

    print(f"==> Wrote {report_path}", flush=True)
    return 0 if all(result.get("ok") for result in all_results) else 1


@dataclasses.dataclass(frozen=True)
class AwsClients:
    cloudformation: Any
    lambda_client: Any
    s3: Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack-name", default="s3-unspool", help="CloudFormation stack containing FunctionName output")
    parser.add_argument("--function-name", help="Lambda function name; overrides --stack-name lookup")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--fixture-prefix", default=DEFAULT_FIXTURE_PREFIX)
    parser.add_argument("--destination-prefix", default=DEFAULT_DESTINATION_PREFIX)
    parser.add_argument("--memories", default=",".join(str(value) for value in DEFAULT_MEMORIES))
    parser.add_argument("--fixtures", default=",".join(DEFAULT_FIXTURES))
    parser.add_argument(
        "--runs",
        default="full,update-catalog,update-ignore",
        help="Comma-separated run variants: full, update-catalog, update-ignore",
    )
    parser.add_argument("--samples", type=int, default=3, help="Measured Lambda invokes per benchmark case")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Parallel sample workers per memory size; defaults to 1 to limit S3 request costs",
    )
    parser.add_argument(
        "--allow-parallel-s3-costs",
        action="store_true",
        help="Allow --max-workers greater than 1 despite multiplied S3 PUT/LIST request costs",
    )
    parser.add_argument(
        "--cleanup-before-run",
        action="store_true",
        help=(
            "Delete each run-specific destination prefix before invoking Lambda. "
            "Disabled by default because S3 cleanup listing and DeleteObjects add request cost."
        ),
    )
    parser.add_argument("--lambda-timeout", type=int, default=900, help="Lambda timeout in seconds")
    parser.add_argument("--aws-max-attempts", type=int, default=5)
    parser.add_argument("--run-id", help="Stable run id; defaults to UTC timestamp")
    parser.add_argument("--output-dir", type=Path, help="Artifact directory; defaults to benchmark-results/<run-id>")
    parser.add_argument(
        "--results-md",
        type=Path,
        help="Optional Markdown results file to update",
    )
    parser.add_argument("--no-results-md", action="store_true", help="Do not update the Markdown results file")
    parser.add_argument("--profile", help="AWS profile for boto3")
    parser.add_argument("--region", help="AWS region for boto3")
    parser.add_argument("--restore-memory", action="store_true", help="Restore the original Lambda memory after the run")
    parser.add_argument("--dry-run", action="store_true", help="Print planned cases without invoking Lambda")
    parser.add_argument("--no-diagnostics", action="store_true", help="Disable response diagnostics")
    parser.add_argument("--include-operations", action="store_true", help="Ask Lambda to include per-object operations")
    args = parser.parse_args()
    if args.samples <= 0:
        parser.error("--samples must be greater than zero")
    if args.max_workers <= 0:
        parser.error("--max-workers must be greater than zero")
    if args.max_workers > 1 and not args.allow_parallel_s3_costs and not args.dry_run:
        parser.error(
            "--max-workers > 1 multiplies live S3 PUT/LIST request costs; "
            "rerun with --allow-parallel-s3-costs if that is intentional"
        )
    args.destination_prefix = validate_destination_prefix_arg(args.destination_prefix, parser)
    args.fixture_prefix = normalize_prefix_arg(args.fixture_prefix, "--fixture-prefix", parser)
    if args.no_results_md:
        args.results_md = None
    elif args.results_md:
        args.results_md = args.results_md.resolve()
    args.diagnostics = not args.no_diagnostics
    return args


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def normalize_prefix_arg(value: str, option_name: str, parser: argparse.ArgumentParser) -> str:
    prefix = value.strip().strip("/")
    if not prefix:
        parser.error(f"{option_name} must be a non-empty S3 prefix")
    return prefix


def validate_destination_prefix_arg(value: str, parser: argparse.ArgumentParser) -> str:
    prefix = normalize_prefix_arg(value, "--destination-prefix", parser)
    root = SAFE_DESTINATION_PREFIX_ROOT.strip("/")
    if prefix != root and not prefix.startswith(f"{root}/"):
        parser.error(f"--destination-prefix must be under {SAFE_DESTINATION_PREFIX_ROOT}")
    return prefix


def validate_cleanup_prefix(prefix: str) -> str:
    normalized = prefix.strip().strip("/")
    root = SAFE_DESTINATION_PREFIX_ROOT.strip("/")
    if normalized == root or not normalized.startswith(f"{root}/"):
        raise ValueError(f"refusing to delete unsafe benchmark prefix: {prefix!r}")

    suffix_parts = normalized.removeprefix(root).strip("/").split("/")
    if len([part for part in suffix_parts if part]) < 4:
        raise ValueError(f"refusing to delete overly broad benchmark prefix: {prefix!r}")

    return f"{normalized}/"


def build_cases(fixtures: list[str], runs: list[str]) -> list[BenchCase]:
    cases: list[BenchCase] = []
    for fixture in fixtures:
        for run in runs:
            if run == "full":
                cases.append(
                    BenchCase(
                        fixture=fixture,
                        run="full",
                        catalog="enabled",
                        source_zip=f"{fixture}-base.zip",
                        ignore_catalog=False,
                        setup_zip=None,
                    )
                )
            elif run == "update-catalog":
                cases.append(
                    BenchCase(
                        fixture=fixture,
                        run="update-5pct",
                        catalog="enabled",
                        source_zip=f"{fixture}-mutated-5pct.zip",
                        ignore_catalog=False,
                        setup_zip=f"{fixture}-base.zip",
                    )
                )
            elif run == "update-ignore":
                cases.append(
                    BenchCase(
                        fixture=fixture,
                        run="update-5pct",
                        catalog="ignored",
                        source_zip=f"{fixture}-mutated-5pct.zip",
                        ignore_catalog=True,
                        setup_zip=f"{fixture}-base.zip",
                    )
                )
            else:
                raise SystemExit(f"unsupported run variant: {run}")
    return cases


def function_from_stack(clients: AwsClients, stack_name: str) -> str:
    output = clients.cloudformation.describe_stacks(StackName=stack_name)
    outputs = output["Stacks"][0].get("Outputs", [])
    for item in outputs:
        if item.get("OutputKey") == "FunctionName":
            return item["OutputValue"]
    raise RuntimeError(f"stack {stack_name} does not expose a FunctionName output")


def current_lambda_memory(clients: AwsClients, function_name: str) -> int | None:
    try:
        output = clients.lambda_client.get_function_configuration(FunctionName=function_name)
    except ClientError:
        return None
    return int(output["MemorySize"])


def update_lambda_memory(clients: AwsClients, function_name: str, memory_mb: int) -> None:
    wait_for_lambda_update(clients, function_name)
    for attempt in range(1, 4):
        try:
            output = clients.lambda_client.get_function_configuration(FunctionName=function_name)
            if int(output["MemorySize"]) == memory_mb:
                wait_for_lambda_update(clients, function_name)
                return
            clients.lambda_client.update_function_configuration(
                FunctionName=function_name,
                MemorySize=memory_mb,
            )
            wait_for_lambda_update(clients, function_name)
            return
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code == "ResourceConflictException" and attempt < 3:
                wait_for_lambda_update(clients, function_name)
                continue
            raise


def wait_for_lambda_update(clients: AwsClients, function_name: str) -> None:
    waiter = clients.lambda_client.get_waiter("function_updated_v2")
    waiter.wait(FunctionName=function_name, WaiterConfig={"Delay": 2, "MaxAttempts": 180})


def run_sample(
    clients: AwsClients,
    args: argparse.Namespace,
    function_name: str,
    task: SampleTask,
    results_dir: Path,
    setup_dir: Path,
) -> dict[str, Any]:
    destination_prefix = task.destination_prefix.replace(DEFAULT_DESTINATION_PREFIX, args.destination_prefix, 1)
    if args.cleanup_before_run:
        cleanup_prefix(clients, args.bucket, destination_prefix)

    setup_result = None
    if task.case.setup_zip:
        setup_result = invoke_extract(
            clients=clients,
            args=args,
            function_name=function_name,
            source_zip=task.case.setup_zip,
            destination_prefix=destination_prefix,
            ignore_catalog=False,
            include_operations=False,
            measured=False,
        )
        write_json(setup_dir / f"{task.output_stem}-setup.json", setup_result)
        if not setup_result["ok"]:
            result = {
                "ok": False,
                "phase": "setup",
                "error": "setup invocation failed",
                "setup": setup_result,
                **task_metadata(args, task, destination_prefix),
            }
            write_json(results_dir / f"{task.output_stem}.json", result)
            return result

    measured = invoke_extract(
        clients=clients,
        args=args,
        function_name=function_name,
        source_zip=task.case.source_zip,
        destination_prefix=destination_prefix,
        ignore_catalog=task.case.ignore_catalog,
        include_operations=args.include_operations,
        measured=True,
    )
    result = {
        **task_metadata(args, task, destination_prefix),
        **measured,
        "setup": setup_result,
    }
    write_json(results_dir / f"{task.output_stem}.json", result)
    return result


def task_metadata(args: argparse.Namespace, task: SampleTask, destination_prefix: str) -> dict[str, Any]:
    return {
        "memory_mb": task.memory_mb,
        "fixture": task.case.fixture,
        "run": task.case.run,
        "catalog": task.case.catalog,
        "sample": task.sample,
        "source_zip": task.case.source_zip,
        "setup_zip": task.case.setup_zip,
        "ignore_catalog": task.case.ignore_catalog,
        "destination_prefix": f"s3://{args.bucket}/{destination_prefix}",
    }


def failed_task_result(args: argparse.Namespace, task: SampleTask, exc: Exception) -> dict[str, Any]:
    destination_prefix = task.destination_prefix.replace(DEFAULT_DESTINATION_PREFIX, args.destination_prefix, 1)
    return {
        "ok": False,
        "phase": "worker",
        "error": repr(exc),
        **task_metadata(args, task, destination_prefix),
    }


def cleanup_prefix(clients: AwsClients, bucket: str, prefix: str) -> None:
    prefix = validate_cleanup_prefix(prefix)
    paginator = clients.s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for index in range(0, len(objects), 1000):
            batch = objects[index : index + 1000]
            if batch:
                response = clients.s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                errors = response.get("Errors", [])
                if errors:
                    details = ", ".join(
                        f"{error.get('Key', '<unknown>')}: {error.get('Code', 'Error')}"
                        for error in errors[:5]
                    )
                    extra = "" if len(errors) <= 5 else f", and {len(errors) - 5} more"
                    raise RuntimeError(
                        f"DeleteObjects failed for {len(errors)} objects: {details}{extra}"
                    )


def invoke_extract(
    *,
    clients: AwsClients,
    args: argparse.Namespace,
    function_name: str,
    source_zip: str,
    destination_prefix: str,
    ignore_catalog: bool,
    include_operations: bool,
    measured: bool,
) -> dict[str, Any]:
    payload = {
        "source": f"s3://{args.bucket}/{args.fixture_prefix.rstrip('/')}/{source_zip}",
        "destinationPrefix": f"s3://{args.bucket}/{destination_prefix.rstrip('/')}/",
        "deleteExtra": False,
        "diagnostics": args.diagnostics,
        "ignoreCatalog": ignore_catalog,
        "includeOperations": include_operations,
    }
    started = time.perf_counter()
    response = clients.lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload).encode(),
    )
    local_wall_ms = (time.perf_counter() - started) * 1000
    response_payload = response["Payload"].read()
    response_json = parse_response_payload(response_payload)
    log_tail = decode_log_tail(response.get("LogResult"))
    lambda_report = parse_lambda_report(log_tail)
    function_error = response.get("FunctionError")
    summary_errors = None
    if isinstance(response_json, dict):
        summary = response_json.get("summary")
        if isinstance(summary, dict):
            summary_errors = summary.get("errors")
    ok = (
        response.get("StatusCode") == 200
        and function_error is None
        and isinstance(response_json, dict)
        and summary_errors == 0
    )

    return {
        "ok": ok,
        "measured": measured,
        "payload": payload,
        "status_code": response.get("StatusCode"),
        "function_error": function_error,
        "response": response_json,
        "response_payload_bytes": len(response_payload),
        "lambda_report": lambda_report,
        "local_wall_ms": local_wall_ms,
        "log_tail": log_tail,
    }


def parse_response_payload(payload: bytes) -> Any:
    if not payload:
        return None
    decoded = payload.decode(errors="replace")
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        return {"raw": decoded}


def decode_log_tail(log_result: str | None) -> str:
    if not log_result:
        return ""
    return base64.b64decode(log_result).decode(errors="replace")


def parse_lambda_report(log_tail: str) -> dict[str, Any]:
    match = REPORT_RE.search(log_tail)
    if not match:
        return {}
    return {
        "request_id": match.group("request_id"),
        "duration_ms": float(match.group("duration_ms")),
        "billed_duration_ms": int(match.group("billed_duration_ms")),
        "memory_size_mb": int(match.group("memory_size_mb")),
        "max_memory_used_mb": int(match.group("max_memory_used_mb")),
    }


def aggregate_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[int, str, str, str], list[dict[str, Any]]] = {}
    for result in results:
        key = (
            int(result["memory_mb"]),
            str(result["fixture"]),
            str(result["run"]),
            str(result["catalog"]),
        )
        groups.setdefault(key, []).append(result)

    rows: list[dict[str, Any]] = []
    for (memory_mb, fixture, run, catalog), group in sorted(groups.items()):
        ok_group = [item for item in group if item.get("ok")]
        durations = [
            item.get("lambda_report", {}).get("duration_ms")
            for item in ok_group
            if isinstance(item.get("lambda_report", {}).get("duration_ms"), NUMBER_TYPES)
        ]
        max_memory = [
            item.get("lambda_report", {}).get("max_memory_used_mb")
            for item in ok_group
            if isinstance(item.get("lambda_report", {}).get("max_memory_used_mb"), int)
        ]
        summaries = [item.get("response", {}).get("summary", {}) for item in ok_group]
        diagnostics = [item.get("response", {}).get("diagnostics", {}) for item in ok_group]
        rows.append(
            {
                "memory_mb": memory_mb,
                "fixture": fixture,
                "run": run,
                "catalog": catalog,
                "samples": len(group),
                "ok_samples": len(ok_group),
                "failed_samples": len(group) - len(ok_group),
                "duration_ms": stats(durations),
                "max_memory_used_mb": stats(max_memory),
                "summary": {
                    "errors_total": sum_field(summaries, "errors"),
                    "conditional_conflicts_total": sum_field(summaries, "conditional_conflicts"),
                },
                "range": aggregate_range(diagnostics),
                "put": aggregate_put(diagnostics),
            }
        )
    return rows


def stats(values: list[int | float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "median": None, "max": None}
    return {
        "min": float(min(values)),
        "median": float(statistics.median(values)),
        "max": float(max(values)),
    }


def median_field(items: list[dict[str, Any]], field: str) -> float | None:
    values = [item.get(field) for item in items if isinstance(item.get(field), NUMBER_TYPES)]
    return float(statistics.median(values)) if values else None


def sum_field(items: list[dict[str, Any]], field: str) -> int:
    return int(sum(item.get(field, 0) for item in items if isinstance(item.get(field), NUMBER_TYPES)))


def aggregate_range(diagnostics: list[dict[str, Any]]) -> dict[str, float | None]:
    ranges = [item.get("source", {}) for item in diagnostics if isinstance(item.get("source"), dict)]
    return {
        "get_attempts_median": median_field(ranges, "source_get_attempts"),
        "fetched_blocks_median": median_field(ranges, "fetched_blocks"),
        "block_waits_median": median_field(ranges, "block_waits"),
        "source_amplification_median": median_field(ranges, "source_amplification"),
    }


def aggregate_put(diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    puts = [item.get("put", {}) for item in diagnostics if isinstance(item.get("put"), dict)]
    return {
        "failed_attempts_median": median_field(puts, "failed_attempts"),
        "retry_attempts_median": median_field(puts, "retry_attempts"),
        "throttled_attempts_median": median_field(puts, "throttled_attempts"),
        "throttle_waits_median": median_field(puts, "throttle_waits"),
        "failures_by_error_code_total": sum_error_code_maps(puts),
    }


def sum_error_code_maps(items: list[dict[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for item in items:
        failures = item.get("failures_by_error_code", {})
        if not isinstance(failures, dict):
            continue
        for code, count in failures.items():
            if isinstance(code, str) and isinstance(count, NUMBER_TYPES):
                totals[code] = totals.get(code, 0) + int(count)
    return dict(sorted(totals.items()))


def write_charts(aggregate: list[dict[str, Any]], charts_dir: Path) -> dict[str, dict[str, Path]]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: PLC0415

    rows = [row for row in sorted(aggregate, key=chart_sort_key) if row["duration_ms"]["median"] is not None]
    if not rows:
        return {}

    for stale_chart in charts_dir.glob("duration-*.svg"):
        stale_chart.unlink()

    charts: dict[str, dict[str, Path]] = {}
    for fixture, fixture_rows in group_rows_by_fixture(rows):
        groups: list[tuple[int, list[dict[str, Any]]]] = []
        for row in fixture_rows:
            memory_mb = int(row["memory_mb"])
            if not groups or groups[-1][0] != memory_mb:
                groups.append((memory_mb, []))
            groups[-1][1].append(row)

        y_positions: list[float] = []
        tick_positions: list[float] = []
        tick_labels: list[str] = []
        section_spans: list[tuple[float, float, int]] = []
        cursor = 0.0
        for memory_mb, group_rows in groups:
            header_y = cursor
            tick_positions.append(header_y)
            tick_labels.append(f"{memory_mb} MB")
            cursor += 0.9
            start = header_y
            for row in group_rows:
                y_positions.append(cursor)
                tick_positions.append(cursor)
                tick_labels.append(f"  {chart_label(row)}")
                cursor += 1.0
            end = cursor - 1.0
            section_spans.append((start - 0.5, end + 0.5, memory_mb))
            cursor += 0.9

        seconds = [(row["duration_ms"]["median"] or 0) / 1000 for row in fixture_rows]
        lower = [
            ((row["duration_ms"]["median"] or 0) - (row["duration_ms"]["min"] or 0)) / 1000
            for row in fixture_rows
        ]
        upper = [
            ((row["duration_ms"]["max"] or 0) - (row["duration_ms"]["median"] or 0)) / 1000
            for row in fixture_rows
        ]

        variants: dict[str, Path] = {}
        for theme_name, theme in chart_themes().items():
            colors = [chart_color(row, theme_name) for row in fixture_rows]
            fig_height = max(6.2, len(fixture_rows) * 0.38 + len(groups) * 0.55)
            fig, ax = plt.subplots(figsize=(10.5, fig_height))
            fig.patch.set_facecolor(theme["figure"])
            ax.set_facecolor(theme["axes"])
            for index, (y_min, y_max, _memory_mb) in enumerate(section_spans):
                if index % 2 == 0:
                    ax.axhspan(y_min, y_max, color=theme["band"], zorder=0)
                if index > 0:
                    ax.axhline(y_min, color=theme["divider"], linewidth=0.9, zorder=1)

            bars = ax.barh(
                y_positions,
                seconds,
                xerr=[lower, upper],
                capsize=3,
                color=colors,
                error_kw={"ecolor": theme["error"], "elinewidth": 1.1, "capthick": 1.1},
            )
            ax.set_yticks(tick_positions, labels=tick_labels)
            for tick_label in ax.get_yticklabels():
                tick_label.set_color(theme["text"])
                if "/" not in tick_label.get_text():
                    tick_label.set_fontweight("bold")
            ax.tick_params(axis="x", colors=theme["text"])
            for spine in ax.spines.values():
                spine.set_color(theme["spine"])
            ax.invert_yaxis()
            ax.set_xlabel("Lambda REPORT duration (s)", color=theme["text"])
            ax.set_title(chart_title(fixture), color=theme["text"], linespacing=1.25)
            ax.grid(axis="x", color=theme["grid"], alpha=0.42)
            value_labels = ax.bar_label(
                bars,
                labels=[format_seconds_label(value) for value in seconds],
                padding=4,
            )
            for value_label in value_labels:
                value_label.set_color(theme["text"])
            ax.margins(x=0.12)
            fig.subplots_adjust(left=0.31, right=0.96, top=0.87, bottom=0.08)

            chart_path = charts_dir / f"duration-{fixture}-{theme_name}.svg"
            fig.savefig(chart_path, facecolor=fig.get_facecolor())
            plt.close(fig)
            variants[theme_name] = chart_path
        charts[f"{fixture.title()} fixture"] = variants
    return charts


def publish_chart_assets(
    chart_paths: dict[str, dict[str, Path]],
    charts_dir: Path,
) -> dict[str, dict[str, Path]]:
    if not chart_paths:
        return {}

    charts_dir.mkdir(parents=True, exist_ok=True)
    for stale_chart in charts_dir.glob("duration-*.svg"):
        stale_chart.unlink()

    published: dict[str, dict[str, Path]] = {}
    for label, variants in chart_paths.items():
        published_variants: dict[str, Path] = {}
        for theme, source in variants.items():
            destination = charts_dir / source.name
            shutil.copy2(source, destination)
            published_variants[theme] = destination
        published[label] = published_variants
    return published


def chart_themes() -> dict[str, dict[str, str]]:
    return {
        "light": {
            "figure": "#ffffff",
            "axes": "#ffffff",
            "band": "#f6f8fa",
            "divider": "#d0d7de",
            "grid": "#8c959f",
            "spine": "#24292f",
            "text": "#24292f",
            "error": "#24292f",
        },
        "dark": {
            "figure": "#0d1117",
            "axes": "#0d1117",
            "band": "#161b22",
            "divider": "#30363d",
            "grid": "#8b949e",
            "spine": "#8b949e",
            "text": "#f0f6fc",
            "error": "#f0f6fc",
        },
    }


def group_rows_by_fixture(rows: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row["fixture"]), []).append(row)
    fixture_order = {fixture: index for index, fixture in enumerate(DEFAULT_FIXTURES)}
    return sorted(groups.items(), key=lambda item: fixture_order.get(item[0], 99))


def chart_title(fixture: str) -> str:
    metadata = FIXTURE_METADATA.get(fixture)
    if not metadata:
        return f"{fixture.title()} artifact\nMix: {FIXTURE_MIX}"
    return (
        f"{metadata['title']} - {metadata['uncompressed']} uncompressed "
        f"({metadata['zip']}), {metadata['files']}\n"
        f"Mix: {FIXTURE_MIX}"
    )


def chart_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    fixture_order = {"small": 0, "medium": 1, "large": 2}
    scenario_order = {("full", "enabled"): 0, ("update-5pct", "enabled"): 1, ("update-5pct", "ignored"): 2}
    return (
        int(row["memory_mb"]),
        fixture_order.get(str(row["fixture"]), 99),
        scenario_order.get((row["run"], row["catalog"]), 99),
    )


def chart_label(row: dict[str, Any]) -> str:
    return scenario_label(row)


def chart_color(row: dict[str, Any], theme_name: str) -> str:
    if row["run"] == "full":
        return "#58a6ff" if theme_name == "dark" else "#3478f6"
    if row["catalog"] == "enabled":
        return "#3fb950" if theme_name == "dark" else "#16a34a"
    return "#d29922" if theme_name == "dark" else "#f97316"


def scenario_label(row: dict[str, Any]) -> str:
    if row["run"] == "full":
        return "full"
    return "update with catalog" if row["catalog"] == "enabled" else "update with no catalog"


def format_seconds_label(value: float) -> str:
    if value >= 100:
        return f"{value:.0f}s"
    if value >= 10:
        return f"{value:.1f}s"
    return f"{value:.2f}s"


def render_markdown(
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    output_dir: Path,
    function_name: str,
    aggregate: list[dict[str, Any]],
    chart_paths: dict[str, dict[str, Path]],
    chart_link_base: Path,
    include_artifact_paths: bool = False,
    redact_function_name: bool = False,
) -> str:
    elapsed = finished_at - started_at
    display_function_name = "<lambda-function-name>" if redact_function_name else function_name
    lines = [
        "## Automated Benchmark Results",
        "",
        f"Run id: `{run_id}`",
        f"Function: `{display_function_name}`",
        f"Started: `{started_at.isoformat()}`",
        f"Elapsed: `{format_elapsed_seconds(elapsed.total_seconds())}`",
    ]
    if include_artifact_paths:
        lines.append(f"Artifacts: `{relative_or_absolute(output_dir, chart_link_base)}`")
    lines.extend(
        [
            "",
            "| Memory | Fixture | Scenario | Samples | Duration min | Duration median | Duration max | Max memory median | GET attempts median | Fetched blocks median | Block waits median | PUT failures median | PUT retries median | PUT throttles median | Errors |",
            "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in sorted(aggregate, key=chart_sort_key):
        summary = row["summary"]
        range_stats = row["range"]
        put_stats = row.get("put", {})
        lines.append(
            "| "
            + " | ".join(
                [
                    f"{row['memory_mb']} MB",
                    row["fixture"],
                    scenario_label(row),
                    f"{row['ok_samples']}/{row['samples']}",
                    format_ms(row["duration_ms"]["min"]),
                    format_ms(row["duration_ms"]["median"]),
                    format_ms(row["duration_ms"]["max"]),
                    format_mb(row["max_memory_used_mb"]["median"]),
                    format_number(range_stats["get_attempts_median"]),
                    format_number(range_stats["fetched_blocks_median"]),
                    format_number(range_stats["block_waits_median"]),
                    format_number(put_stats.get("failed_attempts_median")),
                    format_number(put_stats.get("retry_attempts_median")),
                    format_number(put_stats.get("throttled_attempts_median")),
                    str(summary["errors_total"]),
                ]
            )
            + " |"
        )

    if chart_paths:
        lines.extend(["", "### Charts", ""])
        for label, variants in chart_paths.items():
            light = variants.get("light")
            dark = variants.get("dark")
            if light and dark:
                light_link = relative_or_absolute(light, chart_link_base)
                dark_link = relative_or_absolute(dark, chart_link_base)
                lines.extend(
                    [
                        "<picture>",
                        f'  <source media="(prefers-color-scheme: dark)" srcset="{dark_link}">',
                        f'  <source media="(prefers-color-scheme: light)" srcset="{light_link}">',
                        f'  <img alt="{label} benchmark duration" src="{light_link}">',
                        "</picture>",
                        "",
                    ]
                )
            elif light:
                link = relative_or_absolute(light, chart_link_base)
                lines.append(f"![{label}]({link})")
                lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def update_results_markdown(path: Path, section: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    marker_section = f"{BENCHMARK_SECTION_START}\n{section.rstrip()}\n{BENCHMARK_SECTION_END}\n"
    if path.exists():
        text = path.read_text()
    else:
        text = (
            "# Benchmark Results\n\n"
            "This file contains generated benchmark output from "
            "`s3-unspool-benchmark`.\n\n"
        )
    start = text.find(BENCHMARK_SECTION_START)
    end = text.find(BENCHMARK_SECTION_END)
    if start != -1 and end != -1 and end > start:
        end += len(BENCHMARK_SECTION_END)
        text = text[:start] + marker_section.rstrip() + text[end:]
        if not text.endswith("\n"):
            text += "\n"
    else:
        if not text.endswith("\n"):
            text += "\n"
        text += "\n" + marker_section
    path.write_text(text)


def relative_or_absolute(path: Path, base: Path) -> str:
    try:
        return os.path.relpath(path, base)
    except ValueError:
        return str(path)


def format_elapsed_seconds(seconds: float) -> str:
    minutes, remainder = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {remainder}s"
    if minutes:
        return f"{minutes}m {remainder}s"
    return f"{remainder}s"


def format_ms(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value / 1000:.2f}s"


def format_mb(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:.0f} MB"


def format_number(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:,.2f}"


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
