"""Nightly maintenance job for partitions, rollups, backfills, and snapshots."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"
DEFAULT_RETENTION_POLICY_PATH = ROOT_DIR / "data_platform" / "config" / "retention_policy.json"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.services.storage_lifecycle import (
    RAW_PAYLOAD_UNREFERENCED_PREDICATE,
    backfill_all_partition_shadows,
    cleanup_orphan_market_events,
    ensure_default_partitions,
    garbage_collect_unreferenced_raw_payloads,
    partition_coverage,
    rollup_old_orderbook_snapshots,
    rollup_old_position_snapshots,
)
from data_platform.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run nightly retention/rollup/backup maintenance.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "") or get_settings().database_url)
    parser.add_argument(
        "--retention-policy-path",
        default=str(DEFAULT_RETENTION_POLICY_PATH),
        help="Path to the JSON retention policy used by dry-run reports.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report retention candidates without running mutating maintenance steps.",
    )
    parser.add_argument(
        "--retention-report-row-limit",
        type=int,
        default=10000,
        help="Cap candidate counting work; reports become lower bounds above this limit.",
    )
    parser.add_argument(
        "--retention-report-timeout-ms",
        type=int,
        default=5000,
        help="Per-count statement timeout for dry-run retention reports.",
    )
    parser.add_argument(
        "--retention-report-count-mode",
        choices=("auto", "exact", "estimate"),
        default="auto",
        help="Use exact capped counts, planner estimates, or auto mode that estimates known high-volume tables.",
    )
    parser.add_argument("--rollup-days", type=int, default=30)
    parser.add_argument("--partition-batch-size", type=int, default=5000)
    parser.add_argument("--orphan-event-batch-size", type=int, default=1000)
    parser.add_argument("--orphan-event-max-batches", type=int, default=10)
    parser.add_argument("--raw-payload-gc-batch-size", type=int, default=1000)
    parser.add_argument("--raw-payload-gc-max-batches", type=int, default=10)
    parser.add_argument("--snapshot-label", default="nightly")
    parser.add_argument("--skip-snapshot", action="store_true")
    parser.add_argument("--skip-orphan-event-cleanup", action="store_true")
    parser.add_argument("--skip-raw-payload-gc", action="store_true")
    parser.add_argument("--summary-log-file", default=str(RUNTIME_DIR / "maintenance_runs.jsonl"))
    return parser.parse_args()


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def load_retention_policy(path: str | Path) -> dict[str, object]:
    policy_path = Path(path)
    with policy_path.open("r", encoding="utf-8") as handle:
        policy = json.load(handle)
    if not isinstance(policy, dict):
        raise ValueError(f"Retention policy must be a JSON object: {policy_path}")
    policy["_policy_path"] = str(policy_path)
    return policy


def _policy_window(policy: dict[str, object], key: str, default_days: int) -> int:
    windows = policy.get("windows")
    if not isinstance(windows, dict):
        return default_days
    raw_value = windows.get(key, default_days)
    try:
        days = int(raw_value)
    except (TypeError, ValueError):
        return default_days
    return max(days, 0)


def _table_exists(session, schema: str, table_name: str) -> bool:
    row = session.execute(
        text("SELECT to_regclass(:qualified_name) IS NOT NULL AS table_exists"),
        {"qualified_name": f"{schema}.{table_name}"},
    ).first()
    return bool(row and row.table_exists)


def _limited_count(
    session,
    *,
    from_where_sql: str,
    params: dict[str, object],
    row_limit: int,
    timeout_ms: int,
    estimate_first: bool = False,
) -> dict[str, object]:
    row_limit = max(row_limit, 1)
    timeout_ms = max(timeout_ms, 100)
    if estimate_first:
        return _estimated_count(
            session,
            from_where_sql=from_where_sql,
            params=params,
            timeout_ms=timeout_ms,
        )
    try:
        session.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
        row = session.execute(
            text(
                f'''
                SELECT count(*) AS row_count
                FROM (
                    SELECT 1
                    {from_where_sql}
                    LIMIT :row_limit_plus_one
                ) candidates
                '''
            ),
            {**params, "row_limit_plus_one": row_limit + 1},
        ).first()
    except SQLAlchemyError as exc:
        session.rollback()
        estimate = _estimated_count(
            session,
            from_where_sql=from_where_sql,
            params=params,
            timeout_ms=timeout_ms,
        )
        return {
            **estimate,
            "count_error": str(exc.__class__.__name__),
            "count_error_detail": str(exc).splitlines()[0][:300],
            "row_limit": row_limit,
            "timeout_ms": timeout_ms,
        }
    candidate_rows = int(row.row_count or 0) if row else 0
    return {
        "candidate_rows": min(candidate_rows, row_limit),
        "candidate_rows_is_lower_bound": candidate_rows > row_limit,
        "row_limit": row_limit,
        "timeout_ms": timeout_ms,
    }


def _estimated_count(
    session,
    *,
    from_where_sql: str,
    params: dict[str, object],
    timeout_ms: int,
) -> dict[str, object]:
    timeout_ms = max(timeout_ms, 100)
    try:
        session.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
        row = session.execute(
            text(
                f'''
                EXPLAIN (FORMAT JSON)
                SELECT 1
                {from_where_sql}
                '''
            ),
            params,
        ).first()
    except SQLAlchemyError as exc:
        session.rollback()
        return {
            "candidate_rows": None,
            "candidate_rows_estimate": None,
            "count_method": "planner_estimate_failed",
            "estimate_error": str(exc.__class__.__name__),
            "estimate_error_detail": str(exc).splitlines()[0][:300],
            "timeout_ms": timeout_ms,
        }
    plan_payload = row[0] if row else None
    if isinstance(plan_payload, str):
        plan_payload = json.loads(plan_payload)
    plan = plan_payload[0].get("Plan", {}) if isinstance(plan_payload, list) and plan_payload else {}
    estimated_rows = int(plan.get("Plan Rows") or 0)
    return {
        "candidate_rows": estimated_rows,
        "candidate_rows_estimate": estimated_rows,
        "candidate_rows_is_estimate": True,
        "count_method": "planner_estimate",
        "timeout_ms": timeout_ms,
    }


def _count_older_rows(
    session,
    *,
    schema: str,
    table_name: str,
    time_column: str,
    cutoff: datetime,
    row_limit: int,
    timeout_ms: int,
    estimate_first: bool = False,
) -> dict[str, object]:
    if not _table_exists(session, schema, table_name):
        return {"table": f"{schema}.{table_name}", "exists": False, "candidate_rows": 0}
    count_result = _limited_count(
        session,
        from_where_sql=f'''
            FROM {schema}."{table_name}"
            WHERE {time_column} < :cutoff
        ''',
        params={"cutoff": cutoff},
        row_limit=row_limit,
        timeout_ms=timeout_ms,
        estimate_first=estimate_first,
    )
    return {
        "table": f"{schema}.{table_name}",
        "exists": True,
        "cutoff": cutoff.isoformat(),
        **count_result,
    }


def _count_raw_payload_gc_candidates(
    session,
    *,
    table_name: str,
    cutoff: datetime,
    row_limit: int,
    timeout_ms: int,
    estimate_first: bool = False,
) -> dict[str, object]:
    if not _table_exists(session, "raw", table_name):
        return {"table": f"raw.{table_name}", "exists": False, "candidate_rows": 0}
    if estimate_first:
        count_result = _limited_count(
            session,
            from_where_sql=f'''
                FROM raw."{table_name}" p
                WHERE p.collected_at < :cutoff
            ''',
            params={"cutoff": cutoff},
            row_limit=row_limit,
            timeout_ms=timeout_ms,
            estimate_first=True,
        )
        return {
            "table": f"raw.{table_name}",
            "exists": True,
            "cutoff": cutoff.isoformat(),
            "reference_filter_applied": False,
            "reference_filter_note": "auto mode estimates age-eligible raw payloads only; real GC still excludes referenced payloads.",
            **count_result,
        }
    count_result = _limited_count(
        session,
        from_where_sql=f'''
            FROM raw."{table_name}" p
            WHERE p.collected_at < :cutoff
            AND {RAW_PAYLOAD_UNREFERENCED_PREDICATE}
        ''',
        params={"cutoff": cutoff},
        row_limit=row_limit,
        timeout_ms=timeout_ms,
        estimate_first=estimate_first,
    )
    return {
        "table": f"raw.{table_name}",
        "exists": True,
        "cutoff": cutoff.isoformat(),
        "reference_filter_applied": True,
        **count_result,
    }


def _count_validated_prediction_snapshots(
    session,
    *,
    cutoff: datetime,
    row_limit: int,
    timeout_ms: int,
    estimate_first: bool = False,
) -> dict[str, object]:
    if not _table_exists(session, "analytics", "ml_market_prediction_snapshot") or not _table_exists(
        session,
        "analytics",
        "ml_market_prediction_validation",
    ):
        return {"exists": False, "candidate_rows": 0}
    count_result = _limited_count(
        session,
        from_where_sql='''
            FROM analytics.ml_market_prediction_snapshot s
            JOIN analytics.ml_market_prediction_validation v
              ON v.ml_market_prediction_snapshot_id = s.ml_market_prediction_snapshot_id
            WHERE s.prediction_generated_at < :cutoff
              AND v.validation_status = 'validated'
        ''',
        params={"cutoff": cutoff},
        row_limit=row_limit,
        timeout_ms=timeout_ms,
        estimate_first=estimate_first,
    )
    return {
        "exists": True,
        "cutoff": cutoff.isoformat(),
        **count_result,
    }


def _count_active_trusted_whales(
    session,
    *,
    row_limit: int,
    timeout_ms: int,
    estimate_first: bool = False,
) -> dict[str, object]:
    if not _table_exists(session, "analytics", "whale_score_snapshot"):
        return {"exists": False, "active_trusted_whales": 0}
    count_result = _limited_count(
        session,
        from_where_sql='''
            FROM analytics.whale_score_snapshot w
            WHERE w.snapshot_time = (SELECT max(snapshot_time) FROM analytics.whale_score_snapshot)
              AND w.is_whale = true
              AND w.is_trusted_whale = true
              AND w.sample_trade_count > 0
        ''',
        params={},
        row_limit=row_limit,
        timeout_ms=timeout_ms,
        estimate_first=estimate_first,
    )
    active_trusted_whales = count_result.pop("candidate_rows", None)
    return {"exists": True, "active_trusted_whales": active_trusted_whales, **count_result}


def build_retention_dry_run_report(
    session,
    policy: dict[str, object],
    *,
    row_limit: int = 10000,
    timeout_ms: int = 5000,
    count_mode: str = "auto",
) -> dict[str, object]:
    generated_at = datetime.now(timezone.utc)
    estimate_all = count_mode == "estimate"
    estimate_high_volume = count_mode in {"auto", "estimate"}

    raw_payload_cutoff = generated_at - timedelta(days=_policy_window(policy, "raw_payload_active_days", 60))
    trade_cutoff = generated_at - timedelta(days=_policy_window(policy, "full_trade_history_days", 180))
    validation_cutoff = generated_at - timedelta(days=_policy_window(policy, "client_validation_days", 180))
    prediction_cutoff = generated_at - timedelta(days=_policy_window(policy, "ml_prediction_validated_days", 180))
    orderbook_cutoff = generated_at - timedelta(days=_policy_window(policy, "orderbook_detail_days", 30))
    dashboard_cutoff = generated_at - timedelta(days=_policy_window(policy, "dashboard_detail_days", 30))
    home_summary_cutoff = generated_at - timedelta(days=_policy_window(policy, "home_summary_detail_days", 30))
    research_summary_cutoff = generated_at - timedelta(days=_policy_window(policy, "research_summary_detail_days", 30))
    whale_score_cutoff = generated_at - timedelta(days=_policy_window(policy, "whale_score_detail_days", 60))

    candidates: dict[str, object] = {
        "raw_payloads_unreferenced_outside_active_window": {
            "action": "archive_then_delete_in_future_phase",
            "protection": "referenced raw payloads are excluded",
            "tables": [
                _count_raw_payload_gc_candidates(
                    session,
                    table_name="api_payload",
                    cutoff=raw_payload_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
                _count_raw_payload_gc_candidates(
                    session,
                    table_name="api_payload_part",
                    cutoff=raw_payload_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
            ],
        },
        "normalized_trades_outside_ml_window": {
            "action": "report_only_keep_until_archive_policy_is_enabled",
            "protection": "last 180 days remain available for ML retraining",
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="transaction_fact",
                    time_column="transaction_time",
                    cutoff=trade_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="transaction_fact_part",
                    time_column="transaction_time",
                    cutoff=trade_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
            ],
        },
        "orderbook_detail_rollup_eligible": {
            "action": "rollup_then_archive_detail_in_future_phase",
            "protection": "detailed rows are counted only after the detail window",
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="orderbook_snapshot",
                    time_column="snapshot_time",
                    cutoff=orderbook_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="orderbook_snapshot_part",
                    time_column="snapshot_time",
                    cutoff=orderbook_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
            ],
        },
        "position_detail_rollup_eligible": {
            "action": "rollup_then_archive_detail_in_future_phase",
            "protection": "detailed rows are counted only after the detail window",
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="position_snapshot",
                    time_column="snapshot_time",
                    cutoff=orderbook_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="position_snapshot_part",
                    time_column="snapshot_time",
                    cutoff=orderbook_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
            ],
        },
        "dashboard_detail_outside_active_window": {
            "action": "summarize_then_archive_detail_in_future_phase",
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="dashboard",
                    time_column="generated_at",
                    cutoff=dashboard_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="home_summary_snapshot",
                    time_column="generated_at",
                    cutoff=home_summary_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="research_analytics_snapshot",
                    time_column="generated_at",
                    cutoff=research_summary_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
            ],
        },
        "whale_score_detail_outside_active_window": {
            "action": "report_only_protect_latest_active_trusted_whales",
            "protection": "latest active trusted whale state must be preserved",
            "active_trusted_whales": _count_active_trusted_whales(
                session,
                row_limit=row_limit,
                timeout_ms=timeout_ms,
                estimate_first=estimate_high_volume,
            ),
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="whale_score_snapshot",
                    time_column="snapshot_time",
                    cutoff=whale_score_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="whale_score_snapshot_part",
                    time_column="snapshot_time",
                    cutoff=whale_score_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_high_volume,
                ),
            ],
        },
        "ml_validation_outside_client_window": {
            "action": "summarize_then_archive_detail_in_future_phase",
            "tables": [
                _count_older_rows(
                    session,
                    schema="analytics",
                    table_name="ml_market_prediction_validation",
                    time_column="validated_at",
                    cutoff=validation_cutoff,
                    row_limit=row_limit,
                    timeout_ms=timeout_ms,
                    estimate_first=estimate_all,
                ),
            ],
        },
        "validated_ml_prediction_snapshots_outside_window": {
            "action": "archive_validated_snapshots_only_in_future_phase",
            "protection": "unvalidated predictions remain protected until their target window can be checked",
            "summary": _count_validated_prediction_snapshots(
                session,
                cutoff=prediction_cutoff,
                row_limit=row_limit,
                timeout_ms=timeout_ms,
                estimate_first=estimate_all,
            ),
        },
    }

    return {
        "started_at": generated_at.isoformat(),
        "ok": True,
        "dry_run": True,
        "policy_version": policy.get("version"),
        "policy_path": policy.get("_policy_path"),
        "scope": policy.get("scope"),
        "counting": {
            "row_limit": row_limit,
            "timeout_ms": timeout_ms,
            "count_mode": count_mode,
            "note": "auto mode uses planner estimates for known high-volume tables and capped exact counts elsewhere.",
        },
        "cutoffs": {
            "raw_payload_active": raw_payload_cutoff.isoformat(),
            "full_trade_history": trade_cutoff.isoformat(),
            "client_validation": validation_cutoff.isoformat(),
            "ml_prediction_validated": prediction_cutoff.isoformat(),
            "orderbook_detail": orderbook_cutoff.isoformat(),
            "dashboard_detail": dashboard_cutoff.isoformat(),
            "home_summary_detail": home_summary_cutoff.isoformat(),
            "research_summary_detail": research_summary_cutoff.isoformat(),
            "whale_score_detail": whale_score_cutoff.isoformat(),
        },
        "candidates": candidates,
        "partition_coverage": partition_coverage(session),
    }


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    retention_policy = load_retention_policy(args.retention_policy_path)
    if args.dry_run:
        with session_scope(args.database_url) as session:
            report = build_retention_dry_run_report(
                session,
                retention_policy,
                row_limit=args.retention_report_row_limit,
                timeout_ms=args.retention_report_timeout_ms,
                count_mode=args.retention_report_count_mode,
            )
        append_jsonl(Path(args.summary_log_file), report)
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0

    with session_scope(args.database_url) as session:
        created_partitions = ensure_default_partitions(session, months_ahead=1)
        backfill_counts = backfill_all_partition_shadows(session, batch_size=args.partition_batch_size)
        orderbook_rollup = rollup_old_orderbook_snapshots(session, older_than_days=args.rollup_days)
        position_rollup = rollup_old_position_snapshots(session, older_than_days=args.rollup_days)
        orphan_event_cleanup = (
            {"skipped": True}
            if args.skip_orphan_event_cleanup
            else cleanup_orphan_market_events(
                session,
                batch_size=args.orphan_event_batch_size,
                max_batches=args.orphan_event_max_batches,
            )
        )
        raw_payload_gc = (
            {"skipped": True}
            if args.skip_raw_payload_gc
            else garbage_collect_unreferenced_raw_payloads(
                session,
                batch_size=args.raw_payload_gc_batch_size,
                max_batches=args.raw_payload_gc_max_batches,
            )
        )
        coverage = partition_coverage(session)

    snapshot_result: dict[str, object] | None = None
    if not args.skip_snapshot:
        env = os.environ.copy()
        env["PSQL_URL"] = args.database_url.replace("+psycopg", "")
        completed = subprocess.run(
            [sys.executable, "scripts/release_snapshot.py", "--label", args.snapshot_label, "--note", "nightly maintenance backup"],
            cwd=ROOT_DIR,
            env=env,
            text=True,
            capture_output=True,
        )
        snapshot_result = {
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout_tail": [line for line in completed.stdout.splitlines() if line.strip()][-5:],
            "stderr_tail": [line for line in completed.stderr.splitlines() if line.strip()][-5:],
        }
        if completed.returncode != 0:
            append_jsonl(
                Path(args.summary_log_file),
                {
                    "started_at": started_at.isoformat(),
                    "ok": False,
                    "created_partitions": created_partitions,
                    "backfill_counts": backfill_counts,
                    "orderbook_rollup": orderbook_rollup,
                    "position_rollup": position_rollup,
                    "orphan_event_cleanup": orphan_event_cleanup,
                    "raw_payload_gc": raw_payload_gc,
                    "coverage": coverage,
                    "snapshot": snapshot_result,
                },
            )
            return completed.returncode

    append_jsonl(
        Path(args.summary_log_file),
        {
            "started_at": started_at.isoformat(),
            "ok": True,
            "created_partitions": created_partitions,
            "backfill_counts": backfill_counts,
            "orderbook_rollup": orderbook_rollup,
            "position_rollup": position_rollup,
            "orphan_event_cleanup": orphan_event_cleanup,
            "raw_payload_gc": raw_payload_gc,
            "coverage": coverage,
            "snapshot": snapshot_result,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
