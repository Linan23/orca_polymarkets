"""Create a versioned PostgreSQL data snapshot release.

Exports data-only SQL for app/analytics/raw schemas plus:
- SHA256 checksum file
- manifest with source info, table counts, partition coverage, and snapshot window metadata
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg


DEFAULT_PSQL_URL = "postgresql://app:password@localhost:5433/app_db"
DEFAULT_OUTPUT_ROOT = "releases/snapshots"
SNAPSHOT_FILENAME = "shared_data_snapshot.sql"
CHECKSUM_FILENAME = "SHA256SUMS.txt"
MANIFEST_FILENAME = "manifest.json"
SNAPSHOT_SCHEMAS = ("app", "analytics", "raw")


def redact_dsn(dsn: str) -> str:
    """Return DSN with password removed."""
    parts = urlsplit(dsn)
    if "@" not in parts.netloc:
        return dsn
    auth, host = parts.netloc.rsplit("@", 1)
    if ":" in auth:
        user = auth.split(":", 1)[0]
        auth = f"{user}:***"
    redacted = parts._replace(netloc=f"{auth}@{host}")
    return urlunsplit(redacted)


def resolve_pg_dump() -> str:
    """Resolve pg_dump binary path."""
    path = shutil.which("pg_dump")
    if path:
        return path
    candidates = (
        "/usr/bin/pg_dump",
        "/usr/lib/postgresql/16/bin/pg_dump",
        "/usr/lib/postgresql/15/bin/pg_dump",
        "/usr/lib/postgresql/14/bin/pg_dump",
        "/usr/local/bin/pg_dump",
        "/opt/homebrew/opt/postgresql@16/bin/pg_dump",
        "/usr/local/opt/postgresql@16/bin/pg_dump",
    )
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("pg_dump not found in PATH or common Linux/Homebrew locations.")


def sha256_file(path: Path) -> str:
    """Compute SHA256 for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_counts(psql_url: str) -> dict[str, int]:
    """Collect row counts across app/raw/analytics tables."""
    query = """
        SELECT format('%I.%I', schemaname, tablename) AS qualified_name,
               (xpath('/row/count/text()', query_to_xml(format('SELECT count(*) AS count FROM %I.%I', schemaname, tablename), false, true, '')))[1]::text::bigint AS row_count
        FROM pg_tables
        WHERE schemaname IN ('app', 'analytics', 'raw')
        ORDER BY schemaname, tablename
    """
    counts: dict[str, int] = {}
    with psycopg.connect(psql_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for name, value in cur.fetchall():
                counts[str(name)] = int(value)
    return counts


def partition_coverage(psql_url: str) -> dict[str, dict[str, str | int | None]]:
    """Collect partition coverage metadata for shadow tables."""
    query = """
        SELECT
            n.nspname AS schema_name,
            p.relname AS parent_name,
            count(*) AS partition_count,
            min(to_date(substring(c.relname FROM '.+_(\\d{6})$'), 'YYYYMM')) AS first_partition,
            max(to_date(substring(c.relname FROM '.+_(\\d{6})$'), 'YYYYMM')) AS last_partition
        FROM pg_inherits
        JOIN pg_class c ON c.oid = pg_inherits.inhrelid
        JOIN pg_class p ON p.oid = pg_inherits.inhparent
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE (n.nspname = 'analytics' AND p.relname IN ('scrape_run_part', 'transaction_fact_part', 'orderbook_snapshot_part', 'position_snapshot_part', 'whale_score_snapshot_part'))
           OR (n.nspname = 'raw' AND p.relname = 'api_payload_part')
        GROUP BY n.nspname, p.relname
        ORDER BY n.nspname, p.relname
    """
    coverage: dict[str, dict[str, str | int | None]] = {}
    with psycopg.connect(psql_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for schema_name, parent_name, partition_count, first_partition, last_partition in cur.fetchall():
                coverage[f"{schema_name}.{parent_name}"] = {
                    "partition_count": int(partition_count or 0),
                    "first_partition": first_partition.isoformat() if first_partition else None,
                    "last_partition": last_partition.isoformat() if last_partition else None,
                }
    return coverage


def snapshot_window(psql_url: str) -> dict[str, str | None]:
    """Return min/max timestamps across append-only domains."""
    query = """
        SELECT min(ts) AS min_timestamp, max(ts) AS max_timestamp
        FROM (
            SELECT collected_at AS ts FROM raw.api_payload
            UNION ALL SELECT started_at AS ts FROM analytics.scrape_run
            UNION ALL SELECT transaction_time AS ts FROM analytics.transaction_fact
            UNION ALL SELECT snapshot_time AS ts FROM analytics.orderbook_snapshot
            UNION ALL SELECT snapshot_time AS ts FROM analytics.position_snapshot
            UNION ALL SELECT snapshot_time AS ts FROM analytics.whale_score_snapshot
        ) unioned
    """
    with psycopg.connect(psql_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            min_timestamp, max_timestamp = cur.fetchone()
    return {
        "min_timestamp": min_timestamp.isoformat() if min_timestamp else None,
        "max_timestamp": max_timestamp.isoformat() if max_timestamp else None,
    }


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description="Create a versioned data snapshot release.")
    parser.add_argument(
        "--psql-url",
        default=os.getenv("PSQL_URL", DEFAULT_PSQL_URL),
        help="Source PostgreSQL URL for pg_dump/data count queries.",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory where versioned snapshot folders are created.",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Optional suffix label for release directory (e.g., nightly).",
    )
    parser.add_argument(
        "--note",
        default="",
        help="Optional release note string saved to manifest.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pg_dump_bin = resolve_pg_dump()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_{args.label.strip()}" if args.label.strip() else ""
    release_dir = Path(args.output_root) / f"{timestamp}{suffix}"
    release_dir.mkdir(parents=True, exist_ok=False)

    snapshot_path = release_dir / SNAPSHOT_FILENAME
    checksum_path = release_dir / CHECKSUM_FILENAME
    manifest_path = release_dir / MANIFEST_FILENAME

    dump_cmd = [
        pg_dump_bin,
        args.psql_url,
        "--data-only",
        *(f"--schema={schema_name}" for schema_name in SNAPSHOT_SCHEMAS),
        "--no-owner",
        "--no-privileges",
        "--file",
        str(snapshot_path),
    ]
    subprocess.run(dump_cmd, check=True)

    digest = sha256_file(snapshot_path)
    checksum_path.write_text(f"{digest}  {SNAPSHOT_FILENAME}\n", encoding="utf-8")

    counts = table_counts(args.psql_url)
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_psql_url": redact_dsn(args.psql_url),
        "snapshot_file": SNAPSHOT_FILENAME,
        "snapshot_sha256": digest,
        "schemas": list(SNAPSHOT_SCHEMAS),
        "table_counts": counts,
        "current_table_counts": {k: v for k, v in counts.items() if k.startswith("analytics.") and not any(token in k for token in ("_history", "_hourly", "_daily", "_part"))},
        "history_table_counts": {k: v for k, v in counts.items() if "_history" in k},
        "rollup_table_counts": {k: v for k, v in counts.items() if k.endswith("_hourly") or k.endswith("_daily")},
        "partition_table_counts": {k: v for k, v in counts.items() if k.endswith("_part")},
        "partition_coverage": partition_coverage(args.psql_url),
        "snapshot_window": snapshot_window(args.psql_url),
        "note": args.note.strip() or None,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Snapshot release created: {release_dir}")
    print(f"- SQL: {snapshot_path}")
    print(f"- SHA256: {checksum_path}")
    print(f"- manifest: {manifest_path}")
    print("\nCollaborator import command:")
    print(f'./scripts/bootstrap.sh --snapshot "{snapshot_path}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
