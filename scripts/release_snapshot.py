"""Create a versioned PostgreSQL data snapshot release.

Exports data-only SQL for analytics/raw schemas plus:
- SHA256 checksum file
- manifest with source info and table counts
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
        "/opt/homebrew/opt/postgresql@16/bin/pg_dump",
        "/usr/local/opt/postgresql@16/bin/pg_dump",
    )
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("pg_dump not found in PATH or common Homebrew locations.")


def sha256_file(path: Path) -> str:
    """Compute SHA256 for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_counts(psql_url: str) -> dict[str, int]:
    """Collect key table counts for manifest metadata."""
    query = """
        SELECT 'analytics.market_event', count(*) FROM analytics.market_event
        UNION ALL SELECT 'analytics.market_contract', count(*) FROM analytics.market_contract
        UNION ALL SELECT 'analytics.transaction_fact', count(*) FROM analytics.transaction_fact
        UNION ALL SELECT 'analytics.position_snapshot', count(*) FROM analytics.position_snapshot
        UNION ALL SELECT 'analytics.orderbook_snapshot', count(*) FROM analytics.orderbook_snapshot
        UNION ALL SELECT 'analytics.dashboard', count(*) FROM analytics.dashboard
        UNION ALL SELECT 'raw.api_payload', count(*) FROM raw.api_payload
    """
    counts: dict[str, int] = {}
    with psycopg.connect(psql_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for name, value in cur.fetchall():
                counts[str(name)] = int(value)
    return counts


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
        help="Optional suffix label for release directory (e.g., week6).",
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
        "--schema=analytics",
        "--schema=raw",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(snapshot_path),
    ]
    subprocess.run(dump_cmd, check=True)

    digest = sha256_file(snapshot_path)
    checksum_path.write_text(f"{digest}  {SNAPSHOT_FILENAME}\n", encoding="utf-8")

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_psql_url": redact_dsn(args.psql_url),
        "snapshot_file": SNAPSHOT_FILENAME,
        "snapshot_sha256": digest,
        "table_counts": table_counts(args.psql_url),
        "note": args.note.strip() or None,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Snapshot release created: {release_dir}")
    print(f"- SQL: {snapshot_path}")
    print(f"- SHA256: {checksum_path}")
    print(f"- manifest: {manifest_path}")
    print("\nCollaborator import command:")
    print(f'.venv/bin/python scripts/setup_collab_db.py --snapshot "{snapshot_path}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
