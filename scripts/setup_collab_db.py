"""Cross-platform collaborator Docker DB setup.

This script works on macOS/Linux/Windows and performs:
1. start Docker PostgreSQL via compose
2. wait for DB readiness
3. run Alembic migrations
4. optional snapshot import (with table reset)
5. smoke validation
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg


DEFAULT_DATABASE_URL = "postgresql+psycopg://app:password@localhost:5433/app_db"
DEFAULT_PSQL_URL = "postgresql://app:password@localhost:5433/app_db"
DEFAULT_CONTAINER = "orcaDB"
DEFAULT_DB_USER = "app"
DEFAULT_DB_NAME = "app_db"


def run(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    check: bool = True,
    stdin_file: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess with optional file stdin."""
    if stdin_file is None:
        return subprocess.run(cmd, env=env, text=True, check=check)
    with stdin_file.open("rb") as handle:
        return subprocess.run(cmd, env=env, stdin=handle, check=check)


def resolve_python(repo_root: Path) -> str:
    """Resolve project virtualenv python when available."""
    win_py = repo_root / ".venv" / "Scripts" / "python.exe"
    unix_py = repo_root / ".venv" / "bin" / "python"
    if win_py.exists():
        return str(win_py)
    if unix_py.exists():
        return str(unix_py)
    return sys.executable


def wait_for_db(psql_url: str, timeout_seconds: int = 60) -> None:
    """Wait until the target PostgreSQL database accepts connections."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with psycopg.connect(psql_url):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Database did not become ready in {timeout_seconds}s: {psql_url}")


def reset_schema_state(psql_url: str) -> None:
    """Drop managed schemas and Alembic state so migrations can rebuild cleanly."""
    with psycopg.connect(psql_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DROP SCHEMA IF EXISTS analytics CASCADE;
                DROP SCHEMA IF EXISTS raw CASCADE;
                DROP TABLE IF EXISTS alembic_version;
                """
            )


def reset_analytics_raw(psql_url: str) -> None:
    """Truncate analytics/raw tables and restart identities."""
    with psycopg.connect(psql_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DO $$
                DECLARE r record;
                BEGIN
                  FOR r IN
                    SELECT schemaname, tablename
                    FROM pg_tables
                    WHERE schemaname IN ('analytics', 'raw')
                    ORDER BY schemaname, tablename
                  LOOP
                    EXECUTE format(
                      'TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE',
                      r.schemaname, r.tablename
                    );
                  END LOOP;
                END $$;
                """
            )


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(
        description="Set up collaborator Docker DB: compose up, migrate, import snapshot, validate."
    )
    parser.add_argument(
        "--snapshot",
        default="",
        help="SQL snapshot path. Defaults to shared_data_snapshot.sql then data_platform/runtime/shared_data_snapshot.sql",
    )
    parser.add_argument("--skip-import", action="store_true", help="Skip snapshot import.")
    parser.add_argument(
        "--no-sample-validation",
        action="store_true",
        help="Run smoke validation without requiring sample data rows.",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="SQLAlchemy URL used by app/Alembic.",
    )
    parser.add_argument(
        "--psql-url",
        default=DEFAULT_PSQL_URL,
        help="Standard PostgreSQL URL used by psycopg.",
    )
    parser.add_argument(
        "--compose-file",
        default="app/compose.yaml",
        help="Docker compose file path.",
    )
    parser.add_argument("--container-name", default=DEFAULT_CONTAINER)
    parser.add_argument("--db-user", default=DEFAULT_DB_USER)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    return parser.parse_args()


def resolve_snapshot(repo_root: Path, snapshot_arg: str, skip_import: bool) -> Path | None:
    """Resolve snapshot file path when import is enabled."""
    if skip_import:
        return None
    if snapshot_arg:
        path = Path(snapshot_arg)
        if not path.is_absolute():
            path = (repo_root / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Snapshot not found: {path}")
        return path

    candidates = [
        repo_root / "shared_data_snapshot.sql",
        repo_root / "data_platform" / "runtime" / "shared_data_snapshot.sql",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        "No snapshot found. Use --snapshot PATH or run with --skip-import."
    )


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)

    snapshot_path = resolve_snapshot(repo_root, args.snapshot, args.skip_import)
    python_bin = resolve_python(repo_root)

    print("Starting Docker PostgreSQL...")
    run(["docker", "compose", "-f", args.compose_file, "up", "-d", "db"])

    print("Waiting for Docker DB...")
    wait_for_db(args.psql_url, timeout_seconds=60)

    env = os.environ.copy()
    env["DATABASE_URL"] = args.database_url
    env["PSQL_URL"] = args.psql_url

    if snapshot_path is not None:
        print("Resetting managed schemas before migrations...")
        reset_schema_state(args.psql_url)

    print("Applying migrations...")
    run([python_bin, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"], env=env)

    if snapshot_path is not None:
        print(f"Importing snapshot: {snapshot_path}")
        print("Resetting analytics/raw tables before import...")
        reset_analytics_raw(args.psql_url)
        run(
            [
                "docker",
                "exec",
                "-i",
                args.container_name,
                "psql",
                "-U",
                args.db_user,
                "-d",
                args.db_name,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin_file=snapshot_path,
        )

    print("Running smoke validation...")
    smoke_cmd = [python_bin, "data_platform/tests/smoke_validate.py"]
    if not args.no_sample_validation:
        smoke_cmd.append("--require-sample-data")
    run(smoke_cmd, env=env)

    print("\nSetup complete.")
    print("Use these in your shell:")
    print(f'  export DATABASE_URL="{args.database_url}"')
    print(f'  export PSQL_URL="{args.psql_url}"')
    print("\nWindows PowerShell:")
    print(f'  $env:DATABASE_URL = "{args.database_url}"')
    print(f'  $env:PSQL_URL = "{args.psql_url}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
