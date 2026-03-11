# Snapshot Release Process

This runbook defines how to publish a reproducible Docker database snapshot for collaborators.

## Purpose

A snapshot release gives every collaborator the same dataset baseline in Docker.
It prevents drift from machine-local ingestion differences.

## Maintainer Flow

1. Ensure source DB is up to date (ingestion + dashboard build complete).
2. Create a versioned snapshot bundle:

```bash
.venv/bin/python scripts/release_snapshot.py --label week6 --note "post-ingest freeze"
```

If your source database is not the Docker default (`5433/app_db`), pass `--psql-url`:

```bash
.venv/bin/python scripts/release_snapshot.py \
  --psql-url postgresql://postgres:postgres@localhost:5432/whaling \
  --label week6
```

3. The script creates:
- `shared_data_snapshot.sql`
- `SHA256SUMS.txt`
- `manifest.json`

under:

- `releases/snapshots/<timestamp>_<label>/`

4. Share that folder (or zip) with collaborators using your chosen distribution channel.

## Collaborator Import

Collaborators import in one command:

```bash
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```

macOS or Ubuntu/Linux full setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```

PowerShell:

```powershell
.\.venv\Scripts\python.exe scripts\setup_collab_db.py --snapshot path\to\shared_data_snapshot.sql
```

## Verification

1. Validate checksum:

```bash
cd path/to/release_folder
shasum -a 256 -c SHA256SUMS.txt
```

2. Validate DB state after import:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data
.venv/bin/python data_platform/tests/data_quality_check.py --require-data
```

3. Confirm Docker DB access:

```bash
docker exec -it orcaDB psql -U app -d app_db
```

## Notes

- Do not commit snapshot SQL files to Git.
- Keep release folders out of source control (`releases/` is local distribution output).
- Include release notes in `--note` for traceability.
