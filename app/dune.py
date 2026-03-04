"""Compatibility wrapper for the Dune query ingest job."""

from __future__ import annotations

from data_platform.jobs.dune_query_ingest import main


if __name__ == "__main__":
    raise SystemExit(main())
