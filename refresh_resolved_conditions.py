"""Root wrapper for refreshing persisted resolved condition outcomes."""

from data_platform.jobs.refresh_resolved_conditions import main


if __name__ == "__main__":
    raise SystemExit(main())
