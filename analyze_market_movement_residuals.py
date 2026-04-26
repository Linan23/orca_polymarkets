"""Compatibility entrypoint for residual whale movement analysis."""

from __future__ import annotations

from data_platform.jobs.analyze_market_movement_residuals import main


if __name__ == "__main__":
    raise SystemExit(main())
