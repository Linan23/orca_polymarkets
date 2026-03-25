"""Compatibility entrypoint for the market-level baseline trainer."""

from data_platform.ml.market_baseline_model import train_market_outcome_baseline


if __name__ == "__main__":
    summary = train_market_outcome_baseline()
    print(summary)
