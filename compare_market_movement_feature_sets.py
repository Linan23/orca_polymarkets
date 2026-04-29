"""Compatibility entrypoint for 12h/24h movement feature-set comparison."""

from data_platform.ml.market_baseline_model import compare_price_vs_whale_market_movement_models


if __name__ == "__main__":
    print(compare_price_vs_whale_market_movement_models())
