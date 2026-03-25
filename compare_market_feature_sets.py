"""Compatibility entrypoint for comparing market model feature sets."""

from data_platform.ml.market_baseline_model import compare_price_vs_whale_market_models


if __name__ == "__main__":
    result = compare_price_vs_whale_market_models()
    print(result)
