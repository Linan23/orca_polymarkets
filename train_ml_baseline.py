"""Compatibility entrypoint for training the first ML baseline model."""

from data_platform.ml.baseline_model import train_profitability_baseline


if __name__ == "__main__":
    summary = train_profitability_baseline()
    print(summary)
