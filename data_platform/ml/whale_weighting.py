"""Versioned whale-weight configuration for market ML features."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_WHALE_WEIGHT_CONFIG_PATH = Path("data_platform/ml/whale_weight_config.json")
DEFAULT_WHALE_WEIGHT_CONFIG: dict[str, Any] = {
    "version": "whale_weights_v1",
    "trust_score": 0.35,
    "profitability_score": 0.20,
    "frequency_score": 0.15,
    "holding_behavior_score": 0.15,
    "entry_exit_score": 0.15,
}


@dataclass(frozen=True)
class WhaleWeightConfig:
    """Normalized weight contract used by feature export and model metadata."""

    version: str
    trust_score: float
    profitability_score: float
    frequency_score: float
    holding_behavior_score: float
    entry_exit_score: float

    @property
    def total_weight(self) -> float:
        """Return the configured non-negative weight total."""
        return (
            self.trust_score
            + self.profitability_score
            + self.frequency_score
            + self.holding_behavior_score
            + self.entry_exit_score
        )

    def as_dict(self) -> dict[str, float | str]:
        """Return a serializable representation for dataset metadata."""
        return {
            "version": self.version,
            "trust_score": self.trust_score,
            "profitability_score": self.profitability_score,
            "frequency_score": self.frequency_score,
            "holding_behavior_score": self.holding_behavior_score,
            "entry_exit_score": self.entry_exit_score,
            "total_weight": round(self.total_weight, 8),
        }


def _coerce_nonnegative_float(raw: Any, *, default: float) -> float:
    """Return a non-negative float, falling back for malformed values."""
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return max(value, 0.0)


def load_whale_weight_config(config_path: Path | None = None) -> WhaleWeightConfig:
    """Load a versioned whale-weight config, using defaults when absent."""
    path = config_path or DEFAULT_WHALE_WEIGHT_CONFIG_PATH
    payload: dict[str, Any] = dict(DEFAULT_WHALE_WEIGHT_CONFIG)
    if path.exists():
        with path.open(encoding="utf-8") as handle:
            loaded = json.load(handle)
        if isinstance(loaded, dict):
            payload.update(loaded)

    config = WhaleWeightConfig(
        version=str(payload.get("version") or DEFAULT_WHALE_WEIGHT_CONFIG["version"]),
        trust_score=_coerce_nonnegative_float(
            payload.get("trust_score"),
            default=float(DEFAULT_WHALE_WEIGHT_CONFIG["trust_score"]),
        ),
        profitability_score=_coerce_nonnegative_float(
            payload.get("profitability_score"),
            default=float(DEFAULT_WHALE_WEIGHT_CONFIG["profitability_score"]),
        ),
        frequency_score=_coerce_nonnegative_float(
            payload.get("frequency_score"),
            default=float(DEFAULT_WHALE_WEIGHT_CONFIG["frequency_score"]),
        ),
        holding_behavior_score=_coerce_nonnegative_float(
            payload.get("holding_behavior_score"),
            default=float(DEFAULT_WHALE_WEIGHT_CONFIG["holding_behavior_score"]),
        ),
        entry_exit_score=_coerce_nonnegative_float(
            payload.get("entry_exit_score"),
            default=float(DEFAULT_WHALE_WEIGHT_CONFIG["entry_exit_score"]),
        ),
    )
    if config.total_weight <= 0:
        return WhaleWeightConfig(
            version=str(DEFAULT_WHALE_WEIGHT_CONFIG["version"]),
            trust_score=float(DEFAULT_WHALE_WEIGHT_CONFIG["trust_score"]),
            profitability_score=float(DEFAULT_WHALE_WEIGHT_CONFIG["profitability_score"]),
            frequency_score=float(DEFAULT_WHALE_WEIGHT_CONFIG["frequency_score"]),
            holding_behavior_score=float(DEFAULT_WHALE_WEIGHT_CONFIG["holding_behavior_score"]),
            entry_exit_score=float(DEFAULT_WHALE_WEIGHT_CONFIG["entry_exit_score"]),
        )
    return config


def compute_weighted_whale_score(
    config: WhaleWeightConfig,
    *,
    trust_score: float,
    profitability_score: float,
    frequency_score: float,
    holding_behavior_score: float = 0.0,
    entry_exit_score: float = 0.0,
) -> float:
    """Return one normalized arbitrary-weight whale score."""
    total_weight = config.total_weight
    if total_weight <= 0:
        return 0.0
    weighted = (
        (config.trust_score * max(float(trust_score), 0.0))
        + (config.profitability_score * max(float(profitability_score), 0.0))
        + (config.frequency_score * max(float(frequency_score), 0.0))
        + (config.holding_behavior_score * max(float(holding_behavior_score), 0.0))
        + (config.entry_exit_score * max(float(entry_exit_score), 0.0))
    )
    return round(weighted / total_weight, 8)
