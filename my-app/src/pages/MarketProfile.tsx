import { useCallback, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import FollowButton from "../components/FollowButton";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  fetchMarketProfileFull,
  getCachedMarketProfileFull,
  type MarketOutcomeProbability,
  type MarketProfileMlPredictionCase,
  type MarketProfileMlPredictionTrend,
  type MarketProfileMlPredictionValidationSummary,
  type MarketProfileTopWhale,
  type MarketProfileTopWhales,
} from "../lib/api";
import { useApiData } from "../hooks/useApiData";
import { formatTrustScorePercent } from "../lib/scoreFormatting";
import { deriveUserIdentity, deriveWhaleTierLabel } from "../lib/userIdentity";

const PREDICTION_WINDOWS = ["12h", "24h"] as const;

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${Math.round(value * 100)}%`;
}

function formatOddsPercent(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${value.toFixed(digits)}%`;
}

function formatPercentagePointMagnitude(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${Math.abs(value).toFixed(digits)} percentage points`;
}

function formatCompactNumber(value: number | string | null | undefined, digits = 1) {
  if (value === null || typeof value === "undefined" || value === "") return "--";
  const numeric = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatLabel(value: string | null | undefined) {
  if (!value) return "Unknown";
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatCurrency(value: number | null) {
  if (value === null) return "--";
  return `$${value.toLocaleString()}`;
}

function formatCount(value: number | string | null | undefined) {
  const numeric = coerceFiniteNumber(value);
  if (numeric === null) return "--";
  return Math.round(numeric).toLocaleString();
}

function coerceFiniteNumber(value: number | string | null | undefined) {
  if (value === null || typeof value === "undefined" || value === "") return null;
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function polymarketMarketUrl(marketUrl: string | null | undefined, marketSlug: string | null | undefined) {
  if (marketUrl) {
    try {
      const parsed = new URL(marketUrl);
      if (parsed.hostname.endsWith("polymarket.com")) return parsed.toString();
    } catch {
      if (marketUrl.startsWith("/")) return `https://polymarket.com${marketUrl}`;
    }
  }
  if (!marketSlug) return null;
  return `https://polymarket.com/market/${encodeURIComponent(marketSlug)}`;
}

function profileTrendCases(trend: MarketProfileMlPredictionTrend | undefined) {
  if (!trend?.available || !trend.windows) return [];
  return PREDICTION_WINDOWS.flatMap((windowName) =>
    (trend.windows?.[windowName] ?? []).map((item) => ({
      ...item,
      window: windowName,
    })),
  );
}

function normalizeSideLabel(value: string | null | undefined) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function windowHours(windowName: string) {
  return windowName === "12h" ? 12 : 24;
}

function predictionRank(item: MarketProfileMlPredictionCase) {
  const tierRank: Record<string, number> = { show: 0, review: 1, hidden: 2 };
  const signalRank: Record<string, number> = { strong: 0, watch: 1, abstain: 2 };
  return (
    (tierRank[item.display_tier] ?? 3) * 1000 +
    (signalRank[item.direction_signal_tier] ?? 3) * 100 -
    Math.abs(item.predicted_delta_pts ?? 0)
  );
}

function primaryTrendCases(cases: MarketProfileMlPredictionCase[], preferredSideLabel?: string | null) {
  const normalizedPreferred = normalizeSideLabel(preferredSideLabel);
  const preferredCases = normalizedPreferred
    ? cases.filter((item) => normalizeSideLabel(item.side_label) === normalizedPreferred)
    : [];
  if (preferredCases.length > 0) {
    return preferredCases.sort((left, right) => windowHours(left.window) - windowHours(right.window));
  }
  const primary = [...cases].sort((left, right) => predictionRank(left) - predictionRank(right))[0];
  if (!primary) return [];
  return cases
    .filter((item) => item.side_label === primary.side_label)
    .sort((left, right) => windowHours(left.window) - windowHours(right.window));
}

function forecastMarketSummary(item: MarketProfileMlPredictionCase) {
  const sideLabel = formatLabel(item.side_label);
  const futureValue = modelFutureOdds(item);
  const oppositeValue = complementProbability(futureValue);
  const oppositeLabel = oppositeSideLabel(item.side_label);
  const hours = windowHours(item.window);
  if (typeof futureValue !== "number" || typeof oppositeValue !== "number") {
    return `Whale activity forecast is still being prepared for the next ${hours} hours.`;
  }
  if (futureValue > 52) {
    return `Whale activity suggests the market is leaning ${sideLabel} over the next ${hours} hours.`;
  }
  if (oppositeValue > 52) {
    return `Whale activity suggests the market is leaning ${oppositeLabel} over the next ${hours} hours.`;
  }
  return `Whale activity suggests the market is close to balanced over the next ${hours} hours.`;
}

function historicalValidationDescription(item: MarketProfileMlPredictionCase) {
  const trainedAccuracyPct = item.validation_accuracy_pct ?? item.direction_signal_accuracy_pct;
  if (item.historical_validation_tier === "market_closed") {
    return "Market is closed, so this forecast is shown for validation only.";
  }
  if (item.historical_validation_tier === "trained_low_confidence") {
    return "Low confidence: similar past forecasts have not been reliable enough yet.";
  }
  if (
    item.historical_validation_tier === "trained_strong_confidence" ||
    item.historical_validation_tier === "trained_watch_confidence"
  ) {
    return typeof trainedAccuracyPct === "number"
      ? `About ${trainedAccuracyPct.toFixed(1)}% of similar past forecasts matched the actual direction.`
      : "Accuracy is still being validated for this forecast type.";
  }
  if (item.historical_validation_tier === "high_confidence_historical_slice") {
    const pct = item.historical_validation_direction_match_pct;
    return typeof pct === "number"
      ? `About ${pct.toFixed(1)}% of similar past forecasts matched the actual direction.`
      : "Similar past forecasts have shown useful direction matches.";
  }
  if (item.historical_validation_tier === "strong_model_signal") {
    return "High confidence signal; exact historical accuracy is still being validated.";
  }
  if (
    item.historical_validation_tier === "review_only" ||
    item.historical_validation_tier === "insufficient_validated_accuracy" ||
    item.direction_signal_tier === "abstain"
  ) {
    return "Not enough validated history yet, so treat this as review-only.";
  }
  if (item.direction_signal_tier === "watch") {
    return "Watch signal: useful direction clue, but not enough validated history yet.";
  }
  return "Accuracy is still being validated for this forecast type.";
}

function modelFutureOdds(item: MarketProfileMlPredictionCase) {
  return item.model_predicted_future_odds_pct ?? item.predicted_future_odds_pct ?? null;
}

function modelDelta(item: MarketProfileMlPredictionCase) {
  return item.model_predicted_delta_pts ?? item.predicted_delta_pts ?? null;
}

function predictionForecastSummary(item: MarketProfileMlPredictionCase) {
  const sideLabel = formatLabel(item.side_label);
  const futureOdds = modelFutureOdds(item);
  const oppositeOdds = complementProbability(futureOdds);
  return `${item.window} market forecast ${sideLabel} ${formatOddsPercent(futureOdds)} / ${oppositeSideLabel(item.side_label)} ${formatOddsPercent(oppositeOdds)}`;
}

function marketLeanLabel(item: MarketProfileMlPredictionCase) {
  const sideLabel = formatLabel(item.side_label);
  const futureValue = modelFutureOdds(item);
  const oppositeValue = complementProbability(futureValue);
  if (typeof futureValue !== "number" || typeof oppositeValue !== "number") return "Market trend pending";
  if (futureValue > 52) return `Market leaning ${sideLabel}`;
  if (oppositeValue > 52) return `Market leaning ${oppositeSideLabel(item.side_label)}`;
  return "Market mostly balanced";
}

function forecastMoveDetail(item: MarketProfileMlPredictionCase) {
  const sideLabel = formatLabel(item.side_label);
  const futureValue = modelFutureOdds(item);
  const futureOdds = formatOddsPercent(futureValue);
  const oppositeOdds = formatOddsPercent(complementProbability(futureValue));
  const oppositeLabel = oppositeSideLabel(item.side_label);
  const futureNumeric = modelFutureOdds(item);
  const oppositeNumeric = complementProbability(futureNumeric);
  if (typeof futureNumeric === "number" && futureNumeric > 52) {
    return `${sideLabel} ${futureOdds} / ${oppositeLabel} ${oppositeOdds}; market leans toward ${sideLabel}.`;
  }
  if (typeof oppositeNumeric === "number" && oppositeNumeric > 52) {
    return `${sideLabel} ${futureOdds} / ${oppositeLabel} ${oppositeOdds}; market leans toward ${oppositeLabel}.`;
  }
  return `${sideLabel} ${futureOdds} / ${oppositeLabel} ${oppositeOdds}; market is close to balanced.`;
}

function firstFiniteNumber(values: Array<number | string | null | undefined>) {
  return values.map(coerceFiniteNumber).find((value): value is number => value !== null) ?? null;
}

function whalePressureInsight(item: MarketProfileMlPredictionCase, allCases: MarketProfileMlPredictionCase[]) {
  const sideLabel = formatLabel(item.side_label);
  const liveFeatures = item.live_window_features ?? {};
  const whaleAnchor = item.whale_anchor ?? {};
  const pressure = firstFiniteNumber([
    liveFeatures["24h"]?.weighted_net_pressure,
    liveFeatures["12h"]?.weighted_net_pressure,
    whaleAnchor.side_net_pressure,
  ]);
  const pressureRatio = firstFiniteNumber([whaleAnchor.pressure_ratio]);
  const lean = whaleLeanSummary(allCases);

  if (pressure === null && !lean.leader) {
    return {
      label: "Whale Pressure",
      value: "Not enough data",
      detail: "Not enough whale pressure data yet.",
    };
  }

  if (pressure !== null && Math.abs(pressure) < 0.01) {
    return {
      label: "Whale Pressure",
      value: "Mixed pressure",
      detail: "Whale buying and selling are close to balanced.",
    };
  }

  const supportsSide = pressure === null ? lean.leader?.key === normalizeSideLabel(item.side_label) : pressure > 0;
  const strength = Math.abs(pressureRatio ?? 0);
  const strengthLabel = strength >= 0.35 ? "Strong whale support" : strength >= 0.1 ? "Light whale support" : "Whale support";
  return {
    label: "Whale Pressure",
    value: supportsSide ? strengthLabel : "Pressure against side",
    detail: supportsSide
      ? `Trusted whale activity is pushing toward ${sideLabel}.`
      : `Trusted whale activity is not clearly supporting ${sideLabel}.`,
  };
}

function netSupportInsight(item: MarketProfileMlPredictionCase) {
  const whaleAnchor = item.whale_anchor ?? {};
  const entryPressure = firstFiniteNumber([whaleAnchor.side_entry_pressure, whaleAnchor.recent_weighted_entry_12h]);
  const exitPressure = firstFiniteNumber([whaleAnchor.side_exit_pressure, whaleAnchor.recent_weighted_exit_12h]);
  const netPressure = firstFiniteNumber([whaleAnchor.side_net_pressure, whaleAnchor.recent_weighted_net_pressure_12h]);
  const resolvedNet =
    netPressure ?? (entryPressure !== null && exitPressure !== null ? entryPressure - exitPressure : null);

  if (resolvedNet === null) {
    return {
      label: "Net Support",
      value: "Not enough data",
      detail: "Not enough whale pressure data yet.",
    };
  }
  if (Math.abs(resolvedNet) < 0.01) {
    return {
      label: "Net Support",
      value: "Balanced pressure",
      detail: "Whale buying and selling are nearly even.",
    };
  }
  return {
    label: "Net Support",
    value: resolvedNet > 0 ? "More whale buying" : "More whale selling",
    detail:
      resolvedNet > 0
        ? "Whale entries outweigh exits on this side."
        : "Whale exits outweigh entries on this side.",
  };
}

function trustedActivityInsight(item: MarketProfileMlPredictionCase) {
  const whaleAnchor = item.whale_anchor ?? {};
  const trustedEvents = firstFiniteNumber([whaleAnchor.trusted_event_count]);
  const totalEvents = firstFiniteNumber([whaleAnchor.event_count]);

  if (trustedEvents === null && totalEvents === null) {
    return {
      label: "Trusted Activity",
      value: "Not enough data",
      detail: "Not enough whale pressure data yet.",
    };
  }
  return {
    label: "Trusted Activity",
    value: `${formatCount(trustedEvents ?? 0)} trusted`,
    detail: `${formatCount(totalEvents ?? trustedEvents ?? 0)} total whale signal${
      Math.round(totalEvents ?? trustedEvents ?? 0) === 1 ? "" : "s"
    } found for this side.`,
  };
}

function entryStrengthInsight(item: MarketProfileMlPredictionCase) {
  const sideLabel = formatLabel(item.side_label);
  const entryNotional = coerceFiniteNumber(item.whale_entry_notional);
  const weightedNotional = coerceFiniteNumber(item.whale_entry_weighted_notional);
  const trustScore = coerceFiniteNumber(item.whale_entry_trust_score);
  const hasTrustedEntry = item.whale_entry_is_trusted === true || (trustScore !== null && trustScore >= 1);

  if (entryNotional === null && weightedNotional === null && trustScore === null) {
    return {
      label: "Entry Strength",
      value: "Not enough data",
      detail: "Not enough whale pressure data yet.",
    };
  }

  const weightedLift =
    entryNotional !== null && weightedNotional !== null && entryNotional > 0 ? weightedNotional / entryNotional : null;
  const value = hasTrustedEntry ? "Trusted entry" : weightedLift !== null && weightedLift >= 1.1 ? "Weighted entry" : "Review entry";
  const sizeText = formatCurrency(Math.round(weightedNotional ?? entryNotional ?? 0));
  return {
    label: "Entry Strength",
    value,
    detail:
      sizeText === "$0"
        ? `${sideLabel} entry is included in the forecast weighting.`
        : `${sideLabel} entry carries about ${sizeText} of weighted pressure.`,
  };
}

type WhaleLeanSide = {
  key: string;
  label: string;
  score: number;
};

function whaleLeanScore(item: MarketProfileMlPredictionCase) {
  const liveFeatures = item.live_window_features ?? {};
  const whaleAnchor = item.whale_anchor ?? {};
  const candidates = [
    coerceFiniteNumber(liveFeatures["24h"]?.weighted_net_pressure),
    coerceFiniteNumber(liveFeatures["12h"]?.weighted_net_pressure),
    coerceFiniteNumber(whaleAnchor.side_total_pressure),
    coerceFiniteNumber(item.whale_entry_weighted_notional),
  ];
  return candidates.find((value): value is number => typeof value === "number") ?? null;
}

function whaleLeanSides(cases: MarketProfileMlPredictionCase[]) {
  const bySide = new Map<string, WhaleLeanSide>();
  cases.forEach((item) => {
    const key = normalizeSideLabel(item.side_label);
    if (!key) return;
    const rawScore = whaleLeanScore(item);
    if (rawScore === null) return;
    const score = Math.max(rawScore, 0);
    const current = bySide.get(key);
    if (!current || score > current.score) {
      bySide.set(key, {
        key,
        label: formatLabel(item.side_label),
        score,
      });
    }
  });
  return [...bySide.values()].sort((left, right) => right.score - left.score);
}

function whaleLeanSummary(cases: MarketProfileMlPredictionCase[]) {
  const sides = whaleLeanSides(cases);
  const leader = sides[0];
  const runnerUp = sides[1];
  const hasClearLean = Boolean(leader && leader.score > 0 && (!runnerUp || leader.score > runnerUp.score));
  return {
    sides,
    leader: hasClearLean ? leader : null,
    maxScore: Math.max(...sides.map((side) => side.score), 0),
  };
}

function whaleSupportLabel(score: number, maxScore: number) {
  if (maxScore <= 0 || score <= 0) return "No clear support";
  const ratio = score / maxScore;
  if (ratio >= 0.95) return "Most support";
  if (ratio >= 0.5) return "Some support";
  return "Light support";
}

function oppositeSideLabel(value: string | null | undefined) {
  const normalized = normalizeSideLabel(value);
  if (normalized === "yes") return "No";
  if (normalized === "no") return "Yes";
  if (normalized === "up") return "Down";
  if (normalized === "down") return "Up";
  return "Other";
}

function sideChartClass(value: string | null | undefined) {
  const normalized = normalizeSideLabel(value);
  if (normalized === "yes" || normalized === "up") return "is-yes";
  if (normalized === "no" || normalized === "down") return "is-no";
  return "is-other";
}

function complementProbability(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.min(Math.max(100 - value, 0), 100);
}

function binaryProbabilityRows(item: MarketProfileMlPredictionCase, value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return [
      { label: formatLabel(item.side_label), value: null },
      { label: oppositeSideLabel(item.side_label), value: null },
    ];
  }
  const sideLabel = formatLabel(item.side_label);
  return [
    { label: sideLabel, value },
    { label: oppositeSideLabel(item.side_label), value: complementProbability(value) },
  ];
}

function completedValidation(item: MarketProfileMlPredictionCase) {
  const hasCurrentActual = typeof item.actual_future_odds_pct === "number" && Number.isFinite(item.actual_future_odds_pct);
  if (hasCurrentActual) {
    return {
      window: item.window,
      side_label: item.side_label,
      observation_time: item.observation_time,
      prediction_start_time: item.prediction_start_time ?? item.observation_time,
      prediction_target_time: item.prediction_target_time ?? null,
      prediction_window_hours: item.prediction_window_hours ?? windowHours(item.window),
      current_odds_pct: item.current_odds_pct,
      model_predicted_future_odds_pct: modelFutureOdds(item),
      model_predicted_delta_pts: modelDelta(item),
      actual_future_odds_pct: item.actual_future_odds_pct ?? null,
      actual_delta_pts: item.actual_delta_pts ?? null,
      prediction_absolute_error_pts: item.prediction_absolute_error_pts ?? null,
      prediction_direction_match: item.prediction_direction_match ?? null,
      prediction_validation_status: item.prediction_validation_status ?? "validated",
      actual_source: item.actual_source ?? null,
      actual_observed_at: item.actual_observed_at ?? null,
      comparison_type: "current_snapshot",
    };
  }
  const latest = item.latest_completed_validation;
  if (latest && typeof latest.actual_future_odds_pct === "number" && Number.isFinite(latest.actual_future_odds_pct)) {
    return latest;
  }
  return null;
}

function outcomeProbabilityRows(
  outcomes: MarketOutcomeProbability[] | null | undefined,
  price: number | null,
  odds: number | null,
) {
  const liveRows = (outcomes ?? []).filter((item) => item.label && typeof item.probability === "number");
  if (liveRows.length > 0) return liveRows;
  const baseline = odds ?? price;
  return [
    { label: "Yes", probability: price },
    { label: "No", probability: baseline === null ? null : Math.min(Math.max(1 - baseline, 0), 1) },
  ];
}

function CurrentMarketProbabilityPanel({
  outcomes,
  price,
  odds,
}: {
  outcomes: MarketOutcomeProbability[] | null | undefined;
  price: number | null;
  odds: number | null;
}) {
  const rows = outcomeProbabilityRows(outcomes, price, odds).slice(0, 2);
  return (
    <div className="market-ml-current-probability">
      <p className="market-ml-section-label">Current market probability</p>
      <div className="market-ml-probability-grid">
        {rows.map((outcome, index) => (
          <div className={`market-ml-probability-tile ${index === 0 ? "is-yes" : "is-no"}`} key={outcome.label}>
            <span>{formatLabel(outcome.label)}</span>
            <strong>{formatPercent(outcome.probability)}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function MarketPredictionTrendChart({
  cases,
  allCases = cases,
}: {
  cases: MarketProfileMlPredictionCase[];
  allCases?: MarketProfileMlPredictionCase[];
}) {
  if (cases.length === 0) return null;

  const base = cases[0];
  const validationComparisons = cases
    .map((item) => ({ item, comparison: completedValidation(item) }))
    .filter(
      (entry) =>
        entry.comparison &&
        typeof entry.comparison.actual_future_odds_pct === "number" &&
        typeof entry.comparison.model_predicted_future_odds_pct === "number",
    );
  const useCompletedValidation = validationComparisons.length > 0;
  const baseComparison = validationComparisons[0]?.comparison;
  const entryOdds = useCompletedValidation
    ? baseComparison?.current_odds_pct ?? base.whale_entry_odds_pct ?? base.current_odds_pct ?? 50
    : base.whale_entry_odds_pct ?? base.current_odds_pct ?? 50;
  const entryTime = base.whale_entry_time ?? base.prediction_start_time ?? base.observation_time;
  const primarySideLabel = formatLabel(base.side_label);
  const oppositeLabel = oppositeSideLabel(base.side_label);
  const forecastSummaries = cases
    .filter((item) => typeof modelFutureOdds(item) === "number")
    .map(predictionForecastSummary);
  const predictedPoints = useCompletedValidation
    ? [
        { hour: 0, odds: entryOdds, label: "entry" },
        ...validationComparisons.map(({ item, comparison }) => ({
          hour: comparison?.prediction_window_hours ?? item.prediction_window_hours ?? windowHours(item.window),
          odds: comparison?.model_predicted_future_odds_pct ?? entryOdds,
          label: item.window,
        })),
      ]
    : [
        { hour: 0, odds: entryOdds, label: "entry" },
        ...cases
          .filter((item) => typeof modelFutureOdds(item) === "number")
          .map((item) => ({
            hour: item.prediction_window_hours ?? windowHours(item.window),
            odds: modelFutureOdds(item) ?? entryOdds,
            label: item.window,
          })),
      ];
  const actualPoints = useCompletedValidation
    ? validationComparisons.map(({ item, comparison }) => ({
        hour: comparison?.prediction_window_hours ?? item.prediction_window_hours ?? windowHours(item.window),
        odds: comparison?.actual_future_odds_pct ?? entryOdds,
        label: item.window,
      }))
    : cases
        .filter((item) => typeof item.actual_future_odds_pct === "number")
        .map((item) => ({
          hour: item.prediction_window_hours ?? windowHours(item.window),
          odds: item.actual_future_odds_pct ?? entryOdds,
          label: item.window,
        }));
  const oppositePredictedPoints = predictedPoints.map((point) => ({
    ...point,
    odds: complementProbability(point.odds) ?? 0,
  }));
  const oppositeActualPoints = actualPoints.map((point) => ({
    ...point,
    odds: complementProbability(point.odds) ?? 0,
  }));
  const predictedSeries = [
    {
      label: primarySideLabel,
      points: predictedPoints,
      className: sideChartClass(base.side_label),
      preferredSide: "above" as const,
    },
    {
      label: oppositeLabel,
      points: oppositePredictedPoints,
      className: sideChartClass(oppositeLabel),
      preferredSide: "below" as const,
    },
  ];
  const actualSeries = [
    {
      label: primarySideLabel,
      points: actualPoints,
      className: sideChartClass(base.side_label),
    },
    {
      label: oppositeLabel,
      points: oppositeActualPoints,
      className: sideChartClass(oppositeLabel),
    },
  ].filter((series) => series.points.length > 0);
  const intervalValues = cases.flatMap((item) => [
    item.interval_low_future_odds_pct,
    item.interval_high_future_odds_pct,
  ]);
  const values = [
    ...predictedSeries.flatMap((series) => series.points.map((point) => point.odds)),
    ...actualSeries.flatMap((series) => series.points.map((point) => point.odds)),
    ...intervalValues,
  ].filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const paddedMin = Math.max(0, Math.floor(rawMin - 6));
  const paddedMax = Math.min(100, Math.ceil(rawMax + 6));
  const minRange = 12;
  const midpoint = (paddedMin + paddedMax) / 2;
  const minOdds = paddedMax - paddedMin < minRange ? Math.max(0, Math.floor(midpoint - minRange / 2)) : paddedMin;
  const maxOdds = paddedMax - paddedMin < minRange ? Math.min(100, Math.ceil(midpoint + minRange / 2)) : paddedMax;
  const width = 520;
  const height = 268;
  const left = 58;
  const right = 44;
  const top = 16;
  const axisLabelY = height - 3;
  const axisY = axisLabelY - 10;
  const plotBottom = axisY - 10;
  const plotWidth = width - left - right;
  const plotHeight = plotBottom - top;
  const yFor = (odds: number) => top + ((maxOdds - odds) / Math.max(maxOdds - minOdds, 1)) * plotHeight;
  const xFor = (hour: number) => left + (Math.min(Math.max(hour, 0), 24) / 24) * plotWidth;
  const lineOddsAtHour = (hour: number) => [
    ...predictedSeries.flatMap((series) => series.points.filter((point) => point.hour === hour).map((point) => point.odds)),
    ...actualSeries.flatMap((series) => series.points.filter((point) => point.hour === hour).map((point) => point.odds)),
  ];
  const pointTextProps = (hour: number, odds: number, preferredSide: "above" | "below") => {
    const x = xFor(hour);
    const isRightEdge = hour >= 22;
    const isLeftEdge = hour <= 2;
    const lineY = yFor(odds);
    const labelTop = top + 18;
    const labelBottom = plotBottom - 20;
    const labelGap = 28;
    const avoidLineYs = lineOddsAtHour(hour).map(yFor);
    const aboveCandidates = [lineY - labelGap, lineY - labelGap - 16, lineY - labelGap - 30];
    const belowCandidates = [lineY + labelGap, lineY + labelGap + 16, lineY + labelGap + 30];
    const orderedCandidates = preferredSide === "above"
      ? [...aboveCandidates, ...belowCandidates]
      : [...belowCandidates, ...aboveCandidates];
    const candidateIsClear = (candidateY: number) =>
      candidateY >= labelTop &&
      candidateY <= labelBottom &&
      avoidLineYs.every((avoidY) => Math.abs(candidateY - avoidY) >= 24);
    const y =
      orderedCandidates.find(candidateIsClear) ??
      Math.max(
        labelTop,
        Math.min(
          orderedCandidates.find((candidateY) => candidateY >= labelTop && candidateY <= labelBottom) ??
            (lineY < (labelTop + labelBottom) / 2 ? lineY + labelGap : lineY - labelGap),
          labelBottom,
        ),
      );

    return {
      x: isRightEdge ? x - 10 : isLeftEdge ? x + 10 : x,
      y,
      textAnchor: isRightEdge ? "end" : isLeftEdge ? "start" : "middle",
    } as const;
  };
  const axisLabelProps = (hour: number) => {
    const x = xFor(hour);
    if (hour === 0) return { x: x + 4, textAnchor: "start" } as const;
    if (hour === 24) return { x: x - 4, textAnchor: "end" } as const;
    return { x, textAnchor: "middle" } as const;
  };
  const linePoints = (points: Array<{ hour: number; odds: number }>) =>
    points.map((point) => `${xFor(point.hour)},${yFor(point.odds)}`).join(" ");
  const shortWindowCase = cases.find((item) => item.window === "12h") ?? cases[0];
  const longWindowCase = cases.find((item) => item.window === "24h") ?? cases.at(-1) ?? cases[0];
  const forecastInsightRows = [
    whalePressureInsight(base, allCases),
    netSupportInsight(base),
    trustedActivityInsight(base),
    entryStrengthInsight(base),
    {
      label: "12h Forecast",
      value: marketLeanLabel(shortWindowCase),
      detail: forecastMoveDetail(shortWindowCase),
    },
    {
      label: "24h Forecast",
      value: marketLeanLabel(longWindowCase),
      detail: forecastMoveDetail(longWindowCase),
    },
  ];
  return (
    <div className="market-ml-chart-shell">
      <div className="market-ml-chart-summary">
        <span>Whale entered {formatDateTime(entryTime)}</span>
        <span>
          Starting market probability {primarySideLabel} {formatOddsPercent(entryOdds)} / {oppositeLabel}{" "}
          {formatOddsPercent(complementProbability(entryOdds))}
        </span>
        {forecastSummaries.map((summary) => (
          <span key={summary}>{summary}</span>
        ))}
      </div>
      <div className="market-ml-chart-legend">
        {predictedSeries.map((series) => (
          <span key={`legend-${series.label}`}>
            <i className={`market-ml-legend-predicted ${series.className}`} /> {series.label}
          </span>
        ))}
        {actualPoints.length > 0 && <span><i className="market-ml-legend-actual" /> Actual probability</span>}
      </div>
      <svg className="market-ml-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Whale-driven probability forecast">
        {[minOdds, Math.round((minOdds + maxOdds) / 2), maxOdds].map((odds) => (
          <g key={`grid-${odds}`}>
            <line x1={left} x2={width - right} y1={yFor(odds)} y2={yFor(odds)} />
            <text x={8} y={yFor(odds) + 4}>{odds}%</text>
          </g>
        ))}
        <line className="market-ml-axis-line" x1={left} x2={width - right} y1={axisY} y2={axisY} />
        {[0, 12, 24].map((hour) => (
          <text className="market-ml-axis-label" key={`axis-${hour}`} {...axisLabelProps(hour)} y={axisLabelY}>
            {hour === 0 ? "Start" : `${hour}h later`}
          </text>
        ))}
        {predictedSeries.map((series) => (
          <polyline
            className={`market-ml-chart-line ${series.className}`}
            key={`forecast-line-${series.label}`}
            points={linePoints(series.points)}
          />
        ))}
        {actualSeries.map((series) => (
          <polyline
            className={`market-ml-chart-line market-ml-chart-actual-line ${series.className}`}
            key={`actual-line-${series.label}`}
            points={linePoints(series.points)}
          />
        ))}
        {predictedSeries.flatMap((series) =>
          series.points.map((point) => (
            <g key={`${series.label}-${point.label}-${point.hour}`}>
              <circle className={series.className} cx={xFor(point.hour)} cy={yFor(point.odds)} r={point.hour === 0 ? 4 : 6} />
              <text className="market-ml-point-label" {...pointTextProps(point.hour, point.odds, series.preferredSide)}>
                {series.label} {formatOddsPercent(point.odds)}
              </text>
            </g>
          )),
        )}
        {actualSeries.flatMap((series) =>
          series.points.map((point) => (
            <g key={`actual-${series.label}-${point.label}-${point.hour}`}>
              <circle className={`market-ml-chart-actual-dot ${series.className}`} cx={xFor(point.hour)} cy={yFor(point.odds)} r={4} />
            </g>
          )),
        )}
      </svg>
      <div className="market-ml-chart-insights" aria-label="Whale trend forecast summary">
        {forecastInsightRows.map((row) => (
          <div className="market-ml-chart-insight" key={`${row.label}-${row.value}`}>
            <span>{row.label}</span>
            <strong>{row.value}</strong>
            <p>{row.detail}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function WhaleLeanPanel({ cases }: { cases: MarketProfileMlPredictionCase[] }) {
  const summary = whaleLeanSummary(cases);
  if (summary.sides.length === 0) {
    return (
      <div className="market-ml-whale-lean-panel">
        <div>
          <p className="market-ml-side-label">Whale Lean</p>
          <h4>Whale lean is mixed</h4>
          <span>No clear side has stronger trusted whale support yet.</span>
        </div>
      </div>
    );
  }

  return (
    <div className="market-ml-whale-lean-panel">
      <div className="market-ml-whale-lean-header">
        <div>
          <p className="market-ml-side-label">Whale Lean</p>
          <h4>{summary.leader ? `Whales leaning ${summary.leader.label}` : "Whale lean is mixed"}</h4>
          <span>
            {summary.leader
              ? `Trusted whales are showing more support for ${summary.leader.label}.`
              : "No clear side has stronger trusted whale support yet."}
          </span>
        </div>
        <span className="market-ml-whale-lean-pill">Whale support</span>
      </div>
      <div className="market-ml-whale-lean-bars">
        {summary.sides.map((side) => {
          const width = summary.maxScore > 0 ? Math.max(6, Math.round((side.score / summary.maxScore) * 100)) : 0;
          return (
            <div className="market-ml-whale-lean-row" key={side.key}>
              <span>{side.label}</span>
              <div className="market-ml-whale-lean-track" aria-hidden="true">
                <i style={{ width: `${width}%` }} />
              </div>
              <strong>{whaleSupportLabel(side.score, summary.maxScore)}</strong>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function MarketPredictionOutcomeSummary({ cases }: { cases: MarketProfileMlPredictionCase[] }) {
  if (cases.length === 0) return null;

  return (
    <div className="market-ml-outcome-summary">
      {cases.map((item) => {
        const comparison = completedValidation(item);
        const predictedValue = comparison?.model_predicted_future_odds_pct ?? modelFutureOdds(item);
        const predictedRows = binaryProbabilityRows(item, predictedValue);
        return (
          <article className="market-ml-outcome-card" key={`outcome-${item.window}-${item.side_label}`}>
            <div className="market-ml-outcome-card-header">
              <span>Market probability</span>
              <strong>{item.window} forecast</strong>
            </div>
            <div className="market-ml-model-probability-grid">
              {predictedRows.map((row) => (
                <span key={`predicted-${item.window}-${row.label}`}>
                  {row.label} <strong>{formatOddsPercent(row.value)}</strong>
                </span>
              ))}
            </div>
            <div className="market-ml-outcome-footer">
              <span>{forecastMarketSummary(item)}</span>
              <span>Accuracy: {historicalValidationDescription(item)}</span>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function MarketPredictionValidationSummary({ summary }: { summary?: MarketProfileMlPredictionValidationSummary | null }) {
  if (!summary || summary.sample_size <= 0) return null;
  return (
    <div className="market-ml-validation-summary">
      <div>
        <p className="market-ml-section-label">12h accuracy check</p>
        <h3>{summary.direction_match_rate_pct.toFixed(1)}% matched actual direction</h3>
        <span>
          {summary.summary_label ?? `Last ${summary.sample_size} completed 12h forecasts`} checked against actual Polymarket probability.
        </span>
      </div>
      <div className="market-ml-validation-stats">
        <span>{summary.direction_match_count}/{summary.sample_size} matched</span>
        <span>Avg error {formatPercentagePointMagnitude(summary.avg_absolute_error_pts)}</span>
      </div>
    </div>
  );
}

function TopMarketWhaleRow({ whale, rank }: { whale: MarketProfileTopWhale; rank: number }) {
  const identity = deriveUserIdentity(whale);
  const latestAction = [whale.latest_side, whale.latest_outcome_label].filter(Boolean).map(formatLabel).join(" ");

  return (
    <article className="market-ml-whale-row">
      <div className="market-ml-whale-rank">#{rank}</div>
      <div className="market-ml-whale-main">
        <div className="market-ml-whale-title">
          <Link to={`/users/${whale.user_id}`}>{identity.primary}</Link>
          <span>{deriveWhaleTierLabel(whale)}</span>
        </div>
        <p>{identity.secondary}</p>
        <div className="market-ml-summary-row">
          <span>Trust {formatTrustScorePercent(whale.trust_score)}</span>
          <span>Market volume {formatCurrency(whale.total_notional)}</span>
          <span>Trades {formatCompactNumber(whale.trade_count, 0)}</span>
          <span>
            Buys {formatCompactNumber(whale.buy_trade_count, 0)} | Sells {formatCompactNumber(whale.sell_trade_count, 0)}
          </span>
        </div>
      </div>
      <div className="market-ml-whale-meta">
        <span>Latest {latestAction || "Trade"}</span>
        <span>{formatDateTime(whale.latest_trade_time)}</span>
        <span>Avg price {formatPercent(whale.avg_trade_price)}</span>
      </div>
    </article>
  );
}

function emptyTopWhales(marketSlug: string): MarketProfileTopWhales {
  return {
    market_slug: marketSlug,
    snapshot_time: null,
    scoring_version: null,
    count: 0,
    items: [],
  };
}

function emptyMlTrend(marketSlug: string): MarketProfileMlPredictionTrend {
  return {
    available: false,
    reason: "missing_market_slug",
    market_slug: marketSlug,
    windows: { "12h": [], "24h": [] },
  };
}

function TopMarketWhalesPanel({
  marketSlug,
  topWhales,
  error,
}: {
  marketSlug: string;
  topWhales?: MarketProfileTopWhales | null;
  error?: string | null;
}) {
  const topWhalesPayload = topWhales ?? emptyTopWhales(marketSlug);
  const whales = topWhales?.items ?? [];

  if (error) {
    return <div className="market-ml-empty">Unable to load top whales: {error}</div>;
  }

  if (whales.length === 0) {
    return (
      <div className="market-ml-empty">
        <strong>No ranked whales found for this market yet.</strong>
        <span>Top-whale ranking appears after scored whale wallets have trades in this market.</span>
      </div>
    );
  }

  return (
    <div className="market-ml-whales-panel">
      <div className="market-ml-whales-summary">
        <div>
          <p className="market-ml-side-label">Market whale ranking</p>
          <h3>Top 5 whales by trust score</h3>
        </div>
        <div className="market-ml-live-stats">
          <span>{formatCompactNumber(topWhalesPayload.count, 0)} ranked whales</span>
          <span>Scores {formatDateTime(topWhalesPayload.snapshot_time)}</span>
        </div>
      </div>
      <div className="market-ml-whale-list">
        {whales.map((whale, index) => (
          <TopMarketWhaleRow key={whale.user_id} whale={whale} rank={index + 1} />
        ))}
      </div>
    </div>
  );
}

function MarketMlPredictionTrendPanel({
  marketSlug,
  trendPayload,
  topWhales,
  loading,
  error,
  preferredSideLabel,
  outcomeProbabilities,
  price,
  odds,
}: {
  marketSlug: string;
  trendPayload?: MarketProfileMlPredictionTrend | null;
  topWhales?: MarketProfileTopWhales | null;
  loading?: boolean;
  error?: string | null;
  preferredSideLabel?: string | null;
  outcomeProbabilities?: MarketOutcomeProbability[] | null;
  price: number | null;
  odds: number | null;
}) {
  const trend = trendPayload ?? emptyMlTrend(marketSlug);
  const cases = profileTrendCases(trend);
  const primaryCases = primaryTrendCases(cases, trend?.primary_side_label ?? preferredSideLabel);
  const [activeTab, setActiveTab] = useState<"trend" | "whales">("trend");
  const marketStatus =
    trend?.live_polymarket_closed === true ? "Closed" : trend?.live_polymarket_closed === false ? "Open" : "Status pending";
  const marketStatusClass =
    trend?.live_polymarket_closed === true ? "is-closed" : trend?.live_polymarket_closed === false ? "is-open" : "";

  return (
    <section className="card profile-card market-ml-trend-card">
      <div className="card-header market-ml-header">
        <div>
          <p className="card-label">ML Trend</p>
          <h2>{activeTab === "whales" ? "Top Market Whales" : "Whale-Based Forecast"}</h2>
          <p className="card-subtext">
            {activeTab === "whales"
              ? "Top 5 whales active in this market, ranked by latest trust score."
              : "Shows how whale activity may move market probability over the next 12 and 24 hours."}
          </p>
        </div>
        <div className="market-ml-actions">
          <span className={`market-ml-status-pill ${marketStatusClass}`}>
            Market {marketStatus}
          </span>
          <div className="market-ml-tabs" role="tablist" aria-label="Market ML profile views">
            <button
              type="button"
              className={activeTab === "trend" ? "active" : ""}
              onClick={() => setActiveTab("trend")}
              role="tab"
              aria-selected={activeTab === "trend"}
            >
              Forecast
            </button>
            <button
              type="button"
              className={activeTab === "whales" ? "active" : ""}
              onClick={() => setActiveTab("whales")}
              role="tab"
              aria-selected={activeTab === "whales"}
            >
              Top Whales
            </button>
          </div>
        </div>
      </div>

      {activeTab === "whales" ? (
        <TopMarketWhalesPanel marketSlug={marketSlug} topWhales={topWhales} error={error} />
      ) : loading ? (
        <div className="market-ml-empty">Loading prediction trend...</div>
      ) : error ? (
        <div className="market-ml-empty">Unable to load prediction trend: {error}</div>
      ) : cases.length === 0 ? (
        <div className="market-ml-empty">
          <strong>No 12h/24h ML prediction snapshot for this market yet.</strong>
          <span>{formatLabel(trend?.prediction_status ?? trend?.reason ?? "market_not_in_ml_prediction_snapshot")}</span>
        </div>
      ) : (
        <>
          <div className="market-ml-chart-layout">
            <div>
              <h3>Whale-driven market forecast</h3>
              <CurrentMarketProbabilityPanel outcomes={outcomeProbabilities} price={price} odds={odds} />
              <WhaleLeanPanel cases={cases} />
              <MarketPredictionOutcomeSummary cases={primaryCases} />
            </div>
            <div className="market-ml-chart-panel">
              <MarketPredictionTrendChart cases={primaryCases} allCases={cases} />
              <MarketPredictionValidationSummary summary={trend?.recent_12h_validation} />
            </div>
          </div>
        </>
      )}
    </section>
  );
}

export default function MarketProfile() {
  const { marketId } = useParams();
  const marketSlug = marketId ?? "";
  const { isMarketFollowed, toggleMarket } = useWatchlist();
  const initialMarketFull = useMemo(
    () => (marketSlug ? getCachedMarketProfileFull(marketSlug, 5) : null),
    [marketSlug],
  );
  const loadMarket = useCallback(() => {
    if (!marketSlug) {
      throw new Error("Missing market slug");
    }
    return fetchMarketProfileFull(marketSlug, 5);
  }, [marketSlug]);
  const { data: fullProfile, loading, error } = useApiData(loadMarket, {
    keepPreviousData: true,
    initialData: initialMarketFull,
    resetKey: `market-${marketSlug}`,
  });
  const data = fullProfile?.profile ?? null;
  const mlTrend = fullProfile?.ml_prediction_trend ?? data?.ml_prediction_trend ?? null;
  const topWhales = fullProfile?.top_whales ?? data?.top_whales ?? null;
  const externalMarketUrl = data ? polymarketMarketUrl(data.market_url, data.market_slug) : null;

  return (
    <div className="page market-profile-page">
      <header className="hero market-hero">
        <div className="hero-top-row">
          <div>
            <p className="eyebrow">Market Profile</p>
            <h1 className="market-title">
              {externalMarketUrl ? (
                <a href={externalMarketUrl} target="_blank" rel="noreferrer">
                  {data?.question ?? marketSlug}
                </a>
              ) : (
                data?.question ?? marketSlug
              )}
            </h1>
            <p className="hero-text">Latest dashboard-backed market snapshot and whale concentration details.</p>
          </div>

          <div className="hero-action-stack">
            <FollowButton
              isFollowing={isMarketFollowed(marketSlug)}
              onToggle={() => toggleMarket(marketSlug)}
            />
          </div>
        </div>

        <div className="hero-actions">
          <Link to="/leaderboard" className="table-link back-link">
            ← Back to leaderboard
          </Link>

          {data && (
            <div className="hero-pills">
              <span className="hero-pill">{data.market_slug}</span>
              <span className="hero-pill">Whale Traders {data.whale_count}</span>
              <span className="hero-pill">Trusted Whales {data.trusted_whale_count}</span>
            </div>
          )}
        </div>
      </header>

      {loading && <section className="status-panel">Loading market profile...</section>}
      {error && <section className="status-panel error-panel">{error}</section>}

      {!loading && !error && data && (
        <>
          <section className="market-summary-card">
            <div className="market-summary-left">
              <p className="summary-label">Market Details</p>

              <div className="summary-stats">
                <div className="stat-chip">
                  <span className="stat-chip-label">Volume</span>
                  <strong>{formatCurrency(data.volume)}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Depth</span>
                  <strong>{data.orderbook_depth?.toLocaleString() ?? "--"}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Whale Traders</span>
                  <strong>{data.whale_count}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Trusted Whales</span>
                  <strong>{data.trusted_whale_count}</strong>
                </div>
                <div className="stat-chip stat-chip-wide">
                  <span className="stat-chip-label">Read Time</span>
                  <strong>{formatDateTime(data.read_time)}</strong>
                </div>
              </div>
            </div>
          </section>

          <MarketMlPredictionTrendPanel
            marketSlug={data.market_slug}
            trendPayload={mlTrend}
            topWhales={topWhales}
            loading={loading}
            error={error}
            preferredSideLabel={data.primary_side_label ?? data.selected_side_label}
            outcomeProbabilities={data.outcome_probabilities}
            price={data.price}
            odds={data.odds}
          />
        </>
      )}
    </div>
  );
}
