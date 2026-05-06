import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import FollowButton from "../components/FollowButton";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  fetchMarketProfile,
  fetchMarketProfileMlTrend,
  fetchMarketProfileTopWhales,
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

function formatSignedPoints(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)} points`;
}

function formatPointMagnitude(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${Math.abs(value).toFixed(digits)} points`;
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

function forecastDirectionTag(item: MarketProfileMlPredictionCase) {
  if (item.predicted_direction === "up") return "Expected to rise";
  if (item.predicted_direction === "down") return "Expected to fall";
  return "Expected to stay near current odds";
}

function historicalValidationTag(item: MarketProfileMlPredictionCase) {
  const trainedAccuracyPct = item.validation_accuracy_pct ?? item.direction_signal_accuracy_pct;
  if (
    item.historical_validation_tier === "trained_strong_confidence" ||
    item.historical_validation_tier === "trained_watch_confidence"
  ) {
    return typeof trainedAccuracyPct === "number" ? `${trainedAccuracyPct.toFixed(1)}%` : "Pending";
  }
  if (item.historical_validation_tier === "trained_low_confidence") {
    return typeof trainedAccuracyPct === "number" ? `Low ${trainedAccuracyPct.toFixed(1)}%` : "Low validation";
  }
  if (item.historical_validation_tier === "high_confidence_historical_slice") {
    const pct = item.historical_validation_direction_match_pct;
    return typeof pct === "number" ? `${pct.toFixed(1)}%` : "Past match available";
  }
  if (item.historical_validation_tier === "strong_model_signal") return "High confidence";
  if (item.historical_validation_tier === "review_only") return "Needs review";
  if (item.historical_validation_tier === "insufficient_validated_accuracy") return "Limited past proof";
  if (item.direction_signal_tier === "watch") return "Watch closely";
  if (item.direction_signal_tier === "abstain") return "Needs review";
  return formatLabel(item.direction_signal_tier);
}

function modelFutureOdds(item: MarketProfileMlPredictionCase) {
  return item.model_predicted_future_odds_pct ?? item.predicted_future_odds_pct ?? null;
}

function modelDelta(item: MarketProfileMlPredictionCase) {
  return item.model_predicted_delta_pts ?? item.predicted_delta_pts ?? null;
}

function predictionForecastSummary(item: MarketProfileMlPredictionCase) {
  return `${item.window} forecast ${formatOddsPercent(modelFutureOdds(item))} (${formatSignedPoints(modelDelta(item))})`;
}

function oppositeSideLabel(value: string | null | undefined) {
  const normalized = normalizeSideLabel(value);
  if (normalized === "yes") return "No";
  if (normalized === "no") return "Yes";
  if (normalized === "up") return "Down";
  if (normalized === "down") return "Up";
  return "Other";
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
    { label: oppositeSideLabel(item.side_label), value: Math.min(Math.max(100 - value, 0), 100) },
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

function MarketPredictionTrendChart({ cases }: { cases: MarketProfileMlPredictionCase[] }) {
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
  const intervalValues = cases.flatMap((item) => [
    item.interval_low_future_odds_pct,
    item.interval_high_future_odds_pct,
  ]);
  const values = [...predictedPoints.map((point) => point.odds), ...actualPoints.map((point) => point.odds), ...intervalValues].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value),
  );
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const paddedMin = Math.max(0, Math.floor(rawMin - 6));
  const paddedMax = Math.min(100, Math.ceil(rawMax + 6));
  const minRange = 12;
  const midpoint = (paddedMin + paddedMax) / 2;
  const minOdds = paddedMax - paddedMin < minRange ? Math.max(0, Math.floor(midpoint - minRange / 2)) : paddedMin;
  const maxOdds = paddedMax - paddedMin < minRange ? Math.min(100, Math.ceil(midpoint + minRange / 2)) : paddedMax;
  const width = 520;
  const height = 320;
  const left = 58;
  const right = 44;
  const top = 24;
  const axisLabelY = height - 10;
  const axisY = axisLabelY - 18;
  const plotBottom = axisY - 26;
  const plotWidth = width - left - right;
  const plotHeight = plotBottom - top;
  const yFor = (odds: number) => top + ((maxOdds - odds) / Math.max(maxOdds - minOdds, 1)) * plotHeight;
  const xFor = (hour: number) => left + (Math.min(Math.max(hour, 0), 24) / 24) * plotWidth;
  const lineOddsAtHour = (hour: number) => [
    ...predictedPoints.filter((point) => point.hour === hour).map((point) => point.odds),
    ...actualPoints.filter((point) => point.hour === hour).map((point) => point.odds),
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
  const predictedLinePoints = predictedPoints.map((point) => `${xFor(point.hour)},${yFor(point.odds)}`).join(" ");
  const actualLinePoints = actualPoints.map((point) => `${xFor(point.hour)},${yFor(point.odds)}`).join(" ");
  return (
    <div className="market-ml-chart-shell">
      <div className="market-ml-chart-summary">
        <span>Whale entered {formatDateTime(entryTime)}</span>
        <span>Starting {formatLabel(base.side_label)} odds {formatOddsPercent(entryOdds)}</span>
        {forecastSummaries.map((summary) => (
          <span key={summary}>{summary}</span>
        ))}
      </div>
      {actualPoints.length > 0 && (
        <div className="market-ml-chart-legend">
          <span><i className="market-ml-legend-predicted" /> Model forecast</span>
          <span><i className="market-ml-legend-actual" /> Actual odds</span>
        </div>
      )}
      <svg className="market-ml-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="ML prediction trend">
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
        <polyline className="market-ml-chart-line" points={predictedLinePoints} />
        {actualLinePoints && <polyline className="market-ml-chart-line market-ml-chart-actual-line" points={actualLinePoints} />}
        {predictedPoints.map((point) => (
          <g key={`${point.label}-${point.hour}`}>
            <circle cx={xFor(point.hour)} cy={yFor(point.odds)} r={point.hour === 0 ? 4 : 6} />
            <text className="market-ml-point-label" {...pointTextProps(point.hour, point.odds, "above")}>
              {formatOddsPercent(point.odds)}
            </text>
          </g>
        ))}
        {actualPoints.map((point) => (
          <g key={`actual-${point.label}-${point.hour}`}>
            <circle className="market-ml-chart-actual-dot" cx={xFor(point.hour)} cy={yFor(point.odds)} r={5} />
            <text className="market-ml-point-label" {...pointTextProps(point.hour, point.odds, "below")}>
              {formatOddsPercent(point.odds)}
            </text>
          </g>
        ))}
      </svg>
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
        const hasValidation = comparison !== null;
        return (
          <article className="market-ml-outcome-card" key={`outcome-${item.window}-${item.side_label}`}>
            <div className="market-ml-outcome-card-header">
              <span>{item.window}</span>
              <strong>Expected odds</strong>
            </div>
            <div className="market-ml-model-probability-grid">
              {predictedRows.map((row) => (
                <span key={`predicted-${item.window}-${row.label}`}>
                  {row.label} <strong>{formatOddsPercent(row.value)}</strong>
                </span>
              ))}
            </div>
            <div className="market-ml-outcome-footer">
              <span>Direction: {forecastDirectionTag(item)}</span>
              <span>Accuracy: {historicalValidationTag(item)}</span>
              <span>Odds change: {formatSignedPoints(comparison?.model_predicted_delta_pts ?? modelDelta(item))}</span>
              {hasValidation && <span>Past error: {formatPointMagnitude(comparison?.prediction_absolute_error_pts ?? item.prediction_absolute_error_pts)}</span>}
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
        <p className="market-ml-section-label">12h validation</p>
        <h3>{summary.direction_match_rate_pct.toFixed(1)}% trend match</h3>
        <span>
          {summary.summary_label ?? `Last ${summary.sample_size} completed 12h checks`} against actual Polymarket odds.
        </span>
      </div>
      <div className="market-ml-validation-stats">
        <span>{summary.direction_match_count}/{summary.sample_size} matched</span>
        <span>Avg error {formatPointMagnitude(summary.avg_absolute_error_pts)}</span>
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

function TopMarketWhalesPanel({ marketSlug }: { marketSlug: string }) {
  const loadTopWhales = useCallback(
    () => (marketSlug ? fetchMarketProfileTopWhales(marketSlug, 5) : Promise.resolve(emptyTopWhales(marketSlug))),
    [marketSlug],
  );
  const { data: topWhales, loading, error } = useApiData(loadTopWhales);
  const whales = topWhales?.items ?? [];

  if (loading) {
    return <div className="market-ml-empty">Loading top whales...</div>;
  }

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
          <span>{formatCompactNumber(topWhales?.count, 0)} ranked whales</span>
          <span>Scores {formatDateTime(topWhales?.snapshot_time)}</span>
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
  preferredSideLabel,
  outcomeProbabilities,
  price,
  odds,
}: {
  marketSlug: string;
  preferredSideLabel?: string | null;
  outcomeProbabilities?: MarketOutcomeProbability[] | null;
  price: number | null;
  odds: number | null;
}) {
  const loadTrend = useCallback(
    () => (marketSlug ? fetchMarketProfileMlTrend(marketSlug) : Promise.resolve(emptyMlTrend(marketSlug))),
    [marketSlug],
  );
  const { data: trendPayload, loading: trendLoading, error: trendError } = useApiData(loadTrend);
  const trend = trendPayload ?? undefined;
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
              : "Shows when whales entered, current odds, and where the model expects odds to move in 12 and 24 hours."}
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
        <TopMarketWhalesPanel marketSlug={marketSlug} />
      ) : trendLoading ? (
        <div className="market-ml-empty">Loading prediction trend...</div>
      ) : trendError ? (
        <div className="market-ml-empty">Unable to load prediction trend: {trendError}</div>
      ) : cases.length === 0 ? (
        <div className="market-ml-empty">
          <strong>No 12h/24h ML prediction snapshot for this market yet.</strong>
          <span>{formatLabel(trend?.prediction_status ?? trend?.reason ?? "market_not_in_ml_prediction_snapshot")}</span>
        </div>
      ) : (
        <>
          <div className="market-ml-chart-layout">
            <div>
              <h3>{formatLabel(primaryCases[0]?.side_label)} probability trend</h3>
              <CurrentMarketProbabilityPanel outcomes={outcomeProbabilities} price={price} odds={odds} />
              <MarketPredictionOutcomeSummary cases={primaryCases} />
            </div>
            <div className="market-ml-chart-panel">
              <MarketPredictionTrendChart cases={primaryCases} />
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
  const loadMarket = useCallback(() => fetchMarketProfile(marketSlug), [marketSlug]);
  const { data, loading, error } = useApiData(loadMarket);
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
