const TRUST_SCORE_MAX = 1.15;

function formatScorePercent(value: number | null | undefined, denominator: number) {
  if (typeof value !== "number" || Number.isNaN(value)) return "--";
  return `${((value / denominator) * 100).toFixed(1)}%`;
}

export function formatTrustScorePercent(value: number | null | undefined) {
  return formatScorePercent(value, TRUST_SCORE_MAX);
}

export function formatProfitabilityScorePercent(value: number | null | undefined) {
  return formatScorePercent(value, 1);
}
