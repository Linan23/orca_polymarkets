function formatScaledValue(
  value: number | null | undefined,
  suffix: string,
  maximumFractionDigits = 1,
) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return `${new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(value * 100)}${suffix}`;
}

export function formatProbabilityPercent(
  value: number | null | undefined,
  maximumFractionDigits = 1,
) {
  return formatScaledValue(value, "%", maximumFractionDigits);
}

export function formatContractPrice(
  value: number | null | undefined,
  maximumFractionDigits = 1,
) {
  return formatScaledValue(value, "c", maximumFractionDigits);
}

export function formatOpposingProbabilityPercent(
  value: number | null | undefined,
  maximumFractionDigits = 1,
) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  const normalized = Math.min(Math.max(value, 0), 1);
  return formatProbabilityPercent(1 - normalized, maximumFractionDigits);
}

export function formatOpposingContractPrice(
  value: number | null | undefined,
  maximumFractionDigits = 1,
) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  const normalized = Math.min(Math.max(value, 0), 1);
  return formatContractPrice(1 - normalized, maximumFractionDigits);
}

export function formatContractPriceDelta(
  value: number | null | undefined,
  maximumFractionDigits = 1,
) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  const cents = value * 100;
  return `${cents > 0 ? "+" : ""}${new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(cents)}c`;
}
