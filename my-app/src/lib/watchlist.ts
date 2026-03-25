import type { WatchlistState } from "./api";

export const LEGACY_WATCHLIST_STORAGE_KEY = "orca.following.v1";

const EMPTY_WATCHLIST: WatchlistState = {
  users: [],
  markets: [],
};

function uniqueNumbers(values: unknown): number[] {
  if (!Array.isArray(values)) return [];
  const seen = new Set<number>();
  const items: number[] = [];
  for (const value of values) {
    const parsed = typeof value === "number" ? value : Number(value);
    if (!Number.isInteger(parsed) || seen.has(parsed)) continue;
    seen.add(parsed);
    items.push(parsed);
  }
  return items;
}

function uniqueMarketSlugs(values: unknown): string[] {
  if (!Array.isArray(values)) return [];
  const seen = new Set<string>();
  const items: string[] = [];
  for (const value of values) {
    if (typeof value !== "string") continue;
    const normalized = value.trim().toLowerCase();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    items.push(normalized);
  }
  return items;
}

export function readLegacyWatchlist(): WatchlistState {
  if (typeof window === "undefined") {
    return EMPTY_WATCHLIST;
  }

  const rawValue = window.localStorage.getItem(LEGACY_WATCHLIST_STORAGE_KEY);
  if (!rawValue) {
    return EMPTY_WATCHLIST;
  }

  try {
    const parsed = JSON.parse(rawValue) as Record<string, unknown>;
    return {
      users: uniqueNumbers(parsed.users),
      markets: uniqueMarketSlugs(parsed.markets),
    };
  } catch {
    return EMPTY_WATCHLIST;
  }
}

export function clearLegacyWatchlist() {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.removeItem(LEGACY_WATCHLIST_STORAGE_KEY);
}
