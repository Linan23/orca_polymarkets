export type WatchlistState = {
  users: number[];
  markets: string[];
};

export const WATCHLIST_STORAGE_KEY = "orca.following.v1";
const WATCHLIST_EVENT = "orca:watchlist-updated";

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

function normalizeWatchlist(value: unknown): WatchlistState {
  if (!value || typeof value !== "object") {
    return EMPTY_WATCHLIST;
  }

  const record = value as Record<string, unknown>;
  return {
    users: uniqueNumbers(record.users),
    markets: uniqueMarketSlugs(record.markets),
  };
}

export function readWatchlist(): WatchlistState {
  if (typeof window === "undefined") {
    return EMPTY_WATCHLIST;
  }

  const rawValue = window.localStorage.getItem(WATCHLIST_STORAGE_KEY);
  if (!rawValue) {
    return EMPTY_WATCHLIST;
  }

  try {
    return normalizeWatchlist(JSON.parse(rawValue));
  } catch {
    return EMPTY_WATCHLIST;
  }
}

function writeWatchlist(nextState: WatchlistState) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify(nextState));
  window.dispatchEvent(new Event(WATCHLIST_EVENT));
}

export function subscribeWatchlist(listener: () => void): () => void {
  if (typeof window === "undefined") {
    return () => undefined;
  }

  const onStorage = (event: Event) => {
    const storageEvent = event as StorageEvent;
    if (storageEvent.type === "storage" && storageEvent.key && storageEvent.key !== WATCHLIST_STORAGE_KEY) {
      return;
    }
    listener();
  };

  window.addEventListener("storage", onStorage);
  window.addEventListener(WATCHLIST_EVENT, onStorage);

  return () => {
    window.removeEventListener("storage", onStorage);
    window.removeEventListener(WATCHLIST_EVENT, onStorage);
  };
}

export function toggleWatchlistUser(userId: number) {
  const current = readWatchlist();
  const exists = current.users.includes(userId);
  writeWatchlist({
    ...current,
    users: exists ? current.users.filter((value) => value !== userId) : [userId, ...current.users],
  });
}

export function toggleWatchlistMarket(marketSlug: string) {
  const normalized = marketSlug.trim().toLowerCase();
  if (!normalized) return;
  const current = readWatchlist();
  const exists = current.markets.includes(normalized);
  writeWatchlist({
    ...current,
    markets: exists ? current.markets.filter((value) => value !== normalized) : [normalized, ...current.markets],
  });
}

export function removeWatchlistUser(userId: number) {
  const current = readWatchlist();
  writeWatchlist({
    ...current,
    users: current.users.filter((value) => value !== userId),
  });
}

export function removeWatchlistMarket(marketSlug: string) {
  const normalized = marketSlug.trim().toLowerCase();
  const current = readWatchlist();
  writeWatchlist({
    ...current,
    markets: current.markets.filter((value) => value !== normalized),
  });
}
