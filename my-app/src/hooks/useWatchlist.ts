import { useEffect, useState } from "react";
import {
  type WatchlistState,
  readWatchlist,
  removeWatchlistMarket,
  removeWatchlistUser,
  subscribeWatchlist,
  toggleWatchlistMarket,
  toggleWatchlistUser,
} from "../lib/watchlist";

export function useWatchlist() {
  const [watchlist, setWatchlist] = useState<WatchlistState>(() => readWatchlist());

  useEffect(() => subscribeWatchlist(() => setWatchlist(readWatchlist())), []);

  return {
    watchlist,
    isUserFollowed: (userId: number) => watchlist.users.includes(userId),
    isMarketFollowed: (marketSlug: string) => watchlist.markets.includes(marketSlug.trim().toLowerCase()),
    toggleUser: toggleWatchlistUser,
    toggleMarket: toggleWatchlistMarket,
    removeUser: removeWatchlistUser,
    removeMarket: removeWatchlistMarket,
  };
}
