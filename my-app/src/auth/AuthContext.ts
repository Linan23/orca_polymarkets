import { createContext, useContext } from "react";
import type {
  AccountPreferences,
  AccountPreferencesPatch,
  AuthAccount,
  LoginPayload,
  SignUpPayload,
  WatchlistState,
} from "../lib/api";

export type AuthContextValue = {
  loading: boolean;
  isAuthenticated: boolean;
  account: AuthAccount | null;
  watchlist: WatchlistState;
  preferences: AccountPreferences;
  refresh: () => Promise<void>;
  login: (payload: LoginPayload) => Promise<void>;
  signup: (payload: SignUpPayload) => Promise<void>;
  logout: () => Promise<void>;
  toggleUserFollow: (userId: number) => Promise<void>;
  toggleMarketFollow: (marketSlug: string) => Promise<void>;
  removeUserFollow: (userId: number) => Promise<void>;
  removeMarketFollow: (marketSlug: string) => Promise<void>;
  updatePreferences: (patch: AccountPreferencesPatch) => Promise<void>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
  const value = useContext(AuthContext);
  if (!value) {
    throw new Error("useAuth must be used inside AuthProvider.");
  }
  return value;
}
