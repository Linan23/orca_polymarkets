type UserIdentitySource = {
  user_id?: number;
  external_user_ref?: string | null;
  wallet_address?: string | null;
  preferred_username?: string | null;
  display_label?: string | null;
};

type WhaleStatusSource = {
  is_whale?: boolean | null;
  is_trusted_whale?: boolean | null;
  is_likely_insider?: boolean | null;
  trust_score?: number | null;
  sample_trade_count?: number | null;
  entry_trade_count?: number | null;
};

export type WhaleTier = "trusted" | "whale" | "potential" | "standard";

const POTENTIAL_WHALE_MIN_TRUST_SCORE = 1.08;
const POTENTIAL_WHALE_MIN_ACTIVITY_COUNT = 5;

function looksGeneratedDisplayLabel(value: string | null | undefined) {
  if (!value || !value.includes("-")) return false;
  const parts = value.split("-");
  return (
    parts.length === 2 &&
    parts.every((part) => /^[A-Z][a-z]+$/.test(part))
  );
}

function shortenWalletAddress(value: string | null | undefined) {
  if (!value) return null;
  const normalized = value.trim();
  if (!/^0x[a-fA-F0-9]{8,}$/.test(normalized)) return normalized;
  return `${normalized.slice(0, 6)}...${normalized.slice(-4)}`;
}

export function deriveUserIdentity(user: UserIdentitySource) {
  const walletIdentity = user.wallet_address ?? user.external_user_ref ?? null;
  const preferredUsername =
    user.preferred_username && user.preferred_username !== walletIdentity
      ? user.preferred_username
      : null;
  const trustedLabel =
    user.display_label && !looksGeneratedDisplayLabel(user.display_label) && user.display_label !== walletIdentity
      ? user.display_label
      : null;

  const primary =
    preferredUsername ??
    trustedLabel ??
    shortenWalletAddress(walletIdentity) ??
    user.external_user_ref ??
    (typeof user.user_id === "number" ? `User ${user.user_id}` : "Trader");

  const secondary = walletIdentity ?? trustedLabel ?? "Wallet not available";

  return {
    primary,
    secondary,
    preferredUsername,
    trustedLabel,
    walletIdentity,
  };
}

export function userIdentitySearchTokens(user: UserIdentitySource) {
  const identity = deriveUserIdentity(user);
  return [
    identity.primary,
    identity.secondary,
    user.preferred_username,
    user.display_label,
    user.wallet_address,
    user.external_user_ref,
    typeof user.user_id === "number" ? String(user.user_id) : null,
  ]
    .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
    .map((value) => value.toLowerCase());
}

export function matchesUserIdentityQuery(user: UserIdentitySource, query: string) {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return true;
  return userIdentitySearchTokens(user).some((value) => value.includes(normalizedQuery));
}

export function deriveWhaleTier(user: WhaleStatusSource): WhaleTier {
  if (user.is_trusted_whale) return "trusted";
  if (user.is_whale) return "whale";
  if (user.is_likely_insider) return "standard";
  const trustScore = typeof user.trust_score === "number" ? user.trust_score : null;
  const activityCount =
    typeof user.sample_trade_count === "number"
      ? user.sample_trade_count
      : typeof user.entry_trade_count === "number"
        ? user.entry_trade_count
        : null;
  if (
    trustScore !== null &&
    activityCount !== null &&
    trustScore >= POTENTIAL_WHALE_MIN_TRUST_SCORE &&
    activityCount >= POTENTIAL_WHALE_MIN_ACTIVITY_COUNT
  ) {
    return "potential";
  }
  return "standard";
}

export function deriveWhaleTierLabel(user: WhaleStatusSource | WhaleTier) {
  const tier = typeof user === "string" ? user : deriveWhaleTier(user);
  if (tier === "trusted") return "Trusted Whale";
  if (tier === "whale") return "Whale";
  if (tier === "potential") return "Potential Whale";
  return "Standard Trader";
}

export function deriveWhaleTierPillClass(user: WhaleStatusSource | WhaleTier) {
  const tier = typeof user === "string" ? user : deriveWhaleTier(user);
  if (tier === "trusted") return "internal-pill";
  if (tier === "whale") return "public-pill";
  if (tier === "potential") return "potential-pill";
  return "standard-pill";
}
