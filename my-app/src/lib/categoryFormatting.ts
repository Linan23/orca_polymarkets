const CATEGORY_COLORS: Record<string, string> = {
  "video game": "#38bdf8",
  "video games": "#38bdf8",
  "video-games": "#38bdf8",
  esports: "#38bdf8",
  technology: "#22c55e",
  tech: "#22c55e",
  crypto: "#f97316",
  finance: "#14b8a6",
  geopolitics: "#eab308",
  "world politics": "#eab308",
  politics: "#a78bfa",
  other: "#94a3b8",
};

export const CATEGORY_FALLBACK_COLORS = [
  "#38bdf8",
  "#22c55e",
  "#f97316",
  "#14b8a6",
  "#eab308",
  "#a78bfa",
  "#94a3b8",
];

export function formatCategoryLabel(value: string | null | undefined) {
  const category = value?.trim();
  if (!category) return "Other";
  if (CATEGORY_COLORS[category.toLowerCase()]) {
    return category
      .split(/[\s_-]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
      .join(" ");
  }

  return category
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

export function getCategoryColor(value: string | null | undefined, index = 0) {
  const normalized = value?.trim().toLowerCase();
  if (normalized && CATEGORY_COLORS[normalized]) {
    return CATEGORY_COLORS[normalized];
  }

  return CATEGORY_FALLBACK_COLORS[index % CATEGORY_FALLBACK_COLORS.length];
}
