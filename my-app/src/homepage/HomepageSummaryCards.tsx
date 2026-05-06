import { PieChart } from "@mui/x-charts/PieChart";
import { useCallback } from "react";
import { useApiData } from "../hooks/useApiData";
import { fetchHomeSummary, type HomeSummary } from "../lib/api";

type HomeSummaryWithFreshness = HomeSummary & {
  is_stale?: boolean;
  stale_as_of?: string | null;
  freshness_source?: string | null;
  last_successful_ingest_at?: string | null;
  last_updated_at?: string | null;
  last_updated?: string | null;
  updated_at?: string | null;
  generated_at?: string | null;
  market_category_coverage?: {
    category_name: string;
    market_count: number;
  }[];
};

function formatCompact(value: number) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatPercent(value: number) {
  return `${Math.round(value)}%`;
}

function formatCategoryLabel(value: string) {
  return value
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function formatLastUpdated(value?: string | number | Date | null) {
  if (!value) return "--";

  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function getDatabaseLastUpdated(summary: HomeSummaryWithFreshness) {
  return (
    summary.last_successful_ingest_at ??
    summary.latest_ingestion?.finished_at ??
    summary.latest_ingestion?.started_at ??
    summary.last_updated_at ??
    summary.last_updated ??
    summary.updated_at ??
    summary.generated_at
  );
}

type CoveragePieRow = {
  name: string;
  value: number;
};

const COVERAGE_COLORS = [
  "#6f7cff",
  "#42d3ff",
  "#4fd18b",
  "#f6c85f",
  "#f07167",
  "#a78bfa",
  "#f59e0b",
  "#94a3b8",
];

function CoveragePieChart({
  rows,
  total,
  totalLabel,
}: {
  rows: CoveragePieRow[];
  total: number;
  totalLabel: string;
}) {
  const activeRows = rows.filter((row) => row.value > 0);
  const pieData = activeRows.map((row, index) => ({
    id: index,
    value: row.value,
    label: row.name,
    color: COVERAGE_COLORS[index % COVERAGE_COLORS.length],
  }));

  return (
    <div className="coverage-pie-content">
      {activeRows.length > 0 ? (
        <div className="coverage-mui-pie-shell">
          <PieChart
            className="coverage-mui-pie"
            series={[
              {
                data: pieData,
                cornerRadius: 3,
                paddingAngle: 1,
                highlightScope: { fade: "global", highlight: "item" },
                faded: { innerRadius: 30, additionalRadius: -30, color: "gray" },
                valueFormatter: (item) => {
                  const percent = total > 0 ? (item.value / total) * 100 : 0;
                  return `${item.label}: ${formatCompact(item.value)} · ${formatPercent(percent)}`;
                },
              },
            ]}
            width={220}
            height={220}
            hideLegend
            margin={{ top: 10, bottom: 10, left: 10, right: 10 }}
            slotProps={{
              tooltip: {
                trigger: "item",
                anchor: "pointer",
                position: "right",
                modifiers: [
                  {
                    name: "offset",
                    options: {
                      offset: [0, 6],
                    },
                  },
                ],
                sx: { zIndex: 3000 },
              },
            }}
          />
        </div>
      ) : (
        <div className="coverage-empty-state">
          <strong>{totalLabel}</strong>
          <span>No coverage data available</span>
        </div>
      )}
    </div>
  );
}

export default function HomepageSummaryCards() {
  const loadSummary = useCallback(() => fetchHomeSummary(), []);
  const { data, loading, error } = useApiData(loadSummary);

  return (
    <section className="summary-section">
      <div className="summary-section-header">
        <p className="leaderboard-kicker">Live System State</p>
        <h2>Research Summary</h2>
        <p className="summary-card-subtext">
          Whale identity metrics currently derive from Polymarket user history.
        </p>
      </div>

      {loading && <div className="status-panel">Loading homepage summary...</div>}
      {error && <div className="status-panel error-panel">{error}</div>}

      {!loading &&
        !error &&
        data &&
        (() => {
          const summary = data as HomeSummaryWithFreshness;
          const lastUpdated = getDatabaseLastUpdated(summary);
          const marketCategoryCoverage =
            summary.market_category_coverage && summary.market_category_coverage.length > 0
              ? summary.market_category_coverage
              : summary.platform_coverage.map((platform) => ({
                  category_name: platform.platform_name,
                  market_count: platform.market_count,
                }));

          const coverageCharts = [
            {
              title: "Market Coverage Breakdown",
              label: "Markets",
              totalLabel: "Total Markets",
              rows: marketCategoryCoverage.map((row) => ({
                name: formatCategoryLabel(row.category_name),
                value: row.market_count,
              })),
            },
            {
              title: "User Coverage Breakdown",
              label: "Whales Tracked",
              totalLabel: "Total Number of Whales Tracked",
              rows: [
                {
                  name: "Trusted Whales",
                  value: summary.trusted_whales,
                },
                {
                  name: "Other Tracked Whales",
                  value: Math.max(summary.whales_detected - summary.trusted_whales, 0),
                },
              ],
            },
          ].map((chart) => ({
            ...chart,
            total:
              chart.title === "User Coverage Breakdown"
                ? summary.whales_detected
                : chart.rows.reduce((sum, row) => sum + row.value, 0),
          }));

          return (
            <div className="summary-grid summary-grid-three">
              <article className="summary-card">
                <p className="summary-card-label">Last Updated</p>

                <div className="last-updated-card">
                  <h3>Last Updated:</h3>
                  <p>{formatLastUpdated(lastUpdated)}</p>
                </div>
              </article>

              {coverageCharts.map((chart) => {
                return (
                  <article className="summary-card" key={chart.title}>
                    <p className="summary-card-label">{chart.title}</p>

                    <div className="coverage-pie-header">
                      <h3>{chart.label}</h3>
                      <strong>{formatCompact(chart.total)}</strong>
                    </div>

                    <CoveragePieChart
                      rows={chart.rows}
                      total={chart.total}
                      totalLabel={chart.totalLabel}
                    />
                  </article>
                );
              })}
            </div>
          );
        })()}
    </section>
  );
}
