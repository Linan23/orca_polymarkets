import { useCallback } from "react";
import { useApiData } from "../hooks/useApiData";
import { fetchHomeSummary, type HomeSummary } from "../lib/api";

function formatCompact(value: number) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
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

function getDatabaseLastUpdated(summary: HomeSummary) {
  const timestampSource = summary as HomeSummary & {
    last_updated_at?: string | null;
    last_updated?: string | null;
    updated_at?: string | null;
    generated_at?: string | null;
  };

  return (
    summary.last_successful_ingest_at ??
    summary.latest_ingestion?.finished_at ??
    summary.latest_ingestion?.started_at ??
    timestampSource.last_updated_at ??
    timestampSource.last_updated ??
    timestampSource.updated_at ??
    timestampSource.generated_at
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
          const lastUpdated = getDatabaseLastUpdated(data);

          const coverageCharts = [
            {
              title: "Market Coverage Breakdown",
              label: "Markets",
              total: data.platform_coverage.reduce(
                (sum, platform) => sum + platform.market_count,
                0
              ),
              getValue: (platform: (typeof data.platform_coverage)[number]) =>
                platform.market_count,
            },
            {
              title: "User Coverage Breakdown",
              label: "Users",
              total: data.platform_coverage.reduce(
                (sum, platform) => sum + platform.user_count,
                0
              ),
              getValue: (platform: (typeof data.platform_coverage)[number]) =>
                platform.user_count,
            },
          ];

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
                let offset = 0;
                const colors = ["#6f7cff", "#42d3ff"];

                const segments = data.platform_coverage
                  .map((platform, index) => {
                    const value = chart.getValue(platform);

                    if (chart.total <= 0 || value <= 0) return null;

                    const start = offset;
                    const end = offset + (value / chart.total) * 100;
                    offset = end;

                    return `${colors[index % colors.length]} ${start}% ${end}%`;
                  })
                  .filter((segment): segment is string => Boolean(segment));

                return (
                  <article className="summary-card" key={chart.title}>
                    <p className="summary-card-label">{chart.title}</p>

                    <div className="coverage-pie-header">
                      <h3>{chart.label}</h3>
                      <strong>{formatCompact(chart.total)}</strong>
                    </div>

                    <div className="coverage-pie-content">
                      <div
                        className="coverage-donut"
                        style={{
                          background:
                            segments.length > 0
                              ? `conic-gradient(${segments.join(", ")})`
                              : "rgba(255, 255, 255, 0.08)",
                        }}
                      >
                        <div className="coverage-donut-hole">
                          <span>Total</span>
                          <strong>{formatCompact(chart.total)}</strong>
                        </div>
                      </div>

                      <div className="coverage-pie-legend">
                        {data.platform_coverage.map((platform, index) => {
                          const value = chart.getValue(platform);
                          const percent =
                            chart.total > 0
                              ? Math.round((value / chart.total) * 100)
                              : 0;

                          return (
                            <div
                              className="coverage-legend-row"
                              key={platform.platform_name}
                            >
                              <span
                                className="coverage-legend-dot"
                                style={{
                                  background: colors[index % colors.length],
                                }}
                              />

                              <div>
                                <strong>{platform.platform_name}</strong>
                                <p>
                                  {formatCompact(value)} · {percent}%
                                </p>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </article>
                );
              })}
            </div>
          );
        })()}
    </section>
  );
}
