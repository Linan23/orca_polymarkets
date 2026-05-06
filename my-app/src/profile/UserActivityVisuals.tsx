import { useState } from "react";
import {
  type HourlyActivityBucket,
  type OutcomeBias,
  type TagExposureSlice,
} from "../lib/api";

const DONUT_COLORS = ["#38bdf8", "#22c55e", "#f97316", "#eab308", "#a78bfa", "#94a3b8"];
const OUTCOME_COLORS: Record<OutcomeBias["label"], string> = {
  yes: "#38bdf8",
  no: "#f97316",
  other: "#94a3b8",
};

function formatPercent(value: number) {
  return `${(value * 100).toFixed(0)}%`;
}

export function TagExposureDonut({ slices }: { slices: TagExposureSlice[] }) {
  const [hoveredLabel, setHoveredLabel] = useState<string | null>(null);
  const activeSlices = slices.filter((slice) => slice.percentage > 0);
  if (activeSlices.length === 0) {
    return <div className="empty-chart-state">No market-category activity for this timeframe.</div>;
  }
  const displayedSlice = activeSlices.find((slice) => slice.label === hoveredLabel) ?? activeSlices[0];

  const segments = activeSlices.reduce<Array<TagExposureSlice & { strokeDashoffset: number }>>((items, slice) => {
    const currentOffset = items.reduce((sum, item) => sum + item.percentage * 100, 0);
    items.push({
      ...slice,
      strokeDashoffset: -currentOffset,
    });
    return items;
  }, []);

  return (
    <div className="donut-layout">
      <div className="donut-shell">
        <svg viewBox="0 0 36 36" className="donut-chart" aria-label="Market category trade mix donut chart">
          <circle cx="18" cy="18" r="15.915" className="donut-track" />
          <g transform="rotate(-90 18 18)">
            {segments.map((slice, index) => {
              const dash = slice.percentage * 100;
              const isActive = displayedSlice.label === slice.label;
              const isMuted = hoveredLabel !== null && !isActive;
              return (
                <circle
                  key={slice.label}
                  cx="18"
                  cy="18"
                  r="15.915"
                  className={`donut-segment${isActive ? " is-active" : ""}${isMuted ? " is-muted" : ""}`}
                  stroke={DONUT_COLORS[index % DONUT_COLORS.length]}
                  strokeDasharray={`${dash} ${100 - dash}`}
                  strokeDashoffset={slice.strokeDashoffset}
                  tabIndex={0}
                  aria-label={`${slice.label}: ${formatPercent(slice.percentage)} of trades, ${slice.trade_count} trades`}
                  onFocus={() => setHoveredLabel(slice.label)}
                  onBlur={() => setHoveredLabel(null)}
                  onMouseEnter={() => setHoveredLabel(slice.label)}
                  onMouseLeave={() => setHoveredLabel(null)}
                />
              );
            })}
          </g>
        </svg>
        <div className="donut-center">
          <strong>{formatPercent(displayedSlice.percentage)}</strong>
          <span>{displayedSlice.label}</span>
          <small>{displayedSlice.trade_count} trades</small>
        </div>
      </div>

      <div className="chart-legend">
        {activeSlices.map((slice, index) => (
          <div
            key={slice.label}
            className={`chart-legend-row${displayedSlice.label === slice.label ? " is-active" : ""}`}
            onMouseEnter={() => setHoveredLabel(slice.label)}
            onMouseLeave={() => setHoveredLabel(null)}
          >
            <span className="chart-swatch" style={{ backgroundColor: DONUT_COLORS[index % DONUT_COLORS.length] }} />
            <div className="chart-legend-main">
              <strong>{slice.label}</strong>
              <small>
                {formatPercent(slice.percentage)} · {slice.trade_count} trades
              </small>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function OutcomeBiasBar({ items }: { items: OutcomeBias[] }) {
  const totalTrades = items.reduce((sum, item) => sum + item.trade_count, 0);
  if (totalTrades === 0) {
    return <div className="empty-chart-state">No outcome-labeled trades for this timeframe.</div>;
  }

  return (
    <div className="bias-layout">
      <div className="bias-bar" aria-label="Outcome bias by trade count">
        {items.map((item) => (
          <div
            key={item.label}
            className="bias-segment"
            style={{
              width: `${Math.max(item.percentage * 100, item.trade_count > 0 ? 6 : 0)}%`,
              backgroundColor: OUTCOME_COLORS[item.label],
            }}
            title={`${item.label}: ${formatPercent(item.percentage)} (${item.trade_count} trades)`}
          />
        ))}
      </div>

      <div className="chart-legend bias-legend">
        {items.map((item) => (
          <div key={item.label} className="chart-legend-row">
            <span className="chart-swatch" style={{ backgroundColor: OUTCOME_COLORS[item.label] }} />
            <div className="chart-legend-main">
              <strong>{item.label.toUpperCase()}</strong>
              <small>
                {formatPercent(item.percentage)} · {item.trade_count} trades
              </small>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function HourlyActivityChart({ buckets }: { buckets: HourlyActivityBucket[] }) {
  const maxTrades = Math.max(...buckets.map((bucket) => bucket.trade_count), 0);
  if (maxTrades === 0) {
    return <div className="empty-chart-state">No hourly activity in this timeframe.</div>;
  }
  const peakHours = buckets
    .filter((bucket) => bucket.trade_count === maxTrades)
    .map((bucket) => `${bucket.hour_utc.toString().padStart(2, "0")}:00 UTC`);
  const peakLabel = peakHours.join(", ");

  return (
    <div className="histogram">
      <div className="histogram-bars" aria-label="Hourly activity histogram in UTC">
        {buckets.map((bucket) => {
          const height = maxTrades > 0 ? Math.max((bucket.trade_count / maxTrades) * 100, bucket.trade_count > 0 ? 8 : 0) : 0;
          const hourLabel = `${bucket.hour_utc.toString().padStart(2, "0")}:00 UTC`;
          const isPeak = bucket.trade_count === maxTrades;
          return (
            <div key={bucket.hour_utc} className={`histogram-column${isPeak ? " peak" : ""}`}>
              <div
                className="histogram-bar"
                style={{ height: `${height}%` }}
                title={`${hourLabel} · ${bucket.trade_count} trades${isPeak ? " · most active" : ""}`}
                aria-label={`${hourLabel}: ${bucket.trade_count} trades${isPeak ? ", most active hour" : ""}`}
              />
              <span className="histogram-label">{bucket.hour_utc.toString().padStart(2, "0")}</span>
            </div>
          );
        })}
      </div>
      <p className="chart-footnote">Most active: {peakLabel}. Activity hours are shown in UTC.</p>
    </div>
  );
}
