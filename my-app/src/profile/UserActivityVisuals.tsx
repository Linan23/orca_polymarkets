import { BarChart } from "@mui/x-charts/BarChart";
import { PieChart } from "@mui/x-charts/PieChart";
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

function formatCompact(value: number) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function TagExposureDonut({ slices }: { slices: TagExposureSlice[] }) {
  const activeSlices = slices.filter((slice) => slice.percentage > 0);
  if (activeSlices.length === 0) {
    return <div className="empty-chart-state">No market-category activity for this timeframe.</div>;
  }
  const totalTrades = activeSlices.reduce((sum, slice) => sum + slice.trade_count, 0);
  const pieData = activeSlices.map((slice, index) => ({
    id: index,
    value: slice.trade_count,
    label: slice.label,
    color: DONUT_COLORS[index % DONUT_COLORS.length],
  }));

  return (
    <div className="market-category-pie-layout">
      <div className="market-category-pie-shell">
        <PieChart
          className="market-category-pie"
          series={[
            {
              data: pieData,
              cornerRadius: 3,
              paddingAngle: 1,
              highlightScope: { fade: "global", highlight: "item" },
              faded: { innerRadius: 30, additionalRadius: -30, color: "gray" },
              valueFormatter: (item) => {
                const slice = activeSlices[Number(item.id)];
                const percent = totalTrades > 0 ? item.value / totalTrades : 0;
                return `${item.label}: ${formatCompact(slice?.trade_count ?? item.value)} trades · ${formatPercent(percent)}`;
              },
            },
          ]}
          width={260}
          height={250}
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
                    offset: [8, 8],
                  },
                },
              ],
              sx: { zIndex: 99999 },
            },
          }}
        />
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
  const chartData = buckets.map((bucket) => ({
    hour: `${bucket.hour_utc.toString().padStart(2, "0")}:00`,
    trades: bucket.trade_count,
  }));
  const peakBucket = buckets
    .filter((bucket) => bucket.trade_count > 0)
    .sort((first, second) => {
      if (second.trade_count !== first.trade_count) return second.trade_count - first.trade_count;
      return first.hour_utc - second.hour_utc;
    })[0];
  const peakLabel = `${peakBucket.hour_utc.toString().padStart(2, "0")}:00 UTC`;

  return (
    <div className="trading-hours-chart">
      <BarChart
        dataset={chartData}
        xAxis={[
          {
            dataKey: "hour",
            scaleType: "band",
            tickPlacement: "middle",
            tickLabelPlacement: "middle",
          },
        ]}
        yAxis={[
          {
            position: "none",
          },
        ]}
        series={[
          {
            dataKey: "trades",
            label: "Trades",
            color: "#38bdf8",
            valueFormatter: (value) => `${value ?? 0} trades`,
          },
        ]}
        height={300}
        margin={{ top: 18, right: 16, bottom: 46, left: 4 }}
        hideLegend
      />
      <p className="chart-footnote">Most active: {peakLabel}. Activity hours are shown in UTC.</p>
    </div>
  );
}
