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

export function TagExposureDonut({ slices }: { slices: TagExposureSlice[] }) {
  const activeSlices = slices.filter((slice) => slice.percentage > 0);
  if (activeSlices.length === 0) {
    return <div className="empty-chart-state">No market-category activity for this timeframe.</div>;
  }
  const pieData = activeSlices.map((slice, index) => ({
    id: index,
    value: Number((slice.percentage * 100).toFixed(4)),
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
              arcLabel: (item) => `${Math.round(item.value)}%`,
              arcLabelMinAngle: 24,
              cornerRadius: 3,
              paddingAngle: 1,
              valueFormatter: (item) => {
                const slice = activeSlices[Number(item.id)];
                return `${item.value.toFixed(1)}% · ${slice?.trade_count ?? 0} trades`;
              },
            },
          ]}
          width={260}
          height={240}
          hideLegend
          margin={{ top: 8, bottom: 8, left: 8, right: 8 }}
        />
      </div>

      <div className="chart-legend">
        {activeSlices.map((slice, index) => (
          <div key={slice.label} className="chart-legend-row">
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
            label: "Trades",
            width: 46,
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
        grid={{ horizontal: true }}
        hideLegend
      />
      <p className="chart-footnote">Most active: {peakLabel}. Activity hours are shown in UTC.</p>
    </div>
  );
}
