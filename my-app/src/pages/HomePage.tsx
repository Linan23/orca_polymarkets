import { useCallback } from "react";
import HomepageSummaryCards from "../homepage/HomepageSummaryCards";
import PolymarketNewsGallery from "../homepage/PolymarketNewsGallery";
import ResearchAnalyticsSection from "../homepage/ResearchAnalyticsSection";
import TopNavbar from "../homepage/TopNavbar";
import { useApiData } from "../hooks/useApiData";
import { fetchHomeSummary } from "../lib/api";

function formatSignalValue(value: number | null) {
  if (value === null) return "--";
  return new Intl.NumberFormat("en-US").format(value);
}

export default function HomePage() {
  const loadSummary = useCallback(() => fetchHomeSummary(), []);
  const { data } = useApiData(loadSummary, { keepPreviousData: true });
  const totalTrackedTrades =
    data?.platform_coverage.reduce((sum, platform) => sum + platform.transaction_count, 0) ?? null;

  return (
    <div className="page page-home home-dashboard">
      <TopNavbar />

      <section className="home-hero">
        <div className="home-hero-copy">
          <p className="eyebrow">Orca Polymarkets</p>
          <h1>Track whales before the market moves.</h1>
          <p>
            Monitor whale activity, follow market signals, and understand
            Polymarket behavior with Orca.
          </p>

          <div className="home-hero-actions">
            <a href="#research" className="home-primary-button">
              View Research
            </a>
            <a href="/definitions" className="home-secondary-button">
              Learn Terms
            </a>
          </div>
        </div>

        <div className="home-hero-card">
          <p className="home-card-label">Live Signal</p>
          <h2>Whale Activity</h2>

          <div className="signal-line">
            <span>Number of Trusted Whales</span>
            <strong>{formatSignalValue(data?.trusted_whales ?? null)}</strong>
          </div>

          <div className="signal-line">
            <span>Total Number of Whales Tracked</span>
            <strong>{formatSignalValue(data?.whales_detected ?? null)}</strong>
          </div>

          <div className="signal-line">
            <span>Total Tracked Trades</span>
            <strong>{formatSignalValue(totalTrackedTrades)}</strong>
          </div>
        </div>
      </section>

      <PolymarketNewsGallery />
      <HomepageSummaryCards />

      <div id="research">
        <ResearchAnalyticsSection persistTimeframePreference />
      </div>
    </div>
  );
}
