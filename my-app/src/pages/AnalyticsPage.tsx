import HomepageSummaryCards from "../homepage/HomepageSummaryCards";
import ResearchAnalyticsSection from "../homepage/ResearchAnalyticsSection";
import TopNavbar from "../homepage/TopNavbar";

export default function AnalyticsPage() {
  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Global Research Analytics</p>
        <h1>Market-Wide Whale Signals</h1>
        <p className="hero-text">
          Review cross-market whale coverage, resolved-user profitability, and concentration signals across the current database.
        </p>
      </section>

      <HomepageSummaryCards />
      <ResearchAnalyticsSection showExportControls />
    </div>
  );
}
