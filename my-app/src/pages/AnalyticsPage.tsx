import HomepageSummaryCards from "../homepage/HomepageSummaryCards";
import ResearchAnalyticsSection from "../homepage/ResearchAnalyticsSection";
import TopNavbar from "../homepage/TopNavbar";

export default function AnalyticsPage() {
  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Research Analytics</p>
        <h1>Whale Analytics</h1>
        <p className="hero-text">
          Review live whale coverage, resolved-user profitability, and market concentration signals from the current database.
        </p>
      </section>

      <HomepageSummaryCards />
      <ResearchAnalyticsSection showExportControls />
    </div>
  );
}
