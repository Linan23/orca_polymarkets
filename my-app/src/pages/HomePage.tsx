import HomepageSummaryCards from "../homepage/HomepageSummaryCards";
import PolymarketNewsGallery from "../homepage/PolymarketNewsGallery";
import ResearchAnalyticsSection from "../homepage/ResearchAnalyticsSection";
import TopNavbar from "../homepage/TopNavbar";

export default function HomePage() {
  return (
    <div className="page page-home">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Orca Dashboard</h1>
        <p className="hero-text">
          Follow markets, monitor whale activity, and track the strongest signals across Polymarket.
        </p>
      </section>

      <PolymarketNewsGallery />
      <HomepageSummaryCards />
      <ResearchAnalyticsSection persistTimeframePreference />
    </div>
  );
}
