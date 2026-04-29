import HomepageSummaryCards from "../homepage/HomepageSummaryCards";
import PolymarketNewsGallery from "../homepage/PolymarketNewsGallery";
import ResearchAnalyticsSection from "../homepage/ResearchAnalyticsSection";
import TopNavbar from "../homepage/TopNavbar";

export default function HomePage() {
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
            <span>Trusted whale coverage</span>
            <strong>2</strong>
          </div>

          <div className="signal-line">
            <span>Top trust score</span>
            <strong>9172.690</strong>
          </div>

          <div className="signal-line">
            <span>Tracked trades</span>
            <strong>77</strong>
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