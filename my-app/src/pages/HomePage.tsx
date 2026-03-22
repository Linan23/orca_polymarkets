import PolymarketNewsGallery from "../homepage/PolymarketNewsGallery";
import PolymarketTimeline from "../homepage/PolymarketTimeline";
import TopNavbar from "../homepage/TopNavbar";

export default function HomePage() {
  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Orca Dashboard</h1>
        <p className="hero-text">
          Follow markets, keep up with the latest Polymarket news, and explore whale activity.
        </p>
      </section>

      <PolymarketNewsGallery />
      <PolymarketTimeline />
    </div>
  );
}