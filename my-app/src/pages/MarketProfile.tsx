import { useParams, Link } from "react-router-dom";

type MarketProfileRow = {
  market_profile_section_id: string;
  dashboard_id: string;
  market_ref: string;
  realtime_source: string;
  snapshot_time: string;
  realtime_payload: string;
};

const marketProfiles: MarketProfileRow[] = [
  {
    market_profile_section_id: "mp-001",
    dashboard_id: "dashboard-001",
    market_ref: "market-1",
    realtime_source: "Polymarket WebSocket",
    snapshot_time: "2025-03-11T12:00:00Z",
    realtime_payload: `{
  "bid": 0.47,
  "ask": 0.49,
  "last": 0.48,
  "spread": 0.02,
  "volume": 50000
}`,
  },
  {
    market_profile_section_id: "mp-002",
    dashboard_id: "dashboard-001",
    market_ref: "market-2",
    realtime_source: "Polymarket WebSocket",
    snapshot_time: "2025-03-11T12:02:00Z",
    realtime_payload: `{
  "bid": 0.51,
  "ask": 0.53,
  "last": 0.52,
  "spread": 0.02,
  "volume": 61000
}`,
  },
];

export default function MarketProfile() {
  const { marketId } = useParams();
  const profile =
    marketProfiles.find((item) => item.market_ref === marketId) ?? {
      market_profile_section_id: "mp-placeholder",
      dashboard_id: "dashboard-001",
      market_ref: marketId ?? "unknown-market",
      realtime_source: "Placeholder source",
      snapshot_time: new Date().toISOString(),
      realtime_payload: `{
  "message": "No saved profile found yet for this market."
}`,
    };

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Market Profile</p>
        <h1>{profile.market_ref}</h1>
        <p className="hero-text">
          Real-time market profile snapshot and source details.
        </p>
        <Link to="/" className="table-link">
          ← Back to dashboard
        </Link>
      </header>

      <section className="card profile-card">
        <div className="card-header">
          <p className="card-label">Profile Snapshot</p>
          <h2>Market Details</h2>
        </div>

        <div className="profile-grid">
          <div className="profile-item">
            <span className="profile-key">market_profile_section_id</span>
            <span>{profile.market_profile_section_id}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">dashboard_id</span>
            <span>{profile.dashboard_id}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">market_ref</span>
            <span>{profile.market_ref}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">realtime_source</span>
            <span>{profile.realtime_source}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">snapshot_time</span>
            <span>{profile.snapshot_time}</span>
          </div>

          <div className="profile-item profile-item-full">
            <span className="profile-key">realtime_payload</span>
            <pre className="payload-box">{profile.realtime_payload}</pre>
          </div>
        </div>
      </section>
    </div>
  );
}