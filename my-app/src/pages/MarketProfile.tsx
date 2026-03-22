import { useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { useState } from "react";

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

  const parsedPayload = useMemo(() => {
    try {
      return JSON.parse(profile.realtime_payload);
    } catch {
      return null;
    }
  }, [profile.realtime_payload]);
  
  const [isFollowing, setIsFollowing] = useState(false);

  const bid = typeof parsedPayload?.bid === "number" ? parsedPayload.bid : null;
  const ask = typeof parsedPayload?.ask === "number" ? parsedPayload.ask : null;
  const last =
    typeof parsedPayload?.last === "number" ? parsedPayload.last : null;
  const spread =
    typeof parsedPayload?.spread === "number" ? parsedPayload.spread : null;
  const volume =
    typeof parsedPayload?.volume === "number" ? parsedPayload.volume : null;

  const lastPercent = last !== null ? Math.round(last * 100) : null;
  const yesPrice = last !== null ? (last * 100).toFixed(1) : "--";
  const noPrice = last !== null ? ((1 - last) * 100).toFixed(1) : "--";

  const formattedSnapshotTime = new Date(profile.snapshot_time).toLocaleString(
    undefined,
    {
      dateStyle: "medium",
      timeStyle: "short",
    }
  );

  return (
    <div className="page market-profile-page">
      <header className="hero market-hero">
        <div className="hero-top-row">
          <div>
            <p className="eyebrow">Market Profile</p>
            <h1 className="market-title">{profile.market_ref}</h1>
            <p className="hero-text">
              Real-time market profile snapshot and source details.
            </p>
          </div>
          <button
            type="button"
            className={`follow-btn ${isFollowing ? "active" : ""}`}
            onClick={() => setIsFollowing((prev) => !prev)}
            aria-pressed={isFollowing}
          >
            <span className="follow-icon">
              {isFollowing ? (
                // FILLED STAR
                <svg viewBox="0 0 24 24" fill="currentColor">
                  <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
                </svg>
              ) : (
                // OUTLINE STAR
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
                </svg>
              )}
            </span>

            <span>{isFollowing ? "Following" : "Follow"}</span>
          </button>
        </div>

        <div className="hero-actions">
          <Link to="/" className="table-link back-link">
            ← Back to dashboard
          </Link>

          <div className="hero-pills">
            <span className="hero-pill">Source: {profile.realtime_source}</span>
            <span className="hero-pill">Snapshot: {formattedSnapshotTime}</span>
          </div>
        </div>
      </header>

      <section className="market-summary-card">
        <div className="market-summary-left">
          <p className="summary-label">Current Probability</p>
          <div className="summary-main">
            <h2>{lastPercent !== null ? `${lastPercent}%` : "—"}</h2>
            <span className="summary-trend">
              {spread !== null ? `Spread ${spread.toFixed(2)}` : "Live market"}
            </span>
          </div>

          <div className="summary-stats">
            <div className="stat-chip">
              <span className="stat-chip-label">Bid</span>
              <strong>{bid !== null ? bid.toFixed(2) : "--"}</strong>
            </div>
            <div className="stat-chip">
              <span className="stat-chip-label">Ask</span>
              <strong>{ask !== null ? ask.toFixed(2) : "--"}</strong>
            </div>
            <div className="stat-chip">
              <span className="stat-chip-label">Last</span>
              <strong>{last !== null ? last.toFixed(2) : "--"}</strong>
            </div>
            <div className="stat-chip">
              <span className="stat-chip-label">Volume</span>
              <strong>
                {volume !== null ? volume.toLocaleString() : "--"}
              </strong>
            </div>
          </div>
        </div>

        <div className="market-summary-right">
          <button className="trade-btn trade-btn-yes" type="button">
            <span className="trade-label">Buy Yes</span>
            <strong>{yesPrice}¢</strong>
          </button>

          <button className="trade-btn trade-btn-no" type="button">
            <span className="trade-label">Buy No</span>
            <strong>{noPrice}¢</strong>
          </button>
        </div>
      </section>

      <section className="card profile-card">
        <div className="card-header">
          <p className="card-label">Profile Snapshot</p>
          <h2>Market Details</h2>
          <p className="card-subtext">
            Preserved raw variables with a cleaner UI layer on top.
          </p>
        </div>

        <div className="profile-grid polished-grid">
          <div className="profile-item">
            <span className="profile-key">market_profile_section_id</span>
            <span className="profile-value">{profile.market_profile_section_id}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">dashboard_id</span>
            <span className="profile-value">{profile.dashboard_id}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">market_ref</span>
            <span className="profile-value">{profile.market_ref}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">realtime_source</span>
            <span className="profile-value">{profile.realtime_source}</span>
          </div>

          <div className="profile-item">
            <span className="profile-key">snapshot_time</span>
            <span className="profile-value">{profile.snapshot_time}</span>
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