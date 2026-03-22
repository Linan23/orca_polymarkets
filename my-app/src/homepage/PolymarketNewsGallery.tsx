import { useRef } from "react";

const posts = [
  {
    id: 1,
    text: 'JUST IN: Tesla, SpaceX & xAI unveil TERAFAB — a 1TW chip factory to power a "galactic civilization"',
    time: "1h ago",
    url: "https://x.com/Polymarket",
  },
  {
    id: 2,
    text: "19% chance the AI bubble bursts this year.",
    time: "2h ago",
    url: "https://x.com/Polymarket",
  },
  {
    id: 3,
    text: 'JUST IN: AI cow collar startup Halter raises at $2,000,000,000 valuation, uses proprietary “cowgorithm” to herd...',
    time: "3h ago",
    url: "https://x.com/Polymarket",
  },
  {
    id: 4,
    text: "BREAKING: Iranian man arrested attempting to break into UK nuclear base in Scotland.",
    time: "3h ago",
    url: "https://x.com/Polymarket",
  },
  {
    id: 5,
    text: "Fed speakers this week could reshape rate-cut odds across prediction markets.",
    time: "4h ago",
    url: "https://x.com/Polymarket",
  },
  {
    id: 6,
    text: "Election market volatility spikes after new polling and debate speculation.",
    time: "5h ago",
    url: "https://x.com/Polymarket",
  },
];

export default function PolymarketNewsGallery() {
  const scrollRef = useRef<HTMLDivElement>(null);

  const scroll = (direction: "left" | "right") => {
    if (!scrollRef.current) return;

    const amount = 360;
    scrollRef.current.scrollBy({
      left: direction === "left" ? -amount : amount,
      behavior: "smooth",
    });
  };

  return (
    <section className="pm-news-section">
      <div className="pm-news-header">
        <h2>LATEST NEWS &amp; UPDATES</h2>
      </div>

      <div className="pm-news-carousel">
        <button
          className="pm-news-arrow left"
          onClick={() => scroll("left")}
          aria-label="Scroll left"
          type="button"
        >
          ‹
        </button>

        <div className="pm-news-scroll" ref={scrollRef}>
          {posts.map((post) => (
            <a
              key={post.id}
              href={post.url}
              target="_blank"
              rel="noreferrer"
              className="pm-news-card"
            >
              <div className="pm-news-card-top">
                <div className="pm-news-account">
                  <div className="pm-news-avatar">⌁</div>
                  <div className="pm-news-meta">
                    <span className="pm-news-name">Polymarket</span>
                    <span className="pm-news-handle">@Polymarket</span>
                  </div>
                </div>

                <span className="pm-news-time">{post.time}</span>
              </div>

              <p className="pm-news-text">{post.text}</p>

              <div className="pm-news-card-bottom">
                <span className="pm-news-link">↗</span>
              </div>
            </a>
          ))}
        </div>

        <button
          className="pm-news-arrow right"
          onClick={() => scroll("right")}
          aria-label="Scroll right"
          type="button"
        >
          ›
        </button>
      </div>
    </section>
  );
}