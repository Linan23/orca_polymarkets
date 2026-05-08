import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApiData } from "../hooks/useApiData";
import { fetchPolymarketTweetFeed, type PolymarketTweet } from "../lib/api";

const POLYMARKET_X_URL = "https://x.com/Polymarket";
const TWEET_CARD_LIMIT = 6;
const AUTO_ROTATE_MS = 9000;

function formatTweetTime(value: string | null) {
  if (!value) return "Live";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Live";
  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.max(0, Math.floor(diffMs / 60_000));
  if (diffMinutes < 1) return "Now";
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
  }).format(date);
}

function TweetCard({
  post,
  profileImageUrl,
}: {
  post: PolymarketTweet;
  profileImageUrl?: string | null;
}) {
  return (
    <a href={post.url} target="_blank" rel="noreferrer" className="pm-news-card">
      <div className="pm-news-card-top">
        <div className="pm-news-account">
          {profileImageUrl ? (
            <img src={profileImageUrl} alt="" className="pm-news-avatar pm-news-avatar-img" />
          ) : (
            <div className="pm-news-avatar">P</div>
          )}
          <div className="pm-news-meta">
            <span className="pm-news-name">Polymarket</span>
            <span className="pm-news-handle">@Polymarket</span>
          </div>
        </div>

        <span className="pm-news-time">{formatTweetTime(post.created_at)}</span>
      </div>

      <p className="pm-news-text">{post.text}</p>

      <div className="pm-news-card-bottom">
        <span className="pm-news-link">View post ↗</span>
      </div>
    </a>
  );
}

function FeedStateCard({
  loading,
  error,
  reason,
}: {
  loading: boolean;
  error: string | null;
  reason?: string | null;
}) {
  const message = loading
    ? "Loading latest Polymarket posts..."
    : error
      ? "Unable to load live posts right now."
      : reason === "missing_x_bearer_token"
        ? "Live post cards need an X API token on the server."
        : "Live posts are unavailable right now.";

  return (
    <a href={POLYMARKET_X_URL} target="_blank" rel="noreferrer" className="pm-news-card pm-news-state-card">
      <div className="pm-news-card-top">
        <div className="pm-news-account">
          <div className="pm-news-avatar">P</div>
          <div className="pm-news-meta">
            <span className="pm-news-name">Polymarket</span>
            <span className="pm-news-handle">@Polymarket</span>
          </div>
        </div>
        <span className="pm-news-time">Live</span>
      </div>
      <p className="pm-news-text">{message}</p>
      <div className="pm-news-card-bottom">
        <span className="pm-news-link">Open on X ↗</span>
      </div>
    </a>
  );
}

export default function PolymarketNewsGallery() {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [isPaused, setIsPaused] = useState(false);
  const loadTweetFeed = useCallback(() => fetchPolymarketTweetFeed(TWEET_CARD_LIMIT), []);
  const { data, loading, error } = useApiData(loadTweetFeed, { keepPreviousData: true });
  const posts = useMemo(() => data?.items ?? [], [data]);

  const scroll = useCallback((direction: "left" | "right") => {
    const container = scrollRef.current;
    if (!container) return;

    const firstCard = container.querySelector<HTMLElement>(".pm-news-card");
    const amount = firstCard ? firstCard.offsetWidth + 16 : 360;
    if (direction === "right" && container.scrollLeft + container.clientWidth >= container.scrollWidth - 16) {
      container.scrollTo({ left: 0, behavior: "smooth" });
      return;
    }
    container.scrollBy({
      left: direction === "left" ? -amount : amount,
      behavior: "smooth",
    });
  }, []);

  useEffect(() => {
    if (isPaused || posts.length <= 1) return undefined;
    const interval = window.setInterval(() => scroll("right"), AUTO_ROTATE_MS);
    return () => window.clearInterval(interval);
  }, [isPaused, posts.length, scroll]);

  return (
    <section className="pm-news-section">
      <div className="pm-news-header">
        <h2>LIVE TWEET FEED</h2>
      </div>

      <div
        className="pm-news-carousel"
        onMouseEnter={() => setIsPaused(true)}
        onMouseLeave={() => setIsPaused(false)}
        onFocus={() => setIsPaused(true)}
        onBlur={() => setIsPaused(false)}
      >
        <button
          className="pm-news-arrow left"
          onClick={() => scroll("left")}
          aria-label="Scroll left"
          type="button"
        >
          ‹
        </button>

        <div className="pm-news-scroll" ref={scrollRef}>
          {posts.length > 0 ? (
            posts.map((post) => (
              <TweetCard
                key={post.id}
                post={post}
                profileImageUrl={data?.account.profile_image_url}
              />
            ))
          ) : (
            <FeedStateCard loading={loading} error={error} reason={data?.reason} />
          )}
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
