import { useEffect, useRef, useState } from "react";

const POLYMARKET_X_URL = "https://x.com/Polymarket";
const TWITTER_WIDGET_SCRIPT_ID = "twitter-widgets-script";

type TwitterWidgets = {
  widgets?: {
    load?: (element?: HTMLElement) => Promise<unknown> | void;
  };
};

declare global {
  interface Window {
    twttr?: TwitterWidgets;
  }
}

let twitterScriptPromise: Promise<void> | null = null;

function loadTwitterWidgets() {
  if (window.twttr?.widgets?.load) {
    return Promise.resolve();
  }

  if (twitterScriptPromise) {
    return twitterScriptPromise;
  }

  twitterScriptPromise = new Promise<void>((resolve, reject) => {
    const existingScript = document.getElementById(TWITTER_WIDGET_SCRIPT_ID) as HTMLScriptElement | null;
    if (existingScript) {
      existingScript.addEventListener("load", () => resolve(), { once: true });
      existingScript.addEventListener("error", () => reject(new Error("Unable to load X timeline.")), { once: true });
      return;
    }

    const script = document.createElement("script");
    script.id = TWITTER_WIDGET_SCRIPT_ID;
    script.src = "https://platform.twitter.com/widgets.js";
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      twitterScriptPromise = null;
      reject(new Error("Unable to load X timeline."));
    };
    document.body.appendChild(script);
  });

  return twitterScriptPromise;
}

export default function PolymarketNewsGallery() {
  const timelineRef = useRef<HTMLDivElement>(null);
  const [loadState, setLoadState] = useState<"loading" | "ready" | "failed">("loading");

  useEffect(() => {
    let cancelled = false;
    let fallbackTimer: number | undefined;

    loadTwitterWidgets()
      .then(() => {
        if (cancelled) return;
        return Promise.resolve(window.twttr?.widgets?.load?.(timelineRef.current ?? undefined));
      })
      .then(() => {
        if (cancelled) return;
        fallbackTimer = window.setTimeout(() => {
          if (!cancelled && !timelineRef.current?.querySelector("iframe")) {
            setLoadState("failed");
          }
        }, 8000);
        setLoadState("ready");
      })
      .catch(() => {
        if (!cancelled) {
          setLoadState("failed");
        }
      });

    return () => {
      cancelled = true;
      if (fallbackTimer) window.clearTimeout(fallbackTimer);
    };
  }, []);

  return (
    <section className="pm-news-section">
      <div className="pm-news-header">
        <div>
          <p className="pm-news-kicker">LIVE TWEET FEED</p>
          <h2>@Polymarket</h2>
        </div>
        <a className="pm-news-profile-link" href={POLYMARKET_X_URL} target="_blank" rel="noreferrer">
          Open on X
        </a>
      </div>

      <div className="pm-news-live-shell" ref={timelineRef}>
        {loadState === "loading" && (
          <div className="pm-news-loading">
            <span>Loading live Polymarket posts...</span>
          </div>
        )}

        <a
          className="twitter-timeline"
          data-theme="dark"
          data-chrome="noheader nofooter noborders transparent"
          data-height="420"
          data-dnt="true"
          href={POLYMARKET_X_URL}
        >
          Tweets by Polymarket
        </a>

        {loadState === "failed" && (
          <div className="pm-news-fallback">
            <strong>Live feed is unavailable in this browser.</strong>
            <span>Open the latest Polymarket posts directly on X.</span>
            <a href={POLYMARKET_X_URL} target="_blank" rel="noreferrer">
              View @Polymarket
            </a>
          </div>
        )}
      </div>
    </section>
  );
}
