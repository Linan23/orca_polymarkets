import { useEffect } from "react";

declare global {
  interface Window {
    twttr?: any;
  }
}

export default function PolymarketTimeline() {
  useEffect(() => {
    const script = document.createElement("script");
    script.src = "https://platform.twitter.com/widgets.js";
    script.async = true;
    document.body.appendChild(script);
  }, []);

  return (
    <section>
      <h2>Live Polymarket Feed</h2>
      <a
        className="twitter-timeline"
        data-theme="dark"
        data-height="500"
        href="https://twitter.com/Polymarket"
      >
        Tweets by Polymarket
      </a>
    </section>
  );
}