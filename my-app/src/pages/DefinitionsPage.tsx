
import TopNavbar from "../homepage/TopNavbar";
import { useLayoutEffect, useRef, useState } from "react";



const definitions = [
  {
    term: "Whales",
    description:
      "Users or wallets with large positions, high trading volume, or meaningful influence in a market.",
  },

  {
  term: "Trusted Whales",
  description:
    "Trusted Whales are highly successful traders identified through Orca’s proprietary trust-scoring model, which uses machine learning to evaluate consistency, signal quality, and historical market performance.",
},


  {
  term: "Volume",
  description:
    "The total amount of a security/stock that has been traded during a specific time period. High Volume can indicate strong interest and liquidity in a market.",
},

  {
    term: "Notional",
    description:
      "The dollar value of a trade or position. It helps show how much money is behind the activity, not just how many shares were traded.",
  },

  {
  term: "Liquidity",
  description:
    "The ease with which an asset or security can be bought or sold in the market without significantly affecting its price. High liquidity indicates a large number of buyers and sellers.",
},

  {
    term: "P & L",
    description:
      "Profit and Loss. It shows how much a trader, wallet, or position has gained or lost over time.",
  },
  {
    term: "Profits",
    description:
      "Positive gains from trades or market positions when the outcome is worth more than what was spent.",
  },
  {
    term: "Losses",
    description:
      "Negative results from trades or market positions when the outcome is worth less than what was spent.",
  },
];

const slides = [
  {
    title: "Key Definitions",
    eyebrow: "Glossary",
    type: "definitions",
  },
  {
    title: "ML Definitions",
    eyebrow: "Machine Learning",
    type: "ml",
    definitions: [
      {
        term: "Collect Market Data",
        description:
          "Orca collects Polymarket odds, market volume, notional activity, whale trades, and historical market movement.",
      },
      {
        term: "Detect Whale Entry",
        description:
          "The model looks for moments when large or trusted whales enter a market. That entry point becomes the start of the prediction trend.",
      },
      {
        term: "Read Market Odds",
        description:
          "The current Yes or No probability gives the model a starting price. For example, 77% means the market side is priced around a 77% chance.",
      },
      {
        term: "Forecast 12h / 24h",
        description:
          "After whale activity is detected, the model estimates whether the market odds may move up or down over the next 12 and 24 hours.",
      },
      {
        term: "Assign Confidence",
        description:
          "The model labels predictions by reliability. Strong is higher confidence, Watch is useful but less certain, and Review Only means the signal needs more proof.",
      },
      {
        term: "Strong Signal",
        description:
          "A forecast with stronger model confidence and better past validation. It is still a prediction, but it is the model’s most reliable signal type.",
      },
      {
        term: "Watch Signal",
        description:
          "A useful forecast that may be worth watching, but it has less proof than a Strong signal. Users should treat it as a clue, not a confirmed call.",
      },
      {
        term: "Review Only",
        description:
          "A forecast shown for transparency because the model does not have enough validated proof yet. It should not be treated as a reliable signal.",
      },
      {
        term: "Low Confidence",
        description:
          "The model sees a possible direction, but similar past forecasts have not been accurate enough. This means the prediction needs more data or validation.",
      },
      {
        term: "Validated Slice",
        description:
          "A group of similar past predictions that has been checked against actual market movement. The model uses this to explain how reliable a signal has been.",
      },
      {
        term: "Accuracy",
        description:
          "How often similar completed forecasts matched the actual market direction. For example, 80% means 8 out of 10 similar forecasts moved the right way.",
      },
      {
        term: "Validate Against Reality",
        description:
          "Older predictions are compared against what actually happened later. This helps show whether the model is matching real market movement.",
      },
      {
        term: "Show the Trend",
        description:
          "The dashboard shows the prediction trend next to the market trend so users can see where the model expected odds to move.",
      },
      {
        term: "Update With New Data",
        description:
          "As new market and whale data comes in, Orca can refresh predictions and improve future validation.",
      },
    ],
  },
  {
    title: "How ML Works",
    eyebrow: "ML Process",
    type: "ml-process",
    definitions: [
      {
        term: "1. Input",
        description:
          "Orca starts with Polymarket data: current market probability, whale trades, trade size, trusted whale activity, and recent market movement.",
      },
      {
        term: "2. Read the Signal",
        description:
          "The model checks which side whales are supporting, how trusted those whales are, and whether similar whale activity has mattered before.",
      },
      {
        term: "3. Output",
        description:
          "Orca starts with the current market probability, adds the buying or selling pressure from trusted whales, then compares that setup with similar past markets. The result is a 12-hour and 24-hour forecast, plus a confidence level based on how often similar forecasts were right before.",
      },
      {
        term: "4. Validate",
        description:
          "When enough time passes, Orca compares the forecast with what actually happened and uses that history to judge future predictions.",
      },
    ],
  },
  {
    title: "How the Trust Score Works",
    eyebrow: "Trust Score",
    type: "trust",
    definitions: [
      {
        term: "1. Find Whale Activity",
        description:
          "Orca first looks for traders who make large trades, hold meaningful positions, or trade often enough to stand out in Polymarket markets.",
      },
      {
        term: "2. Review Past Behavior",
        description:
          "Next, it checks how that trader behaved before: what they bought or sold, how often they traded, how long they held positions, and whether past trades tended to work.",
      },
      {
        term: "3. Score Reliability",
        description:
          "The system turns that history into a trust score. A higher score means the trader has shown more useful and consistent signals. A lower score means the history is weaker or less proven.",
      },
      {
        term: "4. Group Whale Types",
        description:
          "Traders are grouped by strength. A potential whale shows early whale-like activity, a whale has meaningful activity, and a trusted whale has stronger evidence from past behavior.",
      },
      {
        term: "5. Apply Influence",
        description:
          "Trusted whales carry more weight in the ML forecast than weaker or less-proven whales. This helps the model avoid treating every large trade as equally important.",
      },
      {
        term: "6. Update Over Time",
        description:
          "Trust scores are not permanent. As new trades are collected, strong future behavior can raise a score, while weak or inconsistent behavior can lower it.",
      },
    ],
  },
];



export default function DefinitionsPage() {

const [activeSlide, setActiveSlide] = useState(0);
const [activeSlideHeight, setActiveSlideHeight] = useState<number | null>(null);
const carouselTopRef = useRef<HTMLDivElement | null>(null);
const slideRefs = useRef<Array<HTMLDivElement | null>>([]);
const cardRefs = useRef<Array<HTMLDivElement | null>>([]);

const scrollToCardTop = (slideIndex: number) => {
  const target = cardRefs.current[slideIndex];
  if (!target) return;
  const topbarOffset = 80;
  const targetTop = target.getBoundingClientRect().top + window.scrollY - topbarOffset;
  window.scrollTo({ top: Math.max(0, targetTop), left: 0, behavior: "auto" });
};

const setSlideAndScroll = (nextSlide: number | ((current: number) => number)) => {
  const resolved = typeof nextSlide === "function" ? nextSlide(activeSlide) : nextSlide;
  const nextIndex = Math.min(Math.max(resolved, 0), slides.length - 1);
  setActiveSlide(nextIndex);
  requestAnimationFrame(() => {
    requestAnimationFrame(() => scrollToCardTop(nextIndex));
  });
};

useLayoutEffect(() => {
  const activeNode = slideRefs.current[activeSlide];
  if (!activeNode) return;

  const updateHeight = () => {
    setActiveSlideHeight(activeNode.scrollHeight);
  };

  updateHeight();
  window.addEventListener("resize", updateHeight);
  const observer = typeof ResizeObserver !== "undefined" ? new ResizeObserver(updateHeight) : null;
  observer?.observe(activeNode);

  return () => {
    window.removeEventListener("resize", updateHeight);
    observer?.disconnect();
  };
}, [activeSlide]);

  return (
    <div className="page page-definitions">
      <TopNavbar />

      <section className="definitions-hero">
        <div className="definitions-hero-inner">
          <p className="eyebrow">Orca Polymarkets</p>
          <h1>Understand the markets before following the money.</h1>
          <p>
            Orca explains key Polymarket terms so users can better understand
            whale activity, P & L, profits, losses, and trading behavior.
          </p>
        </div>
      </section>

      <section className="purpose-section">
        <div className="purpose-inner">
          <p className="eyebrow">Purpose of Orca</p>
          <h2>Built to educate users.</h2>
          <p>
            Orca helps users learn from market data, whale behavior,
            leaderboards, and trading-signals. Disclaimer: Dashboard does not provide financial advice or surveil users.
          </p>
        </div>
      </section>

<section className="definitions-section">
  <div className="carousel-container" ref={carouselTopRef}>
    <button
      className="carousel-btn left"
      type="button"
      aria-label="Previous glossary card"
      disabled={activeSlide === 0}
      onClick={() => setSlideAndScroll((prev) => prev - 1)}
    >
      ←
    </button>

    <div
      className="carousel-track"
      style={{
        transform: `translateX(-${activeSlide * 100}%)`,
        height: activeSlideHeight ? `${activeSlideHeight}px` : undefined,
      }}
    >
      {/* SLIDE 1: DEFINITIONS */}
      <div className="carousel-slide" ref={(node) => { slideRefs.current[0] = node; }}>
        <div className="carousel-card definitions-panel" ref={(node) => { cardRefs.current[0] = node; }}>
          <div className="definitions-panel-header">
            <p className="eyebrow">Glossary</p>
            <h2>Key Definitions</h2>
          </div>

          <div className="definitions-list">
            {definitions.map((item) => (
              <div className="definition-row" key={item.term}>
                <h3>{item.term}</h3>
                <p>{item.description}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* SLIDE 2: ML DEFINITIONS */}
      <div className="carousel-slide" ref={(node) => { slideRefs.current[1] = node; }}>
        <div className=" carousel-card definitions-panel" ref={(node) => { cardRefs.current[1] = node; }}>
        <div className="definitions-panel-header">
  <p className="eyebrow">Machine Learning</p>
  <h2>ML Definitions</h2>
</div>

          <div className="definitions-list definitions-list-ml">
  {slides[1].definitions?.map((item) => (
    <div className="definition-row" key={item.term}>
      <h3>{item.term}</h3>
      <p>{item.description}</p>
    </div>
  ))}
</div>


        </div>
      </div>

      {/* SLIDE 3: HOW ML WORKS */}
      <div className="carousel-slide" ref={(node) => { slideRefs.current[2] = node; }}>
        <div className="carousel-card definitions-panel" ref={(node) => { cardRefs.current[2] = node; }}>
     <div className="definitions-panel-header">
  <p className="eyebrow">ML Process</p>
  <h2>How ML Works</h2>
</div>

       <div className="definitions-list definitions-list-steps">
  {slides[2].definitions?.map((item) => (
    <div className="definition-row" key={item.term}>
      <h3>{item.term}</h3>
      <p>{item.description}</p>
    </div>
  ))}
</div>

        </div>
      </div>

      {/* SLIDE 4: TRUST */}
      <div className="carousel-slide" ref={(node) => { slideRefs.current[3] = node; }}>
        <div className="carousel-card definitions-panel" ref={(node) => { cardRefs.current[3] = node; }}>
     <div className="definitions-panel-header">
  <p className="eyebrow">Trust Score</p>
  <h2>How the Trust Score Works</h2>
</div>

       <div className="definitions-list">
  {slides[3].definitions?.map((item) => (
    <div className="definition-row" key={item.term}>
      <h3>{item.term}</h3>
      <p>{item.description}</p>
    </div>
  ))}
</div>

        </div>
      </div>
    </div>

    <button
      className="carousel-btn right"
      type="button"
      aria-label="Next glossary card"
      disabled={activeSlide === slides.length - 1}
      onClick={() => setSlideAndScroll((prev) => prev + 1)}
    >
      →
    </button>

    {/* DOTS */}
    <div className="carousel-dots">
      {slides.map((_, i) => (
        <div
          key={i}
          className={`dot ${activeSlide === i ? "active" : ""}`}
          onClick={() => setSlideAndScroll(i)}
        />
      ))}
    </div>
  </div>
</section>

    </div>
  );

}
