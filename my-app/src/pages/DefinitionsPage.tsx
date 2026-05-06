
import TopNavbar from "../homepage/TopNavbar";
import { useState } from "react";



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
    title: "How the ML Works",
    eyebrow: "Machine Learning",
    type: "ml",
    definitions: [
      {
        term: "Market Odds",
        description:
          "The current Yes or No probability shown by the market. For example, 77% means the market side is priced around a 77% chance.",
      },
      {
        term: "Prediction Trend",
        description:
          "The model’s expected odds movement after whale activity. It shows whether the market may move up or down over the forecast window.",
      },
      {
        term: "Whale Entry",
        description:
          "The point when a large or trusted trader starts buying into a market. Orca uses this as the starting point for the prediction trend.",
      },
      {
        term: "12h / 24h Forecast",
        description:
          "The model estimates where the market odds may move over the next 12 and 24 hours after whale activity is detected.",
      },
      {
        term: "Review Only",
        description:
          "A signal shown for context, but not treated as reliable yet. It usually means the model needs more data or stronger validation for that market type.",
      },
      {
        term: "Validated Slice",
        description:
          "A group of similar past predictions that has enough history to compare forecasts against actual market movement.",
      },
      {
        term: "Low Confidence",
        description:
          "A prediction where the model does not have enough support to make a strong call. Users should treat it as weak or uncertain.",
      },
      {
        term: "Validation",
        description:
          "A check that compares older predictions against what actually happened later. This helps show whether the model is matching real market movement.",
      },
    ],
  },
  {
    title: "How the Trust Score Works",
    eyebrow: "Trust Score",
    type: "trust",
    definitions: [
      {
        term: "Find Whale Activity",
        description:
          "Orca first looks for traders with large positions, high volume, or repeated activity in Polymarket markets.",
      },
      {
        term: "Check Trade History",
        description:
          "The system reviews each trader’s past markets, profit and loss, timing, buying behavior, selling behavior, and trading frequency.",
      },
      {
        term: "Score Reliability",
        description:
          "A higher trust score means the trader has shown stronger historical signal quality. A lower score means the trader has weaker or less consistent history.",
      },
      {
        term: "Separate Whale Types",
        description:
          "A whale has meaningful activity. A potential whale shows some whale-like behavior but needs more history. A trusted whale has stronger evidence of useful past signals.",
      },
      {
        term: "Apply Model Weight",
        description:
          "Trusted whales can carry more influence in the ML model than weaker whales. This helps the model avoid treating every large trader the same.",
      },
      {
        term: "Update Over Time",
        description:
          "Trust scores can change as new trades are collected. Better future performance can strengthen a score, while weak or inconsistent behavior can reduce it.",
      },
    ],
  },
];



export default function DefinitionsPage() {

const [activeSlide, setActiveSlide] = useState(0);

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
            leaderboards, and trading signals. The goal is to make complex
            market activity easier to understand — not to provide financial
            advice.
          </p>
        </div>
      </section>

<section className="definitions-section">
  <div className="carousel-container">
    <button
      className="carousel-btn left"
      onClick={() => setActiveSlide((prev) => Math.max(prev - 1, 0))}
    >
      ‹
    </button>

    <div
      className="carousel-track"
      style={{ transform: `translateX(-${activeSlide * 100}%)` }}
    >
      {/* SLIDE 1: DEFINITIONS */}
      <div className="carousel-slide">
        <div className="carousel-card definitions-panel">
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

      {/* SLIDE 2: ML */}
      <div className="carousel-slide">
        <div className=" carousel-card definitions-panel">
        <div className="definitions-panel-header">
  <p className="eyebrow">Machine Learning</p>
  <h2>How the ML Works</h2>
</div>

          <div className="definitions-list">
  {slides[1].definitions?.map((item) => (
    <div className="definition-row" key={item.term}>
      <h3>{item.term}</h3>
      <p>{item.description}</p>
    </div>
  ))}
</div>


        </div>
      </div>

      {/* SLIDE 3: TRUST */}
      <div className="carousel-slide">
        <div className="carousel-card definitions-panel">
     <div className="definitions-panel-header">
  <p className="eyebrow">Trust Score</p>
  <h2>How the Trust Score Works</h2>
</div>

       <div className="definitions-list">
  {slides[2].definitions?.map((item) => (
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
      onClick={() =>
        setActiveSlide((prev) => Math.min(prev + 1, slides.length - 1))
      }
    >
      ›
    </button>

    {/* DOTS */}
    <div className="carousel-dots">
      {slides.map((_, i) => (
        <div
          key={i}
          className={`dot ${activeSlide === i ? "active" : ""}`}
          onClick={() => setActiveSlide(i)}
        />
      ))}
    </div>
  </div>
</section>

    </div>
  );

}
