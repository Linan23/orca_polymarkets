
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
    points: [
      "Orca collects market and trader behavior data.",
      "The model looks for patterns in whale activity, volume, and past trades.",
      "It compares current behavior to historical outcomes.",
      "The goal is to help estimate which traders may give stronger market signals.",
    ],
  },
  {
    title: "How the Trust Score Works",
    eyebrow: "Trust Score",
    type: "trust",
    points: [
      "The trust score ranks traders based on consistency and past performance.",
      "Higher scores suggest stronger historical signal quality.",
      "The score can use factors like win rate, volume, timing, and profitability.",
      "Trusted whales are users with stronger scores and more reliable behavior patterns.",
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
  {slides[1].points?.map((point, i) => (
    <div className="definition-row" key={point}>
      <h3>{i + 1}</h3>
      <p>{point}</p>
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
  {slides[2].points?.map((point, i) => (
    <div className="definition-row" key={point}>
      <h3>{i + 1}</h3>
      <p>{point}</p>
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
