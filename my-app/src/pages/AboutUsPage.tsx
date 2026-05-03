import TopNavbar from "../homepage/TopNavbar";

const goals = [
  {
    title: "Study Whale Activity",
    description:
      "Orca tracks high-volume traders in prediction markets to help users understand how large traders enter, exit, and move through markets over time.",
  },
  {
    title: "Explain Market Behavior",
    description:
      "The dashboard turns complex trading data into readable summaries, leaderboards, and signals so users can better study prediction market behavior.",
  },
  {
    title: "Identify Market Signals",
    description:
      "Our goal is to evaluate whether whale activity shows meaningful patterns connected to market inefficiencies, pricing behavior, or informed trading.",
  },
];

const clients = [
  {
    name: "XXX Club",
    description:
      "A student group interested in prediction markets, trading behavior, analytics, and real-world applications of market data.",
  },
  {
    name: "Computer Science Department",
    description:
      "Supports the technical side of the project, including data collection, dashboard design, algorithms, machine learning, and software engineering.",
  },
  {
    name: "Policy Studies Department",
    description:
      "Connects the project to public information, political events, market behavior, decision-making, and the broader impact of prediction markets.",
  },
];

const crew = [
  {
    role: "Captain",
    name: "Crystal",
    description:
      "Leads the project direction, coordinates the team, and helps guide the dashboard from idea to final product.",
  },
  {
    role: "Crew",
    name: "Anthony",
    description:
      "Supports development, research, data organization, dashboard implementation, and technical design.",
  },
  {
    role: "Crew",
    name: "Eric",
    description:
      "Supports development, analysis, documentation, project presentation, and research organization.",
  },
];

export default function AboutUsPage() {
  return (
    <div className="page page-about">
      <TopNavbar />

      <section className="about-hero-dark">
        <div className="about-hero-inner">
          <p className="eyebrow">Orca Polymarkets</p>

          <h1>About the dashboard behind the whale signals.</h1>

          <p>
            Orca is an informative dashboard built to help users study how
            prediction markets work, with a focus on whale activity,
            profitability, market concentration, and trading behavior.
          </p>
        </div>
      </section>

      <section className="about-purpose-section">
        <div className="about-purpose-inner">
          <p className="eyebrow">Purpose of Orca</p>

          <h2>Built to make prediction markets easier to understand.</h2>

          <p>
            This project analyzes high-volume traders, also called whales, in
            prediction markets with a main focus on Polymarket. The goal is to
            identify consistent whale activity, examine trading patterns, and
            study whether profitability comes from market inefficiencies,
            public signals, or behavior that may not be generalizable.
          </p>
        </div>
      </section>

      <section className="about-panel-section">
        <div className="about-panel">
          <div className="about-panel-header">
            <p className="eyebrow">Project Goals</p>
            <h2>What Orca is trying to do</h2>
          </div>

          <div className="about-card-grid">
            {goals.map((goal, index) => (
              <div className="about-info-card" key={goal.title}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <h3>{goal.title}</h3>
                <p>{goal.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="about-panel-section about-panel-section-alt">
        <div className="about-panel">
          <div className="about-panel-header">
            <p className="eyebrow">Clients</p>
            <h2>Who our clients are</h2>
          </div>

          <div className="about-list">
            {clients.map((client, index) => (
              <div className="about-list-row" key={client.name}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <h3>{client.name}</h3>
                <p>{client.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="about-crew-section">
        <div className="about-crew-inner">
          <div className="about-panel-header">
            <p className="eyebrow">Aboard Our Ship</p>
            <h2>Main project creators</h2>
          </div>

          <div className="about-crew-grid">
            {crew.map((member) => (
              <div
                key={member.name}
                className={
                  member.role === "Captain"
                    ? "about-crew-card captain"
                    : "about-crew-card"
                }
              >
                <span>{member.role}</span>
                <h3>{member.name}</h3>
                <p>{member.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}