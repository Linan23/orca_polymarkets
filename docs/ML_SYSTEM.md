# ML System

Orca's ML layer is a dashboard-support system, not an automatic trading system. It estimates how market probability may move after trusted whale activity and validates those forecasts against actual closed-market movement.

## Inputs

The model uses Polymarket-only data:

- Market probability at or near whale entry time.
- Trade direction and notional size.
- Whale trust score.
- Buy/sell pressure by side.
- Whale frequency, holding behavior, and historical reliability.
- Market category and time-to-close context.
- Closed-market outcomes and actual probability movement for validation.

## Forecast Output

Market Profile displays:

- Starting market probability.
- 12h market forecast.
- 24h market forecast.
- Both sides of the market, usually Yes and No.
- Whale lean and pressure summaries.
- Confidence/validation labels in user-facing language.

Basic forecast idea:

```text
Whale Pressure = Trade Notional x Whale Trust Score
Net Pressure = Buy Pressure - Sell Pressure
Predicted Future Probability = Current Probability + Predicted Probability Change
Error = Actual Future Probability - Predicted Future Probability
```

## Confidence

Confidence is validation-backed. A forecast is more trusted when similar historical forecasts were usually correct. A forecast is lower confidence when there are too few comparable cases, weak category history, or mixed whale pressure.

User-facing tiers:

- Strong: stronger validation history and higher confidence.
- Watch: useful signal, but less proven than Strong.
- Review: not enough validation or too much uncertainty.

## Validation

Closed-market data is used to compare:

- Predicted direction versus actual direction.
- Predicted probability versus actual probability.
- 12h and 24h forecast error.
- Category-level performance.
- Whether signals should be Strong, Watch, or Review.

Important metrics:

- Direction accuracy.
- Mean absolute error.
- Root mean squared error.
- Calibration error.
- Coverage by category and time window.

## Known Limitations

- Forecasts are selective signals, not guaranteed outcomes.
- Thin categories can have weak validation history.
- Fast-breaking news can overwhelm whale-pressure signals.
- New markets may start as Review until enough comparable history exists.
- Esports/video-game markets remain review-oriented until row count and validation gates improve.

## Developer Notes

Keep generated ML report JSON/Markdown and runtime artifacts out of Git unless a report is intentionally added for documentation. Current report snapshots should live under ignored runtime or release storage paths.
