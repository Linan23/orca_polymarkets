# Client ML Update

## What Works

- The ML scope is now Polymarket only because Polymarket wallet identity supports trader-level whale tracking.
- The market-level dataset exports resolved Polymarket snapshots with 12h and 24h forward movement targets.
- Whale features include trusted whale weights, trading frequency, entry/exit behavior, holding time, and realized profit behavior.
- The residual model setup first fits price-only movement, then tests whether whale behavior explains the remaining movement.
- On the larger Polymarket trade-covered backfill, Ridge is the current best claim model for both 12h and 24h residual movement prediction.

## What Is Still Uncertain

- The result is sensitive to market segment, especially crypto up/down markets.
- The winner changes on the smaller first backfill, so data coverage still matters.
- This is a validated research signal, not a production trading model.
- More resolved trade coverage would improve confidence in segment-specific conclusions.

## Why Kalshi Is Excluded

Kalshi is excluded from the whale ML claim because wallet-level trader identity cannot be tracked with the same confidence. Without stable trader identity, the model cannot reliably reconstruct whale trust scores, buy/sell frequency, entry/exit behavior, holding time, or realized strategy. Keeping the ML claim Polymarket-only makes the result more technically defensible.

## Current Client-Facing Claim

Whale behavior improves 12h and 24h residual market-movement prediction on the larger Polymarket trade-covered dataset, with Ridge currently the most stable claim model. Random Forest and conservative LightGBM remain supporting benchmarks. The result should be presented with caveats around segment sensitivity and current data coverage.

## Should Use For

- Week 10-11 progress review.
- Feasibility update on whale-based movement prediction.
- Explaining why Polymarket is the correct first ML scope.
- Research reporting and internal dashboard metadata.

## Should Not Use For

- Production trading recommendations.
- Claims about Kalshi whale behavior.
- Claims that the same model wins across every market segment.
- Real-time predictions without exposing model date, dataset scope, and caveats.
