-- Deliverable Queries
-- Run with:
--   ./data_platform/open_psql.sh -f data_platform/sql/deliverable_queries.sql

\echo '=== Pipeline Coverage ==='
SELECT 'market_event' AS table_name, count(*) AS row_count FROM analytics.market_event
UNION ALL
SELECT 'market_contract', count(*) FROM analytics.market_contract
UNION ALL
SELECT 'transaction_fact', count(*) FROM analytics.transaction_fact
UNION ALL
SELECT 'position_snapshot', count(*) FROM analytics.position_snapshot
UNION ALL
SELECT 'orderbook_snapshot', count(*) FROM analytics.orderbook_snapshot
UNION ALL
SELECT 'whale_score_snapshot', count(*) FROM analytics.whale_score_snapshot
UNION ALL
SELECT 'raw.api_payload', count(*) FROM raw.api_payload
ORDER BY table_name;

\echo ''
\echo '=== Latest Ingestion Runs ==='
SELECT
  scrape_run_id,
  job_name,
  endpoint_name,
  status,
  records_written,
  error_count,
  started_at,
  finished_at
FROM analytics.scrape_run
ORDER BY scrape_run_id DESC
LIMIT 15;

\echo ''
\echo '=== Current Whale Leaderboard ==='
WITH latest_scores AS (
  SELECT DISTINCT ON (w.user_id)
    w.user_id,
    w.snapshot_time,
    w.raw_volume_score,
    w.consistency_score,
    w.profitability_score,
    w.trust_score,
    w.is_whale,
    w.is_trusted_whale,
    w.sample_trade_count,
    w.scoring_version
  FROM analytics.whale_score_snapshot w
  ORDER BY w.user_id, w.snapshot_time DESC, w.whale_score_snapshot_id DESC
)
SELECT
  ls.user_id,
  p.platform_name,
  ua.external_user_ref,
  ua.wallet_address,
  ls.raw_volume_score,
  ls.consistency_score,
  ls.profitability_score,
  ls.trust_score,
  ls.is_whale,
  ls.is_trusted_whale,
  ls.sample_trade_count,
  ls.scoring_version,
  ls.snapshot_time
FROM latest_scores ls
JOIN analytics.user_account ua ON ua.user_id = ls.user_id
JOIN analytics.platform p ON p.platform_id = ua.platform_id
ORDER BY ls.trust_score DESC, ls.raw_volume_score DESC
LIMIT 20;

\echo ''
\echo '=== Trusted Whale Leaderboard ==='
WITH latest_scores AS (
  SELECT DISTINCT ON (w.user_id)
    w.user_id,
    w.trust_score,
    w.raw_volume_score,
    w.consistency_score,
    w.profitability_score,
    w.is_trusted_whale,
    w.snapshot_time
  FROM analytics.whale_score_snapshot w
  ORDER BY w.user_id, w.snapshot_time DESC, w.whale_score_snapshot_id DESC
)
SELECT
  ls.user_id,
  p.platform_name,
  ua.external_user_ref,
  ls.trust_score,
  ls.raw_volume_score,
  ls.consistency_score,
  ls.profitability_score,
  ls.snapshot_time
FROM latest_scores ls
JOIN analytics.user_account ua ON ua.user_id = ls.user_id
JOIN analytics.platform p ON p.platform_id = ua.platform_id
WHERE ls.is_trusted_whale = TRUE
ORDER BY ls.trust_score DESC
LIMIT 20;

\echo ''
\echo '=== User Profile Summary (Latest Dashboard) ==='
WITH latest_dashboard AS (
  SELECT dashboard_id
  FROM analytics.dashboard
  ORDER BY dashboard_id DESC
  LIMIT 1
)
SELECT
  up.user_profile_id,
  up.user_id,
  p.platform_name,
  ua.external_user_ref,
  up.primary_market_ref,
  up.total_volume,
  up.total_shares,
  up.profit_loss,
  up.win_rate,
  up.created_at
FROM analytics.user_profile up
JOIN latest_dashboard ld ON ld.dashboard_id = up.dashboard_id
JOIN analytics.user_account ua ON ua.user_id = up.user_id
JOIN analytics.platform p ON p.platform_id = ua.platform_id
ORDER BY up.total_volume DESC, up.total_shares DESC;

\echo ''
\echo '=== Whale Market Concentration ==='
WITH latest_positions AS (
  SELECT DISTINCT ON (ps.user_id, ps.market_contract_id)
    ps.user_id,
    ps.market_contract_id,
    ps.position_size,
    ps.market_value,
    ps.snapshot_time
  FROM analytics.position_snapshot ps
  ORDER BY ps.user_id, ps.market_contract_id, ps.snapshot_time DESC, ps.position_snapshot_id DESC
),
latest_scores AS (
  SELECT DISTINCT ON (w.user_id)
    w.user_id,
    w.is_whale,
    w.is_trusted_whale
  FROM analytics.whale_score_snapshot w
  ORDER BY w.user_id, w.snapshot_time DESC, w.whale_score_snapshot_id DESC
)
SELECT
  mc.market_contract_id,
  p.platform_name,
  mc.market_slug,
  mc.question,
  COUNT(DISTINCT lp.user_id) AS holder_count,
  SUM(lp.position_size) AS total_position_size,
  SUM(COALESCE(lp.market_value, 0)) AS total_market_value,
  COUNT(DISTINCT CASE WHEN ls.is_whale THEN lp.user_id END) AS whale_holder_count,
  COUNT(DISTINCT CASE WHEN ls.is_trusted_whale THEN lp.user_id END) AS trusted_whale_holder_count
FROM latest_positions lp
JOIN analytics.market_contract mc ON mc.market_contract_id = lp.market_contract_id
JOIN analytics.platform p ON p.platform_id = mc.platform_id
LEFT JOIN latest_scores ls ON ls.user_id = lp.user_id
GROUP BY mc.market_contract_id, p.platform_name, mc.market_slug, mc.question
ORDER BY trusted_whale_holder_count DESC, whale_holder_count DESC, total_market_value DESC
LIMIT 20;

\echo ''
\echo '=== Widest Current Spreads ==='
WITH latest_books AS (
  SELECT DISTINCT ON (o.market_contract_id)
    o.market_contract_id,
    o.platform_id,
    o.best_bid,
    o.best_ask,
    o.mid_price,
    o.spread,
    o.depth_levels,
    o.bid_depth_notional,
    o.ask_depth_notional,
    o.snapshot_time
  FROM analytics.orderbook_snapshot o
  ORDER BY o.market_contract_id, o.snapshot_time DESC, o.orderbook_snapshot_id DESC
)
SELECT
  p.platform_name,
  mc.market_contract_id,
  mc.market_slug,
  mc.question,
  lb.best_bid,
  lb.best_ask,
  lb.mid_price,
  lb.spread,
  lb.depth_levels,
  lb.bid_depth_notional,
  lb.ask_depth_notional,
  lb.snapshot_time
FROM latest_books lb
JOIN analytics.market_contract mc ON mc.market_contract_id = lb.market_contract_id
JOIN analytics.platform p ON p.platform_id = lb.platform_id
WHERE lb.spread IS NOT NULL
ORDER BY lb.spread DESC, lb.ask_depth_notional ASC
LIMIT 20;

\echo ''
\echo '=== Most Active Markets by Recorded Trade Notional ==='
SELECT
  mc.market_contract_id,
  p.platform_name,
  mc.market_slug,
  mc.question,
  COUNT(tf.transaction_id) AS trade_count,
  SUM(COALESCE(tf.notional_value, 0)) AS total_notional,
  AVG(tf.price) AS avg_trade_price,
  MIN(tf.transaction_time) AS first_trade_seen,
  MAX(tf.transaction_time) AS last_trade_seen
FROM analytics.transaction_fact tf
JOIN analytics.market_contract mc ON mc.market_contract_id = tf.market_contract_id
JOIN analytics.platform p ON p.platform_id = tf.platform_id
GROUP BY mc.market_contract_id, p.platform_name, mc.market_slug, mc.question
ORDER BY total_notional DESC, trade_count DESC
LIMIT 20;

\echo ''
\echo '=== Tag Coverage ==='
SELECT
  mt.tag_slug,
  mt.tag_label,
  COUNT(DISTINCT mtm.event_id) AS tagged_event_count
FROM analytics.market_tag mt
JOIN analytics.market_tag_map mtm ON mtm.tag_id = mt.tag_id
GROUP BY mt.tag_slug, mt.tag_label
ORDER BY tagged_event_count DESC, mt.tag_slug ASC
LIMIT 30;

\echo ''
\echo '=== Latest Dashboard Market Rows ==='
WITH latest_dashboard AS (
  SELECT dashboard_id
  FROM analytics.dashboard
  ORDER BY dashboard_id DESC
  LIMIT 1
)
SELECT
  dm.market_id,
  dm.market_contract_id,
  mc.platform_id,
  p.platform_name,
  dm.market_slug,
  dm.price,
  dm.volume,
  dm.odds,
  dm.orderbook_depth,
  dm.whale_count,
  dm.trusted_whale_count,
  dm.read_time
FROM analytics.dashboard_market dm
JOIN analytics.market_contract mc ON mc.market_contract_id = dm.market_contract_id
JOIN analytics.platform p ON p.platform_id = mc.platform_id
JOIN latest_dashboard ld ON ld.dashboard_id = dm.dashboard_id
ORDER BY dm.trusted_whale_count DESC, dm.whale_count DESC, dm.volume DESC
LIMIT 20;

\echo ''
\echo '=== Raw Payload Audit ==='
SELECT
  p.platform_name,
  a.entity_type,
  COUNT(*) AS payload_count,
  MIN(a.collected_at) AS first_seen,
  MAX(a.collected_at) AS last_seen
FROM raw.api_payload a
JOIN analytics.platform p ON p.platform_id = a.platform_id
GROUP BY p.platform_name, a.entity_type
ORDER BY p.platform_name, a.entity_type;
