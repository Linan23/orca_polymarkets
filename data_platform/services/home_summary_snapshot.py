"""Build and read cached homepage summary snapshots."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, select, text
from sqlalchemy.orm import Session

from data_platform.models import HomeSummarySnapshot
from data_platform.settings import get_settings


settings = get_settings()

_LATEST_SUCCESSFUL_SCRAPE_SQL = text(
    """
    SELECT COALESCE(finished_at, started_at) AS observed_at
    FROM analytics.scrape_run
    WHERE status = 'success'
    ORDER BY finished_at DESC NULLS LAST, started_at DESC, scrape_run_id DESC
    LIMIT 1
    """
)

_LATEST_SCRAPE_RUN_SQL = text(
    """
    SELECT
      scrape_run_id,
      job_name,
      endpoint_name,
      status,
      started_at,
      finished_at,
      records_written,
      error_count,
      error_summary,
      COALESCE(finished_at, started_at) AS observed_at
    FROM analytics.scrape_run
    ORDER BY started_at DESC, scrape_run_id DESC
    LIMIT 1
    """
)

_LATEST_WHALE_BATCH_SQL = text(
    """
    SELECT snapshot_time, scoring_version
    FROM analytics.whale_score_snapshot
    ORDER BY snapshot_time DESC, whale_score_snapshot_id DESC
    LIMIT 1
    """
)

_WHALE_BATCH_COUNTS_SQL = text(
    """
    SELECT
      COUNT(*) FILTER (WHERE is_whale = TRUE)::integer AS whales_detected,
      COUNT(*) FILTER (WHERE is_trusted_whale = TRUE)::integer AS trusted_whales,
      COUNT(*) FILTER (WHERE profitability_score > 0)::integer AS profitability_users
    FROM analytics.whale_score_snapshot
    WHERE snapshot_time = :snapshot_time
      AND scoring_version = :scoring_version
    """
)

_TOP_TRUSTED_WHALE_SQL = text(
    """
    SELECT
      ua.user_id,
      ua.external_user_ref,
      ua.wallet_address,
      ua.preferred_username,
      ua.display_label,
      w.trust_score,
      w.profitability_score,
      w.sample_trade_count
    FROM analytics.whale_score_snapshot w
    JOIN analytics.user_account ua
      ON ua.user_id = w.user_id
    WHERE w.snapshot_time = :snapshot_time
      AND w.scoring_version = :scoring_version
      AND w.is_trusted_whale = TRUE
    ORDER BY w.trust_score DESC, w.sample_trade_count DESC, w.whale_score_snapshot_id DESC
    LIMIT 1
    """
)

_MOST_WHALE_MARKET_SQL = text(
    """
    WITH latest_dashboard AS (
      SELECT dashboard_id
      FROM analytics.dashboard
      ORDER BY generated_at DESC, dashboard_id DESC
      LIMIT 1
    )
    SELECT
      dm.market_slug,
      mc.question,
      dm.whale_count,
      dm.trusted_whale_count,
      dm.price
    FROM analytics.dashboard_market dm
    JOIN latest_dashboard ld
      ON ld.dashboard_id = dm.dashboard_id
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = dm.market_contract_id
    ORDER BY dm.trusted_whale_count DESC, dm.whale_count DESC, dm.volume DESC, dm.market_id ASC
    LIMIT 1
    """
)

_RESOLVED_COVERAGE_SQL = text(
    """
    WITH available AS (
      SELECT COUNT(*)::integer AS resolved_markets_available
      FROM analytics.resolved_condition rc
      JOIN analytics.platform p
        ON p.platform_id = rc.platform_id
      WHERE p.platform_name = 'polymarket'
    ),
    observed AS (
      SELECT COUNT(DISTINCT mc.condition_ref)::integer AS resolved_markets_observed
      FROM analytics.transaction_fact tf
      JOIN analytics.market_contract mc
        ON mc.market_contract_id = tf.market_contract_id
      JOIN analytics.resolved_condition rc
        ON rc.platform_id = tf.platform_id
       AND rc.condition_ref = mc.condition_ref
      JOIN analytics.platform p
        ON p.platform_id = tf.platform_id
      WHERE p.platform_name = 'polymarket'
        AND tf.outcome_label IS NOT NULL
        AND tf.side IN ('buy', 'sell')
    )
    SELECT
      available.resolved_markets_available,
      observed.resolved_markets_observed
    FROM available
    CROSS JOIN observed
    """
)

_PLATFORM_COVERAGE_SQL = text(
    """
    SELECT
      p.platform_name,
      COALESCE(ua.user_count, 0)::integer AS user_count,
      COALESCE(mc.market_count, 0)::integer AS market_count,
      COALESCE(tf.transaction_count, 0)::integer AS transaction_count,
      COALESCE(obs.orderbook_snapshot_count, 0)::integer AS orderbook_snapshot_count
    FROM analytics.platform p
    LEFT JOIN (
      SELECT platform_id, COUNT(*) AS user_count
      FROM analytics.user_account
      GROUP BY platform_id
    ) ua ON ua.platform_id = p.platform_id
    LEFT JOIN (
      SELECT platform_id, COUNT(*) AS market_count
      FROM analytics.market_contract
      GROUP BY platform_id
    ) mc ON mc.platform_id = p.platform_id
    LEFT JOIN (
      SELECT platform_id, COUNT(*) AS transaction_count
      FROM analytics.transaction_fact
      GROUP BY platform_id
    ) tf ON tf.platform_id = p.platform_id
    LEFT JOIN (
      SELECT platform_id, COUNT(*) AS orderbook_snapshot_count
      FROM analytics.orderbook_snapshot
      GROUP BY platform_id
    ) obs ON obs.platform_id = p.platform_id
    ORDER BY p.platform_name ASC
    """
)

_LATEST_DASHBOARD_TIME_SQL = text(
    """
    SELECT generated_at
    FROM analytics.dashboard
    ORDER BY generated_at DESC, dashboard_id DESC
    LIMIT 1
    """
)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _latest_successful_scrape_time(session: Session) -> datetime | None:
    return session.execute(_LATEST_SUCCESSFUL_SCRAPE_SQL).scalar_one_or_none()


def _freshness_metadata(
    *,
    observed_at: datetime | None,
    threshold_minutes: int,
    freshness_source: str,
    last_successful_ingest_at: datetime | None = None,
) -> dict[str, Any]:
    if observed_at is None:
        return {
            "is_stale": True,
            "stale_as_of": None,
            "freshness_source": freshness_source,
            "last_successful_ingest_at": _iso(last_successful_ingest_at),
        }
    stale_as_of = observed_at + timedelta(minutes=threshold_minutes)
    return {
        "is_stale": datetime.now(timezone.utc) > stale_as_of,
        "stale_as_of": stale_as_of.isoformat(),
        "freshness_source": freshness_source,
        "last_successful_ingest_at": _iso(last_successful_ingest_at),
    }


def _latest_scrape_run_payload(session: Session, *, last_successful_ingest_at: datetime | None) -> dict[str, Any] | None:
    row = session.execute(_LATEST_SCRAPE_RUN_SQL).mappings().first()
    if row is None:
        return None
    payload = {
        "scrape_run_id": int(row["scrape_run_id"]),
        "job_name": row["job_name"],
        "endpoint_name": row["endpoint_name"],
        "status": row["status"],
        "started_at": _iso(row["started_at"]),
        "finished_at": _iso(row["finished_at"]),
        "records_written": int(row["records_written"] or 0),
        "error_count": int(row["error_count"] or 0),
        "error_summary": row["error_summary"],
    }
    payload.update(
        _freshness_metadata(
            observed_at=row["observed_at"],
            threshold_minutes=settings.trade_feed_stale_minutes,
            freshness_source="analytics.scrape_run.finished_at",
            last_successful_ingest_at=last_successful_ingest_at,
        )
    )
    return payload


def compose_home_summary_payload(session: Session) -> dict[str, Any]:
    """Compose the homepage summary from precomputed/compact analytics state."""
    last_successful_ingest_at = _latest_successful_scrape_time(session)
    latest_batch = session.execute(_LATEST_WHALE_BATCH_SQL).mappings().first()
    scoring_version: str | None = None
    whales_detected = 0
    trusted_whales = 0
    profitability_users = 0
    top_trusted_whale: dict[str, Any] | None = None

    if latest_batch is not None:
        scoring_version = str(latest_batch["scoring_version"])
        batch_params = {
            "snapshot_time": latest_batch["snapshot_time"],
            "scoring_version": latest_batch["scoring_version"],
        }
        count_row = session.execute(_WHALE_BATCH_COUNTS_SQL, batch_params).mappings().one()
        whales_detected = int(count_row["whales_detected"] or 0)
        trusted_whales = int(count_row["trusted_whales"] or 0)
        profitability_users = int(count_row["profitability_users"] or 0)
        trusted_row = session.execute(_TOP_TRUSTED_WHALE_SQL, batch_params).mappings().first()
        if trusted_row is not None:
            top_trusted_whale = {
                "user_id": int(trusted_row["user_id"]),
                "external_user_ref": trusted_row["external_user_ref"],
                "wallet_address": trusted_row["wallet_address"],
                "preferred_username": trusted_row["preferred_username"],
                "display_label": trusted_row["display_label"],
                "trust_score": float(trusted_row["trust_score"] or 0),
                "profitability_score": float(trusted_row["profitability_score"] or 0),
                "sample_trade_count": int(trusted_row["sample_trade_count"] or 0),
            }

    market_row = session.execute(_MOST_WHALE_MARKET_SQL).mappings().first()
    most_whale_concentrated_market = None
    if market_row is not None:
        most_whale_concentrated_market = {
            "market_slug": market_row["market_slug"],
            "question": market_row["question"],
            "whale_count": int(market_row["whale_count"] or 0),
            "trusted_whale_count": int(market_row["trusted_whale_count"] or 0),
            "price": float(market_row["price"]) if market_row["price"] is not None else None,
        }

    resolved_row = session.execute(_RESOLVED_COVERAGE_SQL).mappings().one()
    platform_rows = session.execute(_PLATFORM_COVERAGE_SQL).mappings().all()
    latest_dashboard_time = session.execute(_LATEST_DASHBOARD_TIME_SQL).scalar_one_or_none()
    payload = {
        "scoring_version": scoring_version,
        "whales_detected": whales_detected,
        "trusted_whales": trusted_whales,
        "resolved_markets_available": int(resolved_row["resolved_markets_available"] or 0),
        "resolved_markets_observed": int(resolved_row["resolved_markets_observed"] or 0),
        "profitability_users": profitability_users,
        "top_trusted_whale": top_trusted_whale,
        "most_whale_concentrated_market": most_whale_concentrated_market,
        "latest_ingestion": _latest_scrape_run_payload(session, last_successful_ingest_at=last_successful_ingest_at),
        "platform_coverage": [
            {
                "platform_name": row["platform_name"],
                "user_count": int(row["user_count"] or 0),
                "market_count": int(row["market_count"] or 0),
                "transaction_count": int(row["transaction_count"] or 0),
                "orderbook_snapshot_count": int(row["orderbook_snapshot_count"] or 0),
            }
            for row in platform_rows
        ],
    }
    payload.update(
        _freshness_metadata(
            observed_at=latest_dashboard_time,
            threshold_minutes=settings.analytics_stale_minutes,
            freshness_source="analytics.dashboard.generated_at",
            last_successful_ingest_at=last_successful_ingest_at,
        )
    )
    return payload


def build_home_summary_snapshot(session: Session) -> dict[str, Any]:
    """Persist one compact homepage summary snapshot and return a build summary."""
    payload = compose_home_summary_payload(session)
    generated_at = datetime.now(timezone.utc)
    latest_successful_ingest_at = _latest_successful_scrape_time(session)
    row = HomeSummarySnapshot(
        generated_at=generated_at,
        scoring_version=payload.get("scoring_version"),
        whales_detected=int(payload.get("whales_detected") or 0),
        trusted_whales=int(payload.get("trusted_whales") or 0),
        resolved_markets_available=int(payload.get("resolved_markets_available") or 0),
        resolved_markets_observed=int(payload.get("resolved_markets_observed") or 0),
        profitability_users=int(payload.get("profitability_users") or 0),
        latest_successful_ingest_at=latest_successful_ingest_at,
        summary_payload=payload,
    )
    session.add(row)
    session.flush()
    return {
        "home_summary_snapshot_id": row.home_summary_snapshot_id,
        "generated_at": generated_at.isoformat(),
        "scoring_version": row.scoring_version,
        "whales_detected": row.whales_detected,
        "trusted_whales": row.trusted_whales,
        "resolved_markets_available": row.resolved_markets_available,
        "resolved_markets_observed": row.resolved_markets_observed,
        "profitability_users": row.profitability_users,
    }


def latest_home_summary_snapshot_payload(session: Session) -> dict[str, Any] | None:
    """Return the latest cached home summary payload with dynamic freshness fields."""
    row = session.scalars(
        select(HomeSummarySnapshot).order_by(
            desc(HomeSummarySnapshot.generated_at),
            desc(HomeSummarySnapshot.home_summary_snapshot_id),
        ).limit(1)
    ).first()
    if row is None:
        return None
    payload = dict(row.summary_payload or {})
    last_successful_ingest_at = _latest_successful_scrape_time(session)
    payload["latest_ingestion"] = _latest_scrape_run_payload(
        session,
        last_successful_ingest_at=last_successful_ingest_at,
    )
    payload.update(
        _freshness_metadata(
            observed_at=row.generated_at,
            threshold_minutes=settings.analytics_stale_minutes,
            freshness_source="analytics.home_summary_snapshot.generated_at",
            last_successful_ingest_at=last_successful_ingest_at,
        )
    )
    return payload
