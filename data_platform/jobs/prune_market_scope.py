"""Prune market-linked data outside the configured focus domains."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.models import MarketContract, MarketEvent, MarketTag, MarketTagMap, Platform
from data_platform.services.market_scope import (
    DEFAULT_FOCUS_DOMAINS,
    add_focus_domain_argument,
    build_market_scope_texts,
    canonicalize_focus_domains,
    matched_focus_domains,
)
from data_platform.settings import get_settings


CORE_TABLES = (
    "analytics.market_event",
    "analytics.market_contract",
    "analytics.transaction_fact",
    "analytics.transaction_fact_part",
    "analytics.orderbook_snapshot",
    "analytics.orderbook_snapshot_part",
    "analytics.position_snapshot",
    "analytics.position_snapshot_part",
    "analytics.user_account",
    "raw.api_payload",
    "raw.api_payload_part",
)


@dataclass
class ScopeExample:
    platform: str
    event_id: int
    market_contract_id: int | None
    event_slug: str | None
    market_slug: str | None
    title: str | None
    question: str | None
    matched_domains: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prune out-of-scope market data from the local database.")
    parser.add_argument("--database-url", default=get_settings().database_url)
    add_focus_domain_argument(parser)
    parser.add_argument(
        "--platform",
        action="append",
        default=[],
        choices=["polymarket", "kalshi"],
        help="Repeatable platform scope. Defaults to both polymarket and kalshi.",
    )
    parser.add_argument("--sample-size", type=int, default=12, help="How many kept/removed examples to include in the summary.")
    parser.add_argument("--apply", action="store_true", help="Actually delete out-of-scope rows. Dry-run by default.")
    parser.add_argument(
        "--preserve-current-events",
        action="store_true",
        help=(
            "Keep current analytics.market_event rows for removed scopes, while still deleting market-linked facts, "
            "tag maps, histories, and unlinking event raw payload references."
        ),
    )
    args = parser.parse_args()
    try:
        args.focus_domains = canonicalize_focus_domains(args.focus_domain) or list(DEFAULT_FOCUS_DOMAINS)
    except ValueError as exc:
        parser.error(str(exc))
    args.platforms = args.platform or ["polymarket", "kalshi"]
    if args.sample_size <= 0:
        parser.error("--sample-size must be > 0.")
    return args


def _load_event_tags(session: Session, event_ids: set[int]) -> dict[int, list[str]]:
    if not event_ids:
        return {}
    rows = session.execute(
        select(MarketTagMap.event_id, MarketTag.tag_label, MarketTag.tag_slug)
        .join(MarketTag, MarketTag.tag_id == MarketTagMap.tag_id)
        .where(MarketTagMap.event_id.in_(event_ids))
    ).all()
    grouped: dict[int, list[str]] = {}
    for event_id, tag_label, tag_slug in rows:
        grouped.setdefault(int(event_id), []).extend(
            [value for value in (tag_label, tag_slug) if isinstance(value, str) and value.strip()]
        )
    return grouped


def _table_count(session: Session, table_name: str) -> int:
    return int(session.execute(text(f"SELECT count(*) FROM {table_name}")).scalar_one())


def _platform_ids(session: Session, platform_names: list[str]) -> dict[str, int]:
    rows = session.execute(
        select(Platform.platform_name, Platform.platform_id).where(Platform.platform_name.in_(platform_names))
    ).all()
    return {str(name): int(platform_id) for name, platform_id in rows}


def _example_for(
    *,
    platform: str,
    event: MarketEvent,
    market: MarketContract | None,
    matched_domains: list[str],
) -> ScopeExample:
    return ScopeExample(
        platform=platform,
        event_id=event.event_id,
        market_contract_id=market.market_contract_id if market is not None else None,
        event_slug=event.slug,
        market_slug=market.market_slug if market is not None else None,
        title=event.title,
        question=market.question if market is not None else None,
        matched_domains=matched_domains,
    )


def build_scope_summary(session: Session, *, focus_domains: list[str], platforms: list[str], sample_size: int) -> dict[str, Any]:
    platform_ids = _platform_ids(session, platforms)
    if not platform_ids:
        return {
            "focus_domains": focus_domains,
            "platforms": platforms,
            "warning": "No matching platforms exist in the database.",
            "keep_event_ids": [],
            "keep_market_ids": [],
            "delete_event_ids": [],
            "delete_market_ids": [],
        }

    event_rows = session.execute(
        select(MarketEvent, Platform.platform_name)
        .join(Platform, Platform.platform_id == MarketEvent.platform_id)
        .where(Platform.platform_name.in_(platforms))
    ).all()
    market_rows = session.execute(
        select(MarketContract, MarketEvent, Platform.platform_name)
        .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .where(Platform.platform_name.in_(platforms))
    ).all()

    tags_by_event = _load_event_tags(session, {event.event_id for event, _ in event_rows})
    keep_event_ids: set[int] = set()
    keep_market_ids: set[int] = set()
    kept_examples: list[dict[str, Any]] = []
    removed_examples: list[dict[str, Any]] = []

    for market, event, platform_name in market_rows:
        domains = sorted(
            matched_focus_domains(
                build_market_scope_texts(
                    platform_name=platform_name,
                    event=event,
                    market=market,
                    tags=tags_by_event.get(event.event_id, []),
                ),
                focus_domains,
            )
        )
        keep = bool(domains)
        example = _example_for(platform=platform_name, event=event, market=market, matched_domains=domains)
        if keep:
            keep_market_ids.add(market.market_contract_id)
            keep_event_ids.add(event.event_id)
            if len(kept_examples) < sample_size:
                kept_examples.append(example.__dict__)
        elif len(removed_examples) < sample_size:
            removed_examples.append(example.__dict__)

    all_event_ids = {event.event_id for event, _ in event_rows}
    all_market_ids = {market.market_contract_id for market, _, _ in market_rows}
    deleted_event_ids = sorted(all_event_ids - keep_event_ids)
    deleted_market_ids = sorted(all_market_ids - keep_market_ids)

    return {
        "focus_domains": focus_domains,
        "platforms": platforms,
        "keep_event_ids": sorted(keep_event_ids),
        "keep_market_ids": sorted(keep_market_ids),
        "delete_event_ids": deleted_event_ids,
        "delete_market_ids": deleted_market_ids,
        "kept_examples": kept_examples,
        "removed_examples": removed_examples,
        "before_counts": {table_name: _table_count(session, table_name) for table_name in CORE_TABLES},
    }


def _execute_delete(session: Session, sql: str, params: dict[str, Any] | None = None) -> int:
    result = session.execute(text(sql), params or {})
    return int(result.rowcount or 0)


def apply_prune(
    session: Session,
    *,
    summary: dict[str, Any],
    platform_ids: list[int],
    preserve_current_events: bool = False,
) -> dict[str, int]:
    delete_market_ids: list[int] = list(summary["delete_market_ids"])
    delete_event_ids: list[int] = list(summary["delete_event_ids"])
    if not delete_market_ids and not delete_event_ids:
        return {}

    deleted_market_slugs = session.execute(
        select(MarketContract.market_slug).where(MarketContract.market_contract_id.in_(delete_market_ids))
    ).scalars().all() if delete_market_ids else []

    counts: dict[str, int] = {}

    counts["analytics.dashboard_market"] = _execute_delete(session, "DELETE FROM analytics.dashboard_market")
    counts["analytics.market_profile"] = _execute_delete(session, "DELETE FROM analytics.market_profile")
    counts["analytics.user_profile"] = _execute_delete(session, "DELETE FROM analytics.user_profile")
    counts["analytics.user_leaderboard"] = _execute_delete(session, "DELETE FROM analytics.user_leaderboard")
    counts["analytics.dashboard"] = _execute_delete(session, "DELETE FROM analytics.dashboard")
    counts["analytics.whale_score_snapshot_part"] = _execute_delete(session, "DELETE FROM analytics.whale_score_snapshot_part")
    counts["analytics.whale_score_snapshot"] = _execute_delete(session, "DELETE FROM analytics.whale_score_snapshot")

    delete_params = {
        "market_ids": delete_market_ids,
        "event_ids": delete_event_ids,
        "market_slugs": [slug for slug in deleted_market_slugs if slug],
    }

    if delete_market_ids:
        counts["analytics.orderbook_snapshot_hourly"] = _execute_delete(
            session,
            "DELETE FROM analytics.orderbook_snapshot_hourly WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )
        counts["analytics.orderbook_snapshot_daily"] = _execute_delete(
            session,
            "DELETE FROM analytics.orderbook_snapshot_daily WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )
        counts["analytics.position_snapshot_daily"] = _execute_delete(
            session,
            """
            DELETE FROM analytics.position_snapshot_daily
            WHERE market_contract_id = ANY(:market_ids)
               OR event_id = ANY(:event_ids)
            """,
            delete_params,
        )
        counts["analytics.orderbook_snapshot_part"] = _execute_delete(
            session,
            "DELETE FROM analytics.orderbook_snapshot_part WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )
        counts["analytics.orderbook_snapshot"] = _execute_delete(
            session,
            "DELETE FROM analytics.orderbook_snapshot WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )
        counts["analytics.position_snapshot_part"] = _execute_delete(
            session,
            """
            DELETE FROM analytics.position_snapshot_part
            WHERE market_contract_id = ANY(:market_ids)
               OR event_id = ANY(:event_ids)
            """,
            delete_params,
        )
        counts["analytics.position_snapshot"] = _execute_delete(
            session,
            """
            DELETE FROM analytics.position_snapshot
            WHERE market_contract_id = ANY(:market_ids)
               OR event_id = ANY(:event_ids)
            """,
            delete_params,
        )
        counts["analytics.transaction_fact_part"] = _execute_delete(
            session,
            """
            DELETE FROM analytics.transaction_fact_part
            WHERE market_contract_id = ANY(:market_ids)
               OR event_id = ANY(:event_ids)
            """,
            delete_params,
        )
        counts["analytics.transaction_fact"] = _execute_delete(
            session,
            """
            DELETE FROM analytics.transaction_fact
            WHERE market_contract_id = ANY(:market_ids)
               OR event_id = ANY(:event_ids)
            """,
            delete_params,
        )
        counts["analytics.market_contract_history"] = _execute_delete(
            session,
            "DELETE FROM analytics.market_contract_history WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )
        if delete_params["market_slugs"]:
            counts["app.app_watchlist_market"] = _execute_delete(
                session,
                "DELETE FROM app.app_watchlist_market WHERE market_slug = ANY(:market_slugs)",
                delete_params,
            )
        counts["analytics.market_contract"] = _execute_delete(
            session,
            "DELETE FROM analytics.market_contract WHERE market_contract_id = ANY(:market_ids)",
            delete_params,
        )

    if delete_event_ids:
        counts["analytics.position_snapshot_daily"] = counts.get("analytics.position_snapshot_daily", 0) + _execute_delete(
            session,
            "DELETE FROM analytics.position_snapshot_daily WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.position_snapshot_part"] = counts.get("analytics.position_snapshot_part", 0) + _execute_delete(
            session,
            "DELETE FROM analytics.position_snapshot_part WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.position_snapshot"] = counts.get("analytics.position_snapshot", 0) + _execute_delete(
            session,
            "DELETE FROM analytics.position_snapshot WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.transaction_fact_part"] = counts.get("analytics.transaction_fact_part", 0) + _execute_delete(
            session,
            "DELETE FROM analytics.transaction_fact_part WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.transaction_fact"] = counts.get("analytics.transaction_fact", 0) + _execute_delete(
            session,
            "DELETE FROM analytics.transaction_fact WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.market_tag_map_history"] = _execute_delete(
            session,
            "DELETE FROM analytics.market_tag_map_history WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.market_tag_map"] = _execute_delete(
            session,
            "DELETE FROM analytics.market_tag_map WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        counts["analytics.market_event_history"] = _execute_delete(
            session,
            "DELETE FROM analytics.market_event_history WHERE event_id = ANY(:event_ids)",
            delete_params,
        )
        if preserve_current_events:
            counts["analytics.market_event_unlinked"] = _execute_delete(
                session,
                """
                UPDATE analytics.market_event
                   SET raw_payload_id = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE event_id = ANY(:event_ids)
                   AND raw_payload_id IS NOT NULL
                """,
                delete_params,
            )
            counts["analytics.market_event"] = 0
        else:
            counts["analytics.market_event"] = _execute_delete(
                session,
                "DELETE FROM analytics.market_event WHERE event_id = ANY(:event_ids)",
                delete_params,
            )

    counts["analytics.market_tag"] = _execute_delete(
        session,
        """
        DELETE FROM analytics.market_tag t
        WHERE t.platform_id = ANY(:platform_ids)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_tag_map m WHERE m.tag_id = t.tag_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_tag_map_history mh WHERE mh.tag_id = t.tag_id)
        """,
        {"platform_ids": platform_ids},
    )
    counts["raw.api_payload"] = _execute_delete(
        session,
        """
        DELETE FROM raw.api_payload p
        WHERE p.platform_id = ANY(:platform_ids)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_event me WHERE me.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_contract mc WHERE mc.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_event_history meh WHERE meh.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_contract_history mch WHERE mch.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.user_account_history uah WHERE uah.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_tag_map_history mtmh WHERE mtmh.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.transaction_fact tf WHERE tf.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.transaction_fact_part tfp WHERE tfp.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.orderbook_snapshot ob WHERE ob.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.orderbook_snapshot_part obp WHERE obp.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.position_snapshot ps WHERE ps.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.position_snapshot_part psp WHERE psp.raw_payload_id = p.payload_id)
        """,
        {"platform_ids": platform_ids},
    )
    counts["raw.api_payload_part"] = _execute_delete(
        session,
        """
        DELETE FROM raw.api_payload_part p
        WHERE p.platform_id = ANY(:platform_ids)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_event me WHERE me.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_contract mc WHERE mc.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_event_history meh WHERE meh.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_contract_history mch WHERE mch.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.user_account_history uah WHERE uah.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.market_tag_map_history mtmh WHERE mtmh.source_raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.transaction_fact tf WHERE tf.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.transaction_fact_part tfp WHERE tfp.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.orderbook_snapshot ob WHERE ob.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.orderbook_snapshot_part obp WHERE obp.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.position_snapshot ps WHERE ps.raw_payload_id = p.payload_id)
          AND NOT EXISTS (SELECT 1 FROM analytics.position_snapshot_part psp WHERE psp.raw_payload_id = p.payload_id)
        """,
        {"platform_ids": platform_ids},
    )
    return counts


def main() -> int:
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        summary = build_scope_summary(
            session,
            focus_domains=args.focus_domains,
            platforms=args.platforms,
            sample_size=args.sample_size,
        )
        platform_map = _platform_ids(session, args.platforms)
        if args.apply:
            deleted_counts = apply_prune(
                session,
                summary=summary,
                platform_ids=list(platform_map.values()),
                preserve_current_events=args.preserve_current_events,
            )
            summary["deleted_counts"] = deleted_counts
            summary["after_counts"] = {table_name: _table_count(session, table_name) for table_name in CORE_TABLES}
            summary["preserve_current_events"] = args.preserve_current_events
        summary["mode"] = "apply" if args.apply else "dry-run"
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
