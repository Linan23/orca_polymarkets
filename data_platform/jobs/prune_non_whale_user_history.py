"""Prune old non-whale user-linked analytics rows after a three-month window."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.settings import get_settings


CURRENT_WHALES_CTE = """
WITH latest_batch AS (
  SELECT snapshot_time, scoring_version
  FROM analytics.whale_score_snapshot
  ORDER BY snapshot_time DESC, whale_score_snapshot_id DESC
  LIMIT 1
),
current_whales AS (
  SELECT DISTINCT w.user_id
  FROM analytics.whale_score_snapshot w
  JOIN latest_batch b
    ON b.snapshot_time = w.snapshot_time
   AND b.scoring_version = w.scoring_version
  WHERE w.is_whale = TRUE OR w.is_trusted_whale = TRUE
)
"""

REMOVABLE_USERS_CTE = (
    CURRENT_WHALES_CTE
    + """,
removable_users AS (
  SELECT ua.user_id
  FROM analytics.user_account ua
  WHERE ua.platform_id = ANY(:platform_ids)
    AND ua.first_seen_at < :cutoff
    AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ua.user_id)
    AND NOT EXISTS (SELECT 1 FROM app.app_watchlist_user awu WHERE awu.user_id = ua.user_id)
    AND NOT EXISTS (
      SELECT 1 FROM analytics.transaction_fact tf
      WHERE tf.user_id = ua.user_id AND tf.transaction_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.transaction_fact_part tfp
      WHERE tfp.user_id = ua.user_id AND tfp.transaction_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.position_snapshot ps
      WHERE ps.user_id = ua.user_id AND ps.snapshot_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.position_snapshot_part psp
      WHERE psp.user_id = ua.user_id AND psp.snapshot_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.position_snapshot_daily psd
      WHERE psd.user_id = ua.user_id AND psd.bucket_date >= CAST(:cutoff AS date)
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.whale_score_snapshot w
      WHERE w.user_id = ua.user_id AND w.snapshot_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1 FROM analytics.whale_score_snapshot_part wsp
      WHERE wsp.user_id = ua.user_id AND wsp.snapshot_time >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1
      FROM analytics.user_profile up
      JOIN analytics.dashboard d ON d.dashboard_id = up.dashboard_id
      WHERE up.user_id = ua.user_id AND d.generated_at >= :cutoff
    )
    AND NOT EXISTS (
      SELECT 1
      FROM analytics.user_leaderboard ul
      JOIN analytics.dashboard d ON d.dashboard_id = ul.dashboard_id
      WHERE ul.user_id = ua.user_id AND d.generated_at >= :cutoff
    )
)
"""
)


@dataclass(frozen=True)
class PruneStatement:
    table_name: str
    count_sql: str
    delete_sql: str


OLD_ROW_PRUNE_STATEMENTS = [
    PruneStatement(
        "analytics.user_profile",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.user_profile up
        JOIN analytics.dashboard d ON d.dashboard_id = up.dashboard_id
        JOIN analytics.user_account ua ON ua.user_id = up.user_id
        WHERE ua.platform_id = ANY(:platform_ids)
          AND d.generated_at < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = up.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.user_profile up
        USING analytics.dashboard d, analytics.user_account ua
        WHERE d.dashboard_id = up.dashboard_id
          AND ua.user_id = up.user_id
          AND ua.platform_id = ANY(:platform_ids)
          AND d.generated_at < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = up.user_id)
        """,
    ),
    PruneStatement(
        "analytics.user_leaderboard",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.user_leaderboard ul
        JOIN analytics.dashboard d ON d.dashboard_id = ul.dashboard_id
        JOIN analytics.user_account ua ON ua.user_id = ul.user_id
        WHERE ua.platform_id = ANY(:platform_ids)
          AND d.generated_at < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ul.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.user_leaderboard ul
        USING analytics.dashboard d, analytics.user_account ua
        WHERE d.dashboard_id = ul.dashboard_id
          AND ua.user_id = ul.user_id
          AND ua.platform_id = ANY(:platform_ids)
          AND d.generated_at < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ul.user_id)
        """,
    ),
    PruneStatement(
        "analytics.transaction_fact",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.transaction_fact tf
        WHERE tf.platform_id = ANY(:platform_ids)
          AND tf.transaction_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = tf.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.transaction_fact tf
        WHERE tf.platform_id = ANY(:platform_ids)
          AND tf.transaction_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = tf.user_id)
        """,
    ),
    PruneStatement(
        "analytics.transaction_fact_part",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.transaction_fact_part tf
        WHERE tf.platform_id = ANY(:platform_ids)
          AND tf.transaction_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = tf.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.transaction_fact_part tf
        WHERE tf.platform_id = ANY(:platform_ids)
          AND tf.transaction_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = tf.user_id)
        """,
    ),
    PruneStatement(
        "analytics.position_snapshot",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.position_snapshot ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.position_snapshot ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
    ),
    PruneStatement(
        "analytics.position_snapshot_part",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.position_snapshot_part ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.position_snapshot_part ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
    ),
    PruneStatement(
        "analytics.position_snapshot_daily",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.position_snapshot_daily ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.bucket_date < CAST(:cutoff AS date)
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.position_snapshot_daily ps
        WHERE ps.platform_id = ANY(:platform_ids)
          AND ps.bucket_date < CAST(:cutoff AS date)
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = ps.user_id)
        """,
    ),
    PruneStatement(
        "analytics.whale_score_snapshot",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.whale_score_snapshot w
        WHERE w.platform_id = ANY(:platform_ids)
          AND w.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = w.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.whale_score_snapshot w
        WHERE w.platform_id = ANY(:platform_ids)
          AND w.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = w.user_id)
        """,
    ),
    PruneStatement(
        "analytics.whale_score_snapshot_part",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.whale_score_snapshot_part w
        WHERE w.platform_id = ANY(:platform_ids)
          AND w.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = w.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.whale_score_snapshot_part w
        WHERE w.platform_id = ANY(:platform_ids)
          AND w.snapshot_time < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = w.user_id)
        """,
    ),
    PruneStatement(
        "analytics.user_account_history",
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.user_account_history uah
        WHERE uah.platform_id = ANY(:platform_ids)
          AND uah.valid_from < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = uah.user_id)
        """,
        CURRENT_WHALES_CTE
        + """
        DELETE FROM analytics.user_account_history uah
        WHERE uah.platform_id = ANY(:platform_ids)
          AND uah.valid_from < :cutoff
          AND NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = uah.user_id)
        """,
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune user-linked analytics rows older than three months for users who are not current whales.",
    )
    parser.add_argument("--database-url", default=get_settings().database_url)
    parser.add_argument(
        "--platform",
        action="append",
        default=[],
        choices=["polymarket", "kalshi"],
        help="Repeatable platform scope. Defaults to every platform in analytics.platform.",
    )
    parser.add_argument("--sample-size", type=int, default=12)
    parser.add_argument("--apply", action="store_true", help="Actually delete rows. Dry-run by default.")
    return parser.parse_args()


def _scalar_int(session: Session, sql: str, params: dict[str, Any]) -> int:
    return int(session.execute(text(sql), params).scalar_one() or 0)


def _execute_delete(session: Session, sql: str, params: dict[str, Any]) -> int:
    result = session.execute(text(sql), params)
    return int(result.rowcount or 0)


def _platform_ids(session: Session, platform_names: list[str]) -> list[int]:
    if platform_names:
        rows = session.execute(
            text(
                """
                SELECT platform_id
                FROM analytics.platform
                WHERE platform_name = ANY(:platform_names)
                ORDER BY platform_id
                """
            ),
            {"platform_names": platform_names},
        ).scalars().all()
        if len(rows) != len(set(platform_names)):
            found_names = set(
                session.execute(
                    text("SELECT platform_name FROM analytics.platform WHERE platform_name = ANY(:platform_names)"),
                    {"platform_names": platform_names},
                ).scalars().all()
            )
            missing = sorted(set(platform_names) - found_names)
            raise ValueError(f"Unknown platform(s): {', '.join(missing)}")
        return [int(row) for row in rows]
    rows = session.execute(text("SELECT platform_id FROM analytics.platform ORDER BY platform_id")).scalars().all()
    return [int(row) for row in rows]


def _cutoff_timestamp(session: Session) -> Any:
    return session.execute(text("SELECT CURRENT_TIMESTAMP - INTERVAL '3 months' AS cutoff")).scalar_one()


def _latest_batch_summary(session: Session) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT snapshot_time, scoring_version
            FROM analytics.whale_score_snapshot
            ORDER BY snapshot_time DESC, whale_score_snapshot_id DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    if row is None:
        raise RuntimeError("Cannot prune non-whale history before a whale score snapshot exists.")
    return {
        "snapshot_time": row["snapshot_time"].isoformat() if row["snapshot_time"] else None,
        "scoring_version": row["scoring_version"],
    }


def _current_whale_count(session: Session, params: dict[str, Any]) -> int:
    return _scalar_int(
        session,
        CURRENT_WHALES_CTE
        + """
        SELECT count(*) AS row_count
        FROM current_whales cw
        JOIN analytics.user_account ua ON ua.user_id = cw.user_id
        WHERE ua.platform_id = ANY(:platform_ids)
        """,
        params,
    )


def _candidate_non_whale_count(session: Session, params: dict[str, Any]) -> int:
    return _scalar_int(
        session,
        CURRENT_WHALES_CTE
        + """
        SELECT count(DISTINCT old_users.user_id) AS row_count
        FROM (
          SELECT tf.user_id
          FROM analytics.transaction_fact tf
          WHERE tf.platform_id = ANY(:platform_ids) AND tf.transaction_time < :cutoff
          UNION
          SELECT tfp.user_id
          FROM analytics.transaction_fact_part tfp
          WHERE tfp.platform_id = ANY(:platform_ids) AND tfp.transaction_time < :cutoff
          UNION
          SELECT ps.user_id
          FROM analytics.position_snapshot ps
          WHERE ps.platform_id = ANY(:platform_ids) AND ps.snapshot_time < :cutoff
          UNION
          SELECT psp.user_id
          FROM analytics.position_snapshot_part psp
          WHERE psp.platform_id = ANY(:platform_ids) AND psp.snapshot_time < :cutoff
          UNION
          SELECT psd.user_id
          FROM analytics.position_snapshot_daily psd
          WHERE psd.platform_id = ANY(:platform_ids) AND psd.bucket_date < CAST(:cutoff AS date)
          UNION
          SELECT w.user_id
          FROM analytics.whale_score_snapshot w
          WHERE w.platform_id = ANY(:platform_ids) AND w.snapshot_time < :cutoff
          UNION
          SELECT wsp.user_id
          FROM analytics.whale_score_snapshot_part wsp
          WHERE wsp.platform_id = ANY(:platform_ids) AND wsp.snapshot_time < :cutoff
          UNION
          SELECT up.user_id
          FROM analytics.user_profile up
          JOIN analytics.dashboard d ON d.dashboard_id = up.dashboard_id
          JOIN analytics.user_account ua ON ua.user_id = up.user_id
          WHERE ua.platform_id = ANY(:platform_ids) AND d.generated_at < :cutoff
          UNION
          SELECT ul.user_id
          FROM analytics.user_leaderboard ul
          JOIN analytics.dashboard d ON d.dashboard_id = ul.dashboard_id
          JOIN analytics.user_account ua ON ua.user_id = ul.user_id
          WHERE ua.platform_id = ANY(:platform_ids) AND d.generated_at < :cutoff
        ) old_users
        WHERE NOT EXISTS (SELECT 1 FROM current_whales cw WHERE cw.user_id = old_users.user_id)
        """,
        params,
    )


def _old_row_counts(session: Session, params: dict[str, Any]) -> dict[str, int]:
    return {statement.table_name: _scalar_int(session, statement.count_sql, params) for statement in OLD_ROW_PRUNE_STATEMENTS}


def _orphan_user_count(session: Session, params: dict[str, Any]) -> int:
    return _scalar_int(
        session,
        REMOVABLE_USERS_CTE + "SELECT count(*) AS row_count FROM removable_users",
        params,
    )


def _orphan_history_count(session: Session, params: dict[str, Any]) -> int:
    return _scalar_int(
        session,
        REMOVABLE_USERS_CTE
        + """
        SELECT count(*) AS row_count
        FROM analytics.user_account_history uah
        JOIN removable_users ru ON ru.user_id = uah.user_id
        WHERE uah.valid_from >= :cutoff
        """,
        params,
    )


def _sample_users(session: Session, params: dict[str, Any], sample_size: int) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            REMOVABLE_USERS_CTE
            + """
            SELECT
              ua.user_id,
              p.platform_name,
              ua.external_user_ref,
              ua.wallet_address,
              ua.display_label,
              ua.first_seen_at,
              ua.last_seen_at
            FROM analytics.user_account ua
            JOIN removable_users ru ON ru.user_id = ua.user_id
            JOIN analytics.platform p ON p.platform_id = ua.platform_id
            ORDER BY ua.first_seen_at ASC, ua.user_id ASC
            LIMIT :sample_size
            """
        ),
        {**params, "sample_size": sample_size},
    ).mappings().all()
    return [
        {
            "user_id": int(row["user_id"]),
            "platform_name": row["platform_name"],
            "external_user_ref": row["external_user_ref"],
            "wallet_address": row["wallet_address"],
            "display_label": row["display_label"],
            "first_seen_at": row["first_seen_at"].isoformat() if row["first_seen_at"] else None,
            "last_seen_at": row["last_seen_at"].isoformat() if row["last_seen_at"] else None,
        }
        for row in rows
    ]


def build_prune_summary(session: Session, *, platform_ids: list[int], sample_size: int) -> dict[str, Any]:
    cutoff = _cutoff_timestamp(session)
    params = {"platform_ids": platform_ids, "cutoff": cutoff}
    old_row_counts = _old_row_counts(session, params)
    orphan_history_count = _orphan_history_count(session, params)
    orphan_user_count = _orphan_user_count(session, params)
    estimated_counts = dict(old_row_counts)
    estimated_counts["analytics.user_account_history"] += orphan_history_count
    estimated_counts["analytics.user_account"] = orphan_user_count
    return {
        "cutoff_timestamp": cutoff.isoformat() if cutoff else None,
        "latest_whale_score_batch": _latest_batch_summary(session),
        "current_whale_count": _current_whale_count(session, params),
        "candidate_non_whale_count": _candidate_non_whale_count(session, params),
        "estimated_deleted_counts": estimated_counts,
        "sample_removed_users": _sample_users(session, params, sample_size),
    }


def apply_prune(session: Session, *, platform_ids: list[int]) -> dict[str, int]:
    cutoff = _cutoff_timestamp(session)
    params = {"platform_ids": platform_ids, "cutoff": cutoff}
    deleted_counts: dict[str, int] = {}
    for statement in OLD_ROW_PRUNE_STATEMENTS:
        deleted_counts[statement.table_name] = _execute_delete(session, statement.delete_sql, params)
    deleted_counts["analytics.user_account_history"] += _execute_delete(
        session,
        REMOVABLE_USERS_CTE
        + """
        DELETE FROM analytics.user_account_history uah
        USING removable_users ru
        WHERE ru.user_id = uah.user_id
        """,
        params,
    )
    deleted_counts["analytics.user_account"] = _execute_delete(
        session,
        REMOVABLE_USERS_CTE
        + """
        DELETE FROM analytics.user_account ua
        USING removable_users ru
        WHERE ru.user_id = ua.user_id
        """,
        params,
    )
    return deleted_counts


def main() -> int:
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        platform_ids = _platform_ids(session, args.platform)
        summary = build_prune_summary(session, platform_ids=platform_ids, sample_size=args.sample_size)
        summary["mode"] = "apply" if args.apply else "dry-run"
        summary["platform_ids"] = platform_ids
        summary["raw_payloads_pruned"] = False
        if args.apply:
            summary["deleted_counts"] = apply_prune(session, platform_ids=platform_ids)
            summary["after_estimated_deleted_counts"] = build_prune_summary(
                session,
                platform_ids=platform_ids,
                sample_size=args.sample_size,
            )["estimated_deleted_counts"]
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
