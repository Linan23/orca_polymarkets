"""Storage lifecycle helpers for history, rollups, and partition-shadow migration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class PartitionConfig:
    schema: str
    legacy_table: str
    shadow_table: str
    time_column: str
    pk_column: str
    columns: tuple[str, ...]


PARTITION_CONFIGS: tuple[PartitionConfig, ...] = (
    PartitionConfig(
        schema="analytics",
        legacy_table="scrape_run",
        shadow_table="scrape_run_part",
        time_column="started_at",
        pk_column="scrape_run_id",
        columns=(
            "scrape_run_id",
            "platform_id",
            "job_name",
            "endpoint_name",
            "request_url",
            "window_started_at",
            "started_at",
            "finished_at",
            "status",
            "records_written",
            "error_count",
            "error_summary",
            "raw_output_path",
            "created_at",
        ),
    ),
    PartitionConfig(
        schema="raw",
        legacy_table="api_payload",
        shadow_table="api_payload_part",
        time_column="collected_at",
        pk_column="payload_id",
        columns=(
            "payload_id",
            "scrape_run_id",
            "platform_id",
            "entity_type",
            "entity_external_id",
            "collected_at",
            "payload",
            "payload_hash",
            "created_at",
        ),
    ),
    PartitionConfig(
        schema="analytics",
        legacy_table="transaction_fact",
        shadow_table="transaction_fact_part",
        time_column="transaction_time",
        pk_column="transaction_id",
        columns=(
            "transaction_id",
            "user_id",
            "market_contract_id",
            "event_id",
            "platform_id",
            "source_transaction_id",
            "source_fill_id",
            "source_order_id",
            "transaction_type",
            "side",
            "outcome_label",
            "price",
            "shares",
            "notional_value",
            "fee_amount",
            "profit_loss_realized",
            "transaction_time",
            "sequence_ts",
            "raw_payload_id",
            "created_at",
        ),
    ),
    PartitionConfig(
        schema="analytics",
        legacy_table="orderbook_snapshot",
        shadow_table="orderbook_snapshot_part",
        time_column="snapshot_time",
        pk_column="orderbook_snapshot_id",
        columns=(
            "orderbook_snapshot_id",
            "market_contract_id",
            "platform_id",
            "snapshot_time",
            "depth_levels",
            "best_bid",
            "best_ask",
            "mid_price",
            "spread",
            "bid_depth_notional",
            "ask_depth_notional",
            "raw_payload_id",
            "created_at",
        ),
    ),
    PartitionConfig(
        schema="analytics",
        legacy_table="position_snapshot",
        shadow_table="position_snapshot_part",
        time_column="snapshot_time",
        pk_column="position_snapshot_id",
        columns=(
            "position_snapshot_id",
            "user_id",
            "market_contract_id",
            "event_id",
            "platform_id",
            "snapshot_time",
            "position_size",
            "avg_entry_price",
            "current_mark_price",
            "market_value",
            "cash_pnl",
            "realized_pnl",
            "unrealized_pnl",
            "is_redeemable",
            "is_mergeable",
            "raw_payload_id",
            "created_at",
        ),
    ),
    PartitionConfig(
        schema="analytics",
        legacy_table="whale_score_snapshot",
        shadow_table="whale_score_snapshot_part",
        time_column="snapshot_time",
        pk_column="whale_score_snapshot_id",
        columns=(
            "whale_score_snapshot_id",
            "user_id",
            "platform_id",
            "snapshot_time",
            "raw_volume_score",
            "consistency_score",
            "profitability_score",
            "trust_score",
            "insider_penalty",
            "is_whale",
            "is_trusted_whale",
            "sample_trade_count",
            "scoring_version",
            "created_at",
        ),
    ),
)

RAW_PAYLOAD_UNREFERENCED_PREDICATE = '''
    NOT EXISTS (SELECT 1 FROM analytics.market_event me WHERE me.raw_payload_id = p.payload_id)
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
'''


def month_floor(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def add_month(value: datetime) -> datetime:
    base = month_floor(value)
    if base.month == 12:
        return base.replace(year=base.year + 1, month=1)
    return base.replace(month=base.month + 1)


def iter_months(start: datetime, end: datetime) -> list[datetime]:
    months: list[datetime] = []
    current = month_floor(start)
    end_floor = month_floor(end)
    while current <= end_floor:
        months.append(current)
        current = add_month(current)
    return months


def create_month_partition(session: Session, *, schema: str, table_name: str, month_start: datetime) -> str:
    month_start = month_floor(month_start)
    month_end = add_month(month_start)
    partition_name = f"{table_name}_{month_start.strftime('%Y%m')}"
    session.execute(
        text(
            f'''
            CREATE TABLE IF NOT EXISTS {schema}."{partition_name}"
            PARTITION OF {schema}."{table_name}"
            FOR VALUES FROM ('{month_start.isoformat()}') TO ('{month_end.isoformat()}')
            '''
        )
    )
    session.flush()
    return partition_name


def ensure_partition_range(
    session: Session,
    *,
    schema: str,
    table_name: str,
    start: datetime,
    end: datetime,
) -> list[str]:
    created: list[str] = []
    for month_start in iter_months(start, end):
        created.append(create_month_partition(session, schema=schema, table_name=table_name, month_start=month_start))
    return created


def ensure_default_partitions(session: Session, *, months_ahead: int = 1) -> dict[str, list[str]]:
    now = datetime.now(timezone.utc)
    created: dict[str, list[str]] = {}
    for config in PARTITION_CONFIGS:
        entries: list[str] = []
        current = month_floor(now)
        for _ in range(months_ahead + 1):
            entries.append(create_month_partition(session, schema=config.schema, table_name=config.shadow_table, month_start=current))
            current = add_month(current)
        created[f"{config.schema}.{config.shadow_table}"] = entries
    return created


def ensure_backfill_partitions(session: Session, config: PartitionConfig) -> list[str]:
    bounds = session.execute(
        text(
            f'''
            SELECT min(l.{config.time_column}) AS min_ts, max(l.{config.time_column}) AS max_ts
            FROM {config.schema}."{config.legacy_table}" AS l
            WHERE NOT EXISTS (
                SELECT 1
                FROM {config.schema}."{config.shadow_table}" AS p
                WHERE p.{config.pk_column} = l.{config.pk_column}
            )
            '''
        )
    ).first()
    if bounds is None or bounds.min_ts is None or bounds.max_ts is None:
        return []
    return ensure_partition_range(
        session,
        schema=config.schema,
        table_name=config.shadow_table,
        start=bounds.min_ts,
        end=bounds.max_ts,
    )


def backfill_partition_shadow(
    session: Session,
    config: PartitionConfig,
    *,
    batch_size: int = 5000,
) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    ensure_backfill_partitions(session, config)
    columns = ", ".join(config.columns)
    insert_sql = text(
        f'''
        WITH candidates AS (
            SELECT {columns}
            FROM {config.schema}."{config.legacy_table}" AS l
            WHERE NOT EXISTS (
                SELECT 1
                FROM {config.schema}."{config.shadow_table}" AS p
                WHERE p.{config.pk_column} = l.{config.pk_column}
            )
            ORDER BY l.{config.pk_column}
            LIMIT :batch_size
        )
        INSERT INTO {config.schema}."{config.shadow_table}" ({columns})
        SELECT {columns}
        FROM candidates
        ON CONFLICT DO NOTHING
        '''
    )
    result = session.execute(insert_sql, {"batch_size": batch_size})
    session.flush()
    return int(result.rowcount or 0)


def backfill_all_partition_shadows(session: Session, *, batch_size: int = 5000) -> dict[str, int]:
    totals: dict[str, int] = {}
    for config in PARTITION_CONFIGS:
        total = 0
        while True:
            written = backfill_partition_shadow(session, config, batch_size=batch_size)
            total += written
            if written == 0:
                break
        totals[f"{config.schema}.{config.shadow_table}"] = total
    return totals


def rollup_old_orderbook_snapshots(session: Session, *, older_than_days: int = 30) -> dict[str, int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    session.execute(
        text(
            '''
            INSERT INTO analytics.orderbook_snapshot_hourly (
                market_contract_id,
                platform_id,
                bucket_start,
                first_snapshot_time,
                last_snapshot_time,
                sample_count,
                max_depth_levels,
                avg_best_bid,
                avg_best_ask,
                avg_mid_price,
                avg_spread,
                avg_bid_depth_notional,
                avg_ask_depth_notional
            )
            SELECT
                market_contract_id,
                platform_id,
                date_trunc('hour', snapshot_time) AS bucket_start,
                min(snapshot_time) AS first_snapshot_time,
                max(snapshot_time) AS last_snapshot_time,
                count(*) AS sample_count,
                max(depth_levels) AS max_depth_levels,
                avg(best_bid) AS avg_best_bid,
                avg(best_ask) AS avg_best_ask,
                avg(mid_price) AS avg_mid_price,
                avg(spread) AS avg_spread,
                avg(bid_depth_notional) AS avg_bid_depth_notional,
                avg(ask_depth_notional) AS avg_ask_depth_notional
            FROM analytics.orderbook_snapshot
            WHERE snapshot_time < :cutoff
            GROUP BY market_contract_id, platform_id, date_trunc('hour', snapshot_time)
            ON CONFLICT (market_contract_id, platform_id, bucket_start)
            DO UPDATE SET
                first_snapshot_time = EXCLUDED.first_snapshot_time,
                last_snapshot_time = EXCLUDED.last_snapshot_time,
                sample_count = EXCLUDED.sample_count,
                max_depth_levels = EXCLUDED.max_depth_levels,
                avg_best_bid = EXCLUDED.avg_best_bid,
                avg_best_ask = EXCLUDED.avg_best_ask,
                avg_mid_price = EXCLUDED.avg_mid_price,
                avg_spread = EXCLUDED.avg_spread,
                avg_bid_depth_notional = EXCLUDED.avg_bid_depth_notional,
                avg_ask_depth_notional = EXCLUDED.avg_ask_depth_notional
            '''
        ),
        {"cutoff": cutoff},
    )
    session.execute(
        text(
            '''
            INSERT INTO analytics.orderbook_snapshot_daily (
                market_contract_id,
                platform_id,
                bucket_date,
                first_snapshot_time,
                last_snapshot_time,
                sample_count,
                max_depth_levels,
                avg_best_bid,
                avg_best_ask,
                avg_mid_price,
                avg_spread,
                avg_bid_depth_notional,
                avg_ask_depth_notional
            )
            SELECT
                market_contract_id,
                platform_id,
                date(snapshot_time) AS bucket_date,
                min(snapshot_time) AS first_snapshot_time,
                max(snapshot_time) AS last_snapshot_time,
                count(*) AS sample_count,
                max(depth_levels) AS max_depth_levels,
                avg(best_bid) AS avg_best_bid,
                avg(best_ask) AS avg_best_ask,
                avg(mid_price) AS avg_mid_price,
                avg(spread) AS avg_spread,
                avg(bid_depth_notional) AS avg_bid_depth_notional,
                avg(ask_depth_notional) AS avg_ask_depth_notional
            FROM analytics.orderbook_snapshot
            WHERE snapshot_time < :cutoff
            GROUP BY market_contract_id, platform_id, date(snapshot_time)
            ON CONFLICT (market_contract_id, platform_id, bucket_date)
            DO UPDATE SET
                first_snapshot_time = EXCLUDED.first_snapshot_time,
                last_snapshot_time = EXCLUDED.last_snapshot_time,
                sample_count = EXCLUDED.sample_count,
                max_depth_levels = EXCLUDED.max_depth_levels,
                avg_best_bid = EXCLUDED.avg_best_bid,
                avg_best_ask = EXCLUDED.avg_best_ask,
                avg_mid_price = EXCLUDED.avg_mid_price,
                avg_spread = EXCLUDED.avg_spread,
                avg_bid_depth_notional = EXCLUDED.avg_bid_depth_notional,
                avg_ask_depth_notional = EXCLUDED.avg_ask_depth_notional
            '''
        ),
        {"cutoff": cutoff},
    )
    counts = session.execute(
        text(
            '''
            SELECT
                (SELECT count(*) FROM analytics.orderbook_snapshot_hourly) AS hourly_count,
                (SELECT count(*) FROM analytics.orderbook_snapshot_daily) AS daily_count
            '''
        )
    ).first()
    session.flush()
    return {
        "cutoff_days": older_than_days,
        "hourly_rows": int(counts.hourly_count or 0),
        "daily_rows": int(counts.daily_count or 0),
    }


def rollup_old_position_snapshots(session: Session, *, older_than_days: int = 30) -> dict[str, int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    session.execute(
        text(
            '''
            INSERT INTO analytics.position_snapshot_daily (
                user_id,
                market_contract_id,
                event_id,
                platform_id,
                bucket_date,
                first_snapshot_time,
                last_snapshot_time,
                sample_count,
                avg_position_size,
                avg_entry_price,
                avg_mark_price,
                avg_market_value,
                avg_realized_pnl,
                avg_unrealized_pnl
            )
            SELECT
                user_id,
                market_contract_id,
                event_id,
                platform_id,
                date(snapshot_time) AS bucket_date,
                min(snapshot_time) AS first_snapshot_time,
                max(snapshot_time) AS last_snapshot_time,
                count(*) AS sample_count,
                avg(position_size) AS avg_position_size,
                avg(avg_entry_price) AS avg_entry_price,
                avg(current_mark_price) AS avg_mark_price,
                avg(market_value) AS avg_market_value,
                avg(realized_pnl) AS avg_realized_pnl,
                avg(unrealized_pnl) AS avg_unrealized_pnl
            FROM analytics.position_snapshot
            WHERE snapshot_time < :cutoff
            GROUP BY user_id, market_contract_id, event_id, platform_id, date(snapshot_time)
            ON CONFLICT (user_id, market_contract_id, platform_id, bucket_date)
            DO UPDATE SET
                first_snapshot_time = EXCLUDED.first_snapshot_time,
                last_snapshot_time = EXCLUDED.last_snapshot_time,
                sample_count = EXCLUDED.sample_count,
                avg_position_size = EXCLUDED.avg_position_size,
                avg_entry_price = EXCLUDED.avg_entry_price,
                avg_mark_price = EXCLUDED.avg_mark_price,
                avg_market_value = EXCLUDED.avg_market_value,
                avg_realized_pnl = EXCLUDED.avg_realized_pnl,
                avg_unrealized_pnl = EXCLUDED.avg_unrealized_pnl
            '''
        ),
        {"cutoff": cutoff},
    )
    count_row = session.execute(text("SELECT count(*) AS row_count FROM analytics.position_snapshot_daily")).first()
    session.flush()
    return {
        "cutoff_days": older_than_days,
        "daily_rows": int(count_row.row_count or 0),
    }


def partition_coverage(session: Session) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for config in PARTITION_CONFIGS:
        row = session.execute(
            text(
                '''
                SELECT count(*) AS row_count, min(partition_value) AS first_partition, max(partition_value) AS last_partition
                FROM (
                    SELECT to_date(substring(c.relname FROM '.+_(\\d{6})$'), 'YYYYMM') AS partition_value
                    FROM pg_inherits
                    JOIN pg_class c ON c.oid = pg_inherits.inhrelid
                    JOIN pg_class p ON p.oid = pg_inherits.inhparent
                    JOIN pg_namespace n ON n.oid = p.relnamespace
                    WHERE n.nspname = :schema_name AND p.relname = :table_name
                ) partitions
                '''
            ),
            {"schema_name": config.schema, "table_name": config.shadow_table},
        ).first()
        result[f"{config.schema}.{config.shadow_table}"] = {
            "partition_count": int(row.row_count or 0),
            "first_partition": row.first_partition.isoformat() if row.first_partition else None,
            "last_partition": row.last_partition.isoformat() if row.last_partition else None,
        }
    return result


def snapshot_window_bounds(session: Session) -> dict[str, Any]:
    row = session.execute(
        text(
            '''
            SELECT
                min(ts) AS min_timestamp,
                max(ts) AS max_timestamp
            FROM (
                SELECT collected_at AS ts FROM raw.api_payload
                UNION ALL SELECT started_at AS ts FROM analytics.scrape_run
                UNION ALL SELECT transaction_time AS ts FROM analytics.transaction_fact
                UNION ALL SELECT snapshot_time AS ts FROM analytics.orderbook_snapshot
                UNION ALL SELECT snapshot_time AS ts FROM analytics.position_snapshot
                UNION ALL SELECT snapshot_time AS ts FROM analytics.whale_score_snapshot
            ) all_times
            '''
        )
    ).first()
    return {
        "min_timestamp": row.min_timestamp.isoformat() if row and row.min_timestamp else None,
        "max_timestamp": row.max_timestamp.isoformat() if row and row.max_timestamp else None,
    }


def cleanup_orphan_market_events(
    session: Session,
    *,
    batch_size: int = 1000,
    max_batches: int = 10,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if max_batches < 0:
        raise ValueError("max_batches must be >= 0")

    totals = {
        "deleted_event_ids": 0,
        "analytics.market_tag_map_history": 0,
        "analytics.market_tag_map": 0,
        "analytics.market_event_history": 0,
        "analytics.market_event": 0,
        "analytics.market_tag": 0,
        "batches": 0,
    }

    remaining_batches = max_batches
    while remaining_batches != 0:
        event_ids = [
            int(event_id)
            for event_id in session.execute(
                text(
                    '''
                    SELECT me.event_id
                    FROM analytics.market_event me
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM analytics.market_contract mc
                        WHERE mc.event_id = me.event_id
                    )
                    ORDER BY me.event_id
                    LIMIT :batch_size
                    '''
                ),
                {"batch_size": batch_size},
            ).scalars()
        ]
        if not event_ids:
            break

        params = {"event_ids": event_ids}
        totals["deleted_event_ids"] += len(event_ids)
        totals["analytics.market_tag_map_history"] += int(
            session.execute(
                text("DELETE FROM analytics.market_tag_map_history WHERE event_id = ANY(:event_ids)"),
                params,
            ).rowcount
            or 0
        )
        totals["analytics.market_tag_map"] += int(
            session.execute(
                text("DELETE FROM analytics.market_tag_map WHERE event_id = ANY(:event_ids)"),
                params,
            ).rowcount
            or 0
        )
        totals["analytics.market_event_history"] += int(
            session.execute(
                text("DELETE FROM analytics.market_event_history WHERE event_id = ANY(:event_ids)"),
                params,
            ).rowcount
            or 0
        )
        totals["analytics.market_event"] += int(
            session.execute(
                text("DELETE FROM analytics.market_event WHERE event_id = ANY(:event_ids)"),
                params,
            ).rowcount
            or 0
        )
        totals["batches"] += 1
        session.flush()
        if remaining_batches > 0:
            remaining_batches -= 1

    totals["analytics.market_tag"] = int(
        session.execute(
            text(
                '''
                DELETE FROM analytics.market_tag t
                WHERE NOT EXISTS (SELECT 1 FROM analytics.market_tag_map m WHERE m.tag_id = t.tag_id)
                  AND NOT EXISTS (SELECT 1 FROM analytics.market_tag_map_history mh WHERE mh.tag_id = t.tag_id)
                '''
            )
        ).rowcount
        or 0
    )
    session.flush()
    return totals


def garbage_collect_unreferenced_raw_payloads(
    session: Session,
    *,
    batch_size: int = 1000,
    max_batches: int = 10,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if max_batches < 0:
        raise ValueError("max_batches must be >= 0")

    totals = {
        "deleted_payload_rows": 0,
        "raw.api_payload": 0,
        "raw.api_payload_part": 0,
        "batches": 0,
    }

    remaining_batches = max_batches
    while remaining_batches != 0:
        batch_deleted = 0
        for qualified_name in ("raw.api_payload", "raw.api_payload_part"):
            payload_ids = [
                int(payload_id)
                for payload_id in session.execute(
                    text(
                        f'''
                        SELECT p.payload_id
                        FROM {qualified_name} p
                        WHERE {RAW_PAYLOAD_UNREFERENCED_PREDICATE}
                        ORDER BY p.payload_id
                        LIMIT :batch_size
                        '''
                    ),
                    {"batch_size": batch_size},
                ).scalars()
            ]
            if not payload_ids:
                continue
            deleted = int(
                session.execute(
                    text(f"DELETE FROM {qualified_name} WHERE payload_id = ANY(:payload_ids)"),
                    {"payload_ids": payload_ids},
                ).rowcount
                or 0
            )
            totals[qualified_name] += deleted
            batch_deleted += deleted
        if batch_deleted == 0:
            break
        totals["deleted_payload_rows"] += batch_deleted
        totals["batches"] += 1
        session.flush()
        if remaining_batches > 0:
            remaining_batches -= 1

    return totals
