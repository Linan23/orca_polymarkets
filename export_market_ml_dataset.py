"""Compatibility entrypoint for exporting the market-level ML dataset."""

from data_platform.db.session import session_scope
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


if __name__ == "__main__":
    with session_scope() as session:
        summary = export_market_snapshot_dataset(session)
    print(summary)
