"""Compatibility entrypoint for exporting the first ML dataset."""

from data_platform.db.session import session_scope
from data_platform.ml.dataset_builder import export_resolved_user_market_dataset


if __name__ == "__main__":
    with session_scope() as session:
        summary = export_resolved_user_market_dataset(session)
    print(summary)
