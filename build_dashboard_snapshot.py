"""Compatibility entrypoint for building one derived dashboard snapshot.

The dashboard builder implementation lives under ``data_platform.services``.
This wrapper keeps the existing command stable for collaborators.
"""

from data_platform.db.session import session_scope
from data_platform.services.dashboard_builder import build_dashboard_snapshot


if __name__ == "__main__":
    with session_scope() as session:
        summary = build_dashboard_snapshot(session)
    print(summary)
