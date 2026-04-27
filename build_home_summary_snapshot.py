"""Build one cached homepage summary snapshot."""

from data_platform.db.session import session_scope
from data_platform.services.home_summary_snapshot import build_home_summary_snapshot


if __name__ == "__main__":
    with session_scope() as session:
        summary = build_home_summary_snapshot(session)
    print(summary)
