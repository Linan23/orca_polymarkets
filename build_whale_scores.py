"""Compatibility entrypoint for building one whale score snapshot."""

from data_platform.db.session import session_scope
from data_platform.services.whale_scoring import build_whale_score_snapshot


if __name__ == "__main__":
    with session_scope() as session:
        summary = build_whale_score_snapshot(session)
    print(summary)
