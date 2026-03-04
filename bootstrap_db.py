"""Compatibility entrypoint for database schema bootstrap.

The database bootstrap implementation lives under ``data_platform.db``. This
wrapper keeps the existing command stable for collaborators.
"""

from data_platform.db.bootstrap import create_database_objects


if __name__ == "__main__":
    create_database_objects()
    print("Database schemas and tables created.")
