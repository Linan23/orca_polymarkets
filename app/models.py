import sqlalchemy as sa

DB_URL = "postgresql://app:password@localhost:5433/app_db"

engine = sa.create_engine(DB_URL)
metadata = sa.MetaData()

