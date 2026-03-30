import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

_engine = None


def get_engine():
    
    """
    Return the shared SQLAlchemy engine, creating it on first call.

    Uses a module-level singleton so the connection pool is reused across
    all calls within the same process rather than opening a new pool each time.
    pool_pre_ping tests connections before use to handle dropped idle connections
    (common with serverless Postgres like NeonDB). pool_recycle discards
    connections older than 5 minutes for the same reason.
    """

    global _engine
    if _engine is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL is not set.")
        _engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_recycle=300,
        )
    return _engine