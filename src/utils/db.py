import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_engine():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is not set in environment.")

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )