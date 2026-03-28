"""
PostgreSQL loader utilities: upsert, insert, truncate, and query helpers.
"""

import math
import pandas as pd
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _chunk_records(records: list[dict], chunk_size: int):
    """Yield successive fixed-size chunks from a list of records."""
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Replace pandas NaN/NaT with None for SQL compatibility."""
    return df.where(pd.notnull(df), None)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    conflict_columns: list[str],
    chunk_size: int = 500,
) -> None:
    """
    Upsert a DataFrame into a PostgreSQL table using ON CONFLICT DO UPDATE.

    Args:
        df:               DataFrame to load.
        table_name:       Target PostgreSQL table name.
        engine:           SQLAlchemy engine.
        conflict_columns: Columns that define the unique / conflict key.
        chunk_size:       Rows per batch commit.
    """
    if df.empty:
        print(f"[upsert] Skipping {table_name}: DataFrame is empty.")
        return

    df = _clean_df(df.copy())
    records = df.to_dict(orient="records")
    all_columns = list(df.columns)

    if not all_columns:
        print(f"[upsert] Skipping {table_name}: DataFrame has no columns.")
        return

    update_columns = [c for c in all_columns if c not in conflict_columns]

    quoted_columns       = [f'"{c}"' for c in all_columns]
    quoted_conflict_cols = [f'"{c}"' for c in conflict_columns]
    insert_cols_sql      = ", ".join(quoted_columns)
    value_placeholders   = ", ".join([f":{c}" for c in all_columns])
    conflict_cols_sql    = ", ".join(quoted_conflict_cols)

    if update_columns:
        update_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_columns])
        sql = f"""
            INSERT INTO "{table_name}" ({insert_cols_sql})
            VALUES ({value_placeholders})
            ON CONFLICT ({conflict_cols_sql})
            DO UPDATE SET {update_sql};
        """
    else:
        sql = f"""
            INSERT INTO "{table_name}" ({insert_cols_sql})
            VALUES ({value_placeholders})
            ON CONFLICT ({conflict_cols_sql})
            DO NOTHING;
        """

    total_rows   = len(records)
    total_chunks = math.ceil(total_rows / chunk_size)

    try:
        with engine.begin() as conn:
            for idx, chunk in enumerate(_chunk_records(records, chunk_size), start=1):
                conn.execute(text(sql), chunk)
                print(f"[upsert] {table_name}: chunk {idx}/{total_chunks} ({len(chunk)} rows)")
        print(f"[upsert] {table_name}: finished {total_rows} rows.")
    except Exception as e:
        print(f"[upsert] Failed on '{table_name}': {type(e).__name__}: {e}")
        raise


def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    chunk_size: int = 500,
) -> None:
    """Plain INSERT (no conflict handling). Intended for full-refresh loads."""
    if df.empty:
        print(f"[insert] Skipping {table_name}: DataFrame is empty.")
        return

    df = _clean_df(df.copy())
    records     = df.to_dict(orient="records")
    all_columns = list(df.columns)

    if not all_columns:
        print(f"[insert] Skipping {table_name}: DataFrame has no columns.")
        return

    quoted_columns     = [f'"{c}"' for c in all_columns]
    insert_cols_sql    = ", ".join(quoted_columns)
    value_placeholders = ", ".join([f":{c}" for c in all_columns])

    sql = f"""
        INSERT INTO "{table_name}" ({insert_cols_sql})
        VALUES ({value_placeholders});
    """

    total_rows   = len(records)
    total_chunks = math.ceil(total_rows / chunk_size)

    try:
        with engine.begin() as conn:
            for idx, chunk in enumerate(_chunk_records(records, chunk_size), start=1):
                conn.execute(text(sql), chunk)
                print(f"[insert] {table_name}: chunk {idx}/{total_chunks} ({len(chunk)} rows)")
        print(f"[insert] {table_name}: finished {total_rows} rows.")
    except Exception as e:
        print(f"[insert] Failed on '{table_name}': {type(e).__name__}: {e}")
        raise


def truncate_tables(
    engine,
    table_names: list[str],
    restart_identity: bool = True,
    cascade: bool = True,
) -> None:
    """Truncate one or more PostgreSQL tables."""
    if not table_names:
        print("[truncate] No tables provided.")
        return

    quoted_tables = ", ".join([f'"{t}"' for t in table_names])
    sql = f"TRUNCATE TABLE {quoted_tables}"
    if restart_identity:
        sql += " RESTART IDENTITY"
    if cascade:
        sql += " CASCADE"
    sql += ";"

    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"[truncate] Truncated: {', '.join(table_names)}")
    except Exception as e:
        print(f"[truncate] Failed: {type(e).__name__}: {e}")
        raise


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_latest_price_dates(engine, table_name: str) -> pd.DataFrame:
    """Return the latest stored date per ticker from a price table."""
    sql = f"""
        SELECT ticker, MAX(date) AS last_date
        FROM "{table_name}"
        GROUP BY ticker
        ORDER BY ticker;
    """
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(sql), conn)
        if not df.empty:
            df["last_date"] = pd.to_datetime(df["last_date"])
        return df
    except Exception as e:
        print(f"[query] Failed to fetch latest price dates from '{table_name}': {e}")
        raise


def get_price_history_for_tickers(
    engine,
    tickers: list[str],
    lookback_days: int = 90,
    table_name: str = "stock_prices",
) -> pd.DataFrame:
    """
    Fetch recent price history for a list of tickers from PostgreSQL.
    Uses parameterised ANY(:tickers) to avoid SQL injection.
    """
    if not tickers:
        return pd.DataFrame()

    sql = f"""
        SELECT date, open, high, low, close, volume, ticker
        FROM "{table_name}"
        WHERE ticker = ANY(:tickers)
          AND date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        ORDER BY ticker, date;
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(sql), conn, params={"tickers": list(tickers)})
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        print(f"[query] Failed to fetch price history from '{table_name}': {e}")
        raise