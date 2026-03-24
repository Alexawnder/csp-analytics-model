import math
import pandas as pd
from sqlalchemy import text


def _chunk_records(records: list[dict], chunk_size: int):
    """Yield successive chunks from a list of records."""
    for i in range(0, len(records), chunk_size):
        yield records[i:i + chunk_size]


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    conflict_columns: list[str],
    chunk_size: int = 500
) -> None:
    """
    Upsert a pandas DataFrame into a PostgreSQL table.

    Args:
        df: DataFrame to load
        table_name: Target PostgreSQL table name
        engine: SQLAlchemy engine
        conflict_columns: Columns that define uniqueness / conflict key
        chunk_size: Number of rows per batch
    """
    if df.empty:
        print(f"Skipping load for {table_name}: dataframe is empty.")
        return

    df = df.copy()

    # Replace pandas NaN / NaT with Python None for SQL compatibility
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    all_columns = list(df.columns)

    if not all_columns:
        print(f"Skipping load for {table_name}: dataframe has no columns.")
        return

    update_columns = [col for col in all_columns if col not in conflict_columns]

    # Quote identifiers to avoid issues with reserved words or weird names
    quoted_columns = [f'"{col}"' for col in all_columns]
    quoted_conflict_columns = [f'"{col}"' for col in conflict_columns]

    insert_cols_sql = ", ".join(quoted_columns)
    value_placeholders = ", ".join([f":{col}" for col in all_columns])
    conflict_cols_sql = ", ".join(quoted_conflict_columns)

    if update_columns:
        update_sql = ", ".join(
            [f'"{col}" = EXCLUDED."{col}"' for col in update_columns]
        )

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

    try:
        total_rows = len(records)
        total_chunks = math.ceil(total_rows / chunk_size)

        with engine.begin() as conn:
            for idx, chunk in enumerate(_chunk_records(records, chunk_size), start=1):
                conn.execute(text(sql), chunk)
                print(
                    f"Upserted chunk {idx}/{total_chunks} "
                    f"({len(chunk)} rows) into {table_name}."
                )

        print(f"Finished upserting {total_rows} rows into {table_name}.")

    except Exception as e:
        print(f"Failed to upsert table '{table_name}'.")
        print(f"Error type: {type(e).__name__}")
        print(str(e))
        raise