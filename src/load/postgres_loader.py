import pandas as pd


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    if_exists: str = "append",
    chunksize: int = 100
) -> None:
    if df.empty:
        print(f"Skipping load for {table_name}: dataframe is empty.")
        return

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize
        )
        print(f"Loaded {len(df)} rows into {table_name}.")

    except Exception as e:
        print(f"Failed to load table '{table_name}'.")
        print(f"Error type: {type(e).__name__}")
        print(str(e))
        raise