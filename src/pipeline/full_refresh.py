from utils.db import get_engine
from utils.config import (
    PRICE_HISTORY_PERIOD,
    PRICE_INTERVAL,
    UPSERT_CHUNK_SIZE,
    DB_TABLE_STOCK_PRICES,
    DB_TABLE_STOCK_FEATURES,
    DB_TABLE_STOCK_RANKINGS,
)
from extract.stock_prices import fetch_stock_prices
from transform.stock_features import build_stock_features
from transform.stock_rankings import build_stock_rankings
from load.postgres_loader import insert_dataframe, truncate_tables


def run_full_refresh_pipeline(engine, tickers):
    print("Starting full refresh pipeline...")

    stock_prices_df = fetch_stock_prices(
        tickers,
        period=PRICE_HISTORY_PERIOD,
        interval=PRICE_INTERVAL,
    )

    if stock_prices_df.empty:
        print("No stock price data fetched. Aborting.")
        return

    print(f"Stock prices shape:   {stock_prices_df.shape}")

    stock_features_df = build_stock_features(stock_prices_df)
    stock_rankings_df = build_stock_rankings(stock_features_df)

    print(f"Stock features shape: {stock_features_df.shape}")
    print(f"Stock rankings shape: {stock_rankings_df.shape}")

    # Wipe in dependency order (rankings → features → prices), then reload
    truncate_tables(
        engine,
        [DB_TABLE_STOCK_RANKINGS, DB_TABLE_STOCK_FEATURES, DB_TABLE_STOCK_PRICES],
    )

    insert_dataframe(stock_prices_df,   DB_TABLE_STOCK_PRICES,   engine, chunk_size=UPSERT_CHUNK_SIZE)
    insert_dataframe(stock_features_df, DB_TABLE_STOCK_FEATURES, engine, chunk_size=UPSERT_CHUNK_SIZE)
    insert_dataframe(stock_rankings_df, DB_TABLE_STOCK_RANKINGS, engine, chunk_size=UPSERT_CHUNK_SIZE)

    print("Full refresh pipeline completed successfully.")