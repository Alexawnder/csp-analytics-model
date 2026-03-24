from extract.stock_prices import fetch_stock_prices
from transform.stock_features import build_stock_features
from transform.stock_rankings import build_stock_rankings
from utils.db import get_engine
from utils.config import (
    TICKERS,
    PRICE_HISTORY_PERIOD,
    PRICE_INTERVAL,
    DB_TABLE_STOCK_PRICES,
    DB_TABLE_STOCK_FEATURES,
    DB_TABLE_STOCK_RANKINGS,
)
from load.postgres_loader import upsert_dataframe


def main():
    print("Starting stock ranking pipeline...")

    engine = get_engine()

    stock_prices_df = fetch_stock_prices(
        TICKERS,
        period=PRICE_HISTORY_PERIOD,
        interval=PRICE_INTERVAL
    )

    print(f"Stock prices shape: {stock_prices_df.shape}")

    stock_features_df = build_stock_features(stock_prices_df)
    stock_rankings_df = build_stock_rankings(stock_features_df)

    print(f"Stock features shape: {stock_features_df.shape}")
    print(f"Stock rankings shape: {stock_rankings_df.shape}")

    upsert_dataframe(stock_prices_df, DB_TABLE_STOCK_PRICES, engine, ["ticker", "date"])
    upsert_dataframe(stock_features_df, DB_TABLE_STOCK_FEATURES, engine, ["ticker", "date"])
    upsert_dataframe(stock_rankings_df, DB_TABLE_STOCK_RANKINGS, engine, ["ticker", "ranking_date"])

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()