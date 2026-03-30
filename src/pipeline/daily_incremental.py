import logging

from extract.stock_prices import fetch_incremental_stock_prices
from transform.stock_features import build_stock_features
from transform.stock_rankings import build_stock_rankings
from utils.config import (
    DB_TABLE_STOCK_PRICES,
    DB_TABLE_STOCK_FEATURES,
    DB_TABLE_STOCK_RANKINGS,
)
from load.postgres_loader import (
    upsert_dataframe,
    get_latest_price_dates,
    get_price_history_for_tickers,
)

logger = logging.getLogger(__name__)


def run_daily_incremental_pipeline(engine, tickers):
    logger.info("Starting daily incremental pipeline...")

    latest_dates_df = get_latest_price_dates(engine, DB_TABLE_STOCK_PRICES)
    new_prices_df = fetch_incremental_stock_prices(tickers, latest_dates_df)

    if new_prices_df.empty:
        logger.info("No new price data found. Pipeline finished.")
        return

    logger.info("New stock prices shape: %s", new_prices_df.shape)

    upsert_dataframe(new_prices_df, DB_TABLE_STOCK_PRICES, engine, ["ticker", "date"])

    affected_tickers = sorted(new_prices_df["ticker"].dropna().unique().tolist())
    logger.info("Affected tickers: %s", affected_tickers)

    recent_prices_df = get_price_history_for_tickers(
        engine,
        affected_tickers,
        lookback_days=90,
        table_name=DB_TABLE_STOCK_PRICES
    )

    logger.info("Recent prices for recalculation shape: %s", recent_prices_df.shape)

    stock_features_df = build_stock_features(recent_prices_df)
    stock_rankings_df = build_stock_rankings(stock_features_df)

    logger.info("Recomputed stock features shape: %s", stock_features_df.shape)
    logger.info("Updated stock rankings shape: %s", stock_rankings_df.shape)

    upsert_dataframe(stock_features_df, DB_TABLE_STOCK_FEATURES, engine, ["ticker", "date"])
    upsert_dataframe(stock_rankings_df, DB_TABLE_STOCK_RANKINGS, engine, ["ticker", "ranking_date"])

    logger.info("Daily incremental pipeline completed successfully.")
