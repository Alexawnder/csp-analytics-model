import logging
from typing import List

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

PRICE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "ticker"]


def _standardize_price_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Standardize raw yfinance output into the stock_prices table format.
    """
    if df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df["ticker"] = ticker

    required_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "ticker"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        logger.warning("Missing expected columns for %s: %s", ticker, missing_cols)
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df = df[required_cols].copy()
    df.columns = PRICE_COLUMNS

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper()

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["ticker", "date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    return df


def fetch_stock_prices(
    tickers: List[str],
    period: str = "3mo",
    interval: str = "1d"
) -> pd.DataFrame:
    """
    Fetch stock prices using a period/interval approach.
    Useful for full refresh style runs.
    """
    all_data = []

    for ticker in tickers:
        logger.info("Downloading price data for %s...", ticker)

        try:
            raw_df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=False,
                progress=False
            )

            clean_df = _standardize_price_data(raw_df, ticker)

            if clean_df.empty:
                logger.warning("No usable data returned for %s", ticker)
                continue

            all_data.append(clean_df)

        except Exception as e:
            logger.error("Error downloading price data for %s: %s", ticker, e)

    if not all_data:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)
    return final_df


def fetch_incremental_stock_prices(
    tickers: List[str],
    latest_dates_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Fetch only missing stock price rows since the latest stored date per ticker.
    """
    all_new_data = []
    today = pd.Timestamp.today().normalize()

    latest_map = {}
    if not latest_dates_df.empty:
        latest_map = dict(
            zip(
                latest_dates_df["ticker"],
                pd.to_datetime(latest_dates_df["last_date"])
            )
        )

    for ticker in tickers:
        if ticker in latest_map and pd.notna(latest_map[ticker]):
            start_date = latest_map[ticker] + pd.Timedelta(days=1)
        else:
            start_date = today - pd.Timedelta(days=365 * 2)

        if start_date >= today:
            logger.info("[extract incremental] %s: up to date", ticker)
            continue

        logger.info("[extract incremental] %s: %s -> %s", ticker, start_date.date(), today.date())

        try:
            raw_df = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=(today + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=False,
                progress=False
            )

            clean_df = _standardize_price_data(raw_df, ticker)

            if clean_df.empty:
                logger.warning("[extract incremental] No usable new data for %s", ticker)
                continue

            all_new_data.append(clean_df)

        except Exception as e:
            logger.error("[extract incremental] %s failed: %s", ticker, e)

    if not all_new_data:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    final_df = pd.concat(all_new_data, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)
    return final_df
