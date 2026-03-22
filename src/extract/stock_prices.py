from typing import List

import pandas as pd
import yfinance as yf


def fetch_stock_prices(tickers: List[str], period: str = "3mo") -> pd.DataFrame:
    """
    Fetch recent daily stock price data for a list of tickers.

    Args:
        tickers: List of ticker symbols.
        period: yfinance period string such as '1mo', '3mo', '6mo'.

    Returns:
        A cleaned pandas DataFrame with standardized columns:
        date, open, high, low, close, volume, ticker
    """
    all_data = []

    for ticker in tickers:
        print(f"Downloading price data for {ticker}...")

        try:
            df = yf.download(
                ticker,
                period=period,
                interval="1d",
                auto_adjust=False,
                progress=False
            )

            if df.empty:
                print(f"No data returned for {ticker}")
                continue

            df = df.reset_index()

            # Flatten columns if yfinance returns a MultiIndex
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            df["ticker"] = ticker

            # Keep only needed columns
            required_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "ticker"]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                print(f"Missing expected columns for {ticker}: {missing_cols}")
                continue

            df = df[required_cols].copy()

            # Standardize column names
            df.columns = ["date", "open", "high", "low", "close", "volume", "ticker"]

            # Data cleanup
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ticker"] = df["ticker"].astype(str).str.upper()

            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

            all_data.append(df)

        except Exception as e:
            print(f"Error downloading price data for {ticker}: {e}")

    if not all_data:
        return pd.DataFrame(
            columns=["date", "open", "high", "low", "close", "volume", "ticker"]
        )

    final_df = pd.concat(all_data, ignore_index=True)

    # Drop duplicates just in case
    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)

    return final_df