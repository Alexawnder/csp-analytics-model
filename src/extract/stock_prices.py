from typing import List
import pandas as pd
import yfinance as yf


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
        print(f"Missing expected columns for {ticker}: {missing_cols}")
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
        print(f"Downloading price data for {ticker}...")

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
                print(f"No usable data returned for {ticker}")
                continue

            all_data.append(clean_df)

        except Exception as e:
            print(f"Error downloading price data for {ticker}: {e}")

    if not all_data:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)
    return final_df


def extract_stock_prices_full(
    tickers: List[str],
    start_date: str,
    end_date: str | None = None
) -> pd.DataFrame:
    """
    Fetch full stock price history using explicit start/end dates.
    """
    all_data = []

    for ticker in tickers:
        print(f"[extract full] {ticker}")

        try:
            raw_df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False
            )

            clean_df = _standardize_price_data(raw_df, ticker)

            if clean_df.empty:
                print(f"[extract full] No usable data for {ticker}")
                continue

            all_data.append(clean_df)

        except Exception as e:
            print(f"[extract full] {ticker} failed: {e}")

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
            print(f"[extract incremental] {ticker}: up to date")
            continue

        print(f"[extract incremental] {ticker}: {start_date.date()} -> {today.date()}")

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
                print(f"[extract incremental] No usable new data for {ticker}")
                continue

            all_new_data.append(clean_df)

        except Exception as e:
            print(f"[extract incremental] {ticker} failed: {e}")

    if not all_new_data:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    final_df = pd.concat(all_new_data, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)
    return final_df