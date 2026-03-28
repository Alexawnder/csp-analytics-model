"""
Stock price extraction via yfinance 1.2.0+.

yfinance 1.2.0 returns a MultiIndex DataFrame where:
  - Date lives in the index (NOT a column after reset_index)
  - Columns are (Ticker, Field) e.g. ('AAPL', 'Close')

All tickers are downloaded in a single batched call to minimise API round-trips.
A per-ticker sequential fallback is used if the batch call fails.
"""

from typing import List
import pandas as pd
import yfinance as yf


PRICE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "ticker"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_batch_download(raw_df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    """
    Parse a multi-ticker yfinance batch download into tidy PRICE_COLUMNS format.

    yfinance 1.2.0+:
      - Date is in raw_df.index (DatetimeIndex) — do NOT reset_index before parsing
      - Columns are MultiIndex: (Ticker, Field) e.g. ('AAPL', 'Close')

    Older yfinance:
      - Columns are MultiIndex: (Field, Ticker) e.g. ('Close', 'AAPL')
    """
    if raw_df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    # Flat columns = single ticker, delegate to single-ticker parser
    if not isinstance(raw_df.columns, pd.MultiIndex):
        if len(tickers) == 1:
            return _standardize_single_ticker(raw_df.reset_index(), tickers[0])
        print("[batch parse] Unexpected flat columns for multi-ticker download.")
        return pd.DataFrame(columns=PRICE_COLUMNS)

    # Date lives in the index — pull it out once
    dates = raw_df.index

    # Detect column ordering
    level0_values = set(raw_df.columns.get_level_values(0))
    ticker_first  = any(t.upper() in level0_values for t in tickers)

    all_rows: List[pd.DataFrame] = []

    for ticker in tickers:
        t = ticker.upper()
        try:
            if ticker_first:
                # yfinance 1.2.0+: (Ticker, Field)
                sub = pd.DataFrame({
                    "date":   dates,
                    "open":   raw_df[(t, "Open")].values,
                    "high":   raw_df[(t, "High")].values,
                    "low":    raw_df[(t, "Low")].values,
                    "close":  raw_df[(t, "Close")].values,
                    "volume": raw_df[(t, "Volume")].values,
                    "ticker": t,
                })
            else:
                # Older yfinance: (Field, Ticker)
                sub = pd.DataFrame({
                    "date":   dates,
                    "open":   raw_df[("Open",   t)].values,
                    "high":   raw_df[("High",   t)].values,
                    "low":    raw_df[("Low",    t)].values,
                    "close":  raw_df[("Close",  t)].values,
                    "volume": raw_df[("Volume", t)].values,
                    "ticker": t,
                })
        except KeyError:
            print(f"[batch parse] Missing columns for {ticker} -- skipping.")
            continue

        all_rows.append(sub)

    if not all_rows:
        print(f"[batch parse] No tickers parsed. Column sample: {raw_df.columns[:6].tolist()}")
        return pd.DataFrame(columns=PRICE_COLUMNS)

    return _clean_price_df(pd.concat(all_rows, ignore_index=True))


def _standardize_single_ticker(raw_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Standardize a single-ticker yfinance download into PRICE_COLUMNS format.
    """
    if raw_df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df = raw_df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df["ticker"] = ticker.upper()

    required = ["Date", "Open", "High", "Low", "Close", "Volume", "ticker"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        print(f"[single parse] {ticker}: missing columns {missing}")
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df = df[required].copy()
    df.columns = PRICE_COLUMNS
    return _clean_price_df(df)


def _clean_price_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce types, drop nulls, deduplicate, and sort."""
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper()

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close"])
    df = df.drop_duplicates(subset=["ticker", "date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df[PRICE_COLUMNS]


# ---------------------------------------------------------------------------
# Per-ticker fallback
# ---------------------------------------------------------------------------

def _fetch_tickers_individually(
    tickers: List[str],
    period: str | None = None,
    interval: str = "1d",
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    all_data = []

    for ticker in tickers:
        try:
            kwargs = dict(auto_adjust=False, progress=False)
            if period:
                kwargs.update(period=period, interval=interval)
            else:
                kwargs.update(start=start, end=end)
            raw_df   = yf.download(ticker, **kwargs)
            clean_df = _standardize_single_ticker(raw_df, ticker)
            if not clean_df.empty:
                all_data.append(clean_df)
        except Exception as e:
            print(f"[fallback] {ticker} failed: {e}")

    if not all_data:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    return (
        pd.concat(all_data, ignore_index=True)
        .drop_duplicates(subset=["ticker", "date"])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

def fetch_stock_prices(
    tickers: List[str],
    period: str = "3mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """Fetch prices for all tickers in one batched download (full-refresh)."""
    print(f"[fetch] Downloading {len(tickers)} tickers (period={period}, interval={interval})...")
    try:
        raw_df = yf.download(
            tickers,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            group_by="ticker",
        )
        df = _parse_batch_download(raw_df, tickers)
    except Exception as e:
        print(f"[fetch] Batch download failed: {e}. Falling back to per-ticker.")
        df = _fetch_tickers_individually(tickers, period=period, interval=interval)

    if df.empty:
        print("[fetch] No usable price data returned.")
    else:
        print(f"[fetch] {len(df)} rows for {df['ticker'].nunique()} tickers.")
    return df


def extract_stock_prices_full(
    tickers: List[str],
    start_date: str,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch full price history using explicit start/end dates (batched)."""
    print(f"[extract full] {len(tickers)} tickers from {start_date} to {end_date or 'today'}...")
    try:
        raw_df = yf.download(
            tickers,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            group_by="ticker",
        )
        df = _parse_batch_download(raw_df, tickers)
    except Exception as e:
        print(f"[extract full] Batch failed: {e}. Falling back to per-ticker.")
        df = _fetch_tickers_individually(tickers, start=start_date, end=end_date)

    print(f"[extract full] {len(df)} rows for {df['ticker'].nunique()} tickers.")
    return df


def fetch_incremental_stock_prices(
    tickers: List[str],
    latest_dates_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fetch only rows newer than the latest stored date per ticker."""
    today = pd.Timestamp.today().normalize()

    latest_map: dict = {}
    if not latest_dates_df.empty:
        latest_map = dict(
            zip(
                latest_dates_df["ticker"],
                pd.to_datetime(latest_dates_df["last_date"]),
            )
        )

    # Group tickers by their required start date to batch together
    start_groups: dict[str, List[str]] = {}
    for ticker in tickers:
        if ticker in latest_map and pd.notna(latest_map[ticker]):
            start_dt = latest_map[ticker] + pd.Timedelta(days=1)
        else:
            start_dt = today - pd.Timedelta(days=365 * 2)

        if start_dt >= today:
            print(f"[incremental] {ticker}: already up to date.")
            continue

        key = start_dt.strftime("%Y-%m-%d")
        start_groups.setdefault(key, []).append(ticker)

    if not start_groups:
        print("[incremental] All tickers are up to date.")
        return pd.DataFrame(columns=PRICE_COLUMNS)

    end_str   = (today + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    all_parts = []

    for start_str, group_tickers in start_groups.items():
        print(f"[incremental] Fetching {len(group_tickers)} tickers from {start_str}...")
        try:
            raw_df = yf.download(
                group_tickers,
                start=start_str,
                end=end_str,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
            )
            part = _parse_batch_download(raw_df, group_tickers)
        except Exception as e:
            print(f"[incremental] Batch failed ({start_str}): {e}. Falling back.")
            part = _fetch_tickers_individually(group_tickers, start=start_str, end=end_str)

        if not part.empty:
            all_parts.append(part)

    if not all_parts:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    final_df = (
        pd.concat(all_parts, ignore_index=True)
        .drop_duplicates(subset=["ticker", "date"])
        .reset_index(drop=True)
    )
    print(f"[incremental] {len(final_df)} new rows across {final_df['ticker'].nunique()} tickers.")
    return final_df