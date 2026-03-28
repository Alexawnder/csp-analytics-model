"""
Feature engineering: rolling MAs, returns, volatility, and MA crossover flags.
"""

import pandas as pd


FINAL_COLS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "daily_return",
    "return_5d",
    "return_20d",
    "ma20",
    "ma50",
    "volatility_30d",
    "above_ma20",
    "above_ma50",
    "ma20_above_ma50",
]


def build_stock_features(stock_prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling features for each ticker.

    Improvements over original:
    - groupby("ticker")["close"] is computed once and reused for all
      rolling calculations instead of calling groupby 5 separate times.
    - groupby("ticker")["daily_return"] similarly cached for volatility.
    """
    if stock_prices_df.empty:
        return pd.DataFrame(columns=FINAL_COLS)

    df = stock_prices_df.copy()
    df["date"]   = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.dropna(subset=["ticker", "date", "close"])
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )

    print(f"[stock_features] Input rows: {len(df)}")

    # ------------------------------------------------------------------
    # Rolling calculations — group once, reuse
    # ------------------------------------------------------------------
    close_group = df.groupby("ticker")["close"]

    df["daily_return"]  = close_group.transform("pct_change")
    df["return_5d"]     = close_group.transform(lambda s: s.pct_change(periods=5))
    df["return_20d"]    = close_group.transform(lambda s: s.pct_change(periods=20))
    df["ma20"]          = close_group.transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["ma50"]          = close_group.transform(lambda s: s.rolling(50, min_periods=50).mean())

    ret_group = df.groupby("ticker")["daily_return"]
    df["volatility_30d"] = ret_group.transform(lambda s: s.rolling(30, min_periods=30).std())

    # ------------------------------------------------------------------
    # Boolean flags
    # ------------------------------------------------------------------
    df["above_ma20"]      = df["close"] > df["ma20"]
    df["above_ma50"]      = df["close"] > df["ma50"]
    df["ma20_above_ma50"] = df["ma20"]  > df["ma50"]

    # ------------------------------------------------------------------
    # Rounding
    # ------------------------------------------------------------------
    df["daily_return"]   = df["daily_return"].round(6)
    df["return_5d"]      = df["return_5d"].round(6)
    df["return_20d"]     = df["return_20d"].round(6)
    df["ma20"]           = df["ma20"].round(4)
    df["ma50"]           = df["ma50"].round(4)
    df["volatility_30d"] = df["volatility_30d"].round(6)

    # ------------------------------------------------------------------
    # Finalise
    # ------------------------------------------------------------------
    final_df = df[FINAL_COLS].copy()
    final_df["date"] = pd.to_datetime(final_df["date"]).dt.date

    for col in ["above_ma20", "above_ma50", "ma20_above_ma50"]:
        final_df[col] = final_df[col].astype("boolean")

    final_df = (
        final_df.drop_duplicates(subset=["ticker", "date"])
        .reset_index(drop=True)
    )

    print(f"[stock_features] Output rows:          {len(final_df)}")
    print(f"[stock_features] Null return_5d:        {final_df['return_5d'].isna().sum()}")
    print(f"[stock_features] Null return_20d:       {final_df['return_20d'].isna().sum()}")
    print(f"[stock_features] Null ma20:             {final_df['ma20'].isna().sum()}")
    print(f"[stock_features] Null ma50:             {final_df['ma50'].isna().sum()}")
    print(f"[stock_features] Null volatility_30d:   {final_df['volatility_30d'].isna().sum()}")

    return final_df