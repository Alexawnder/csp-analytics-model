import logging

import pandas as pd

logger = logging.getLogger(__name__)


def build_stock_features(stock_prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineer rolling technical features from raw OHLCV price data.

    For each ticker computes:
      - daily_return:    day-over-day percentage change in close price
      - return_5d/20d:   5- and 20-day cumulative return (momentum signals)
      - ma20/ma50:       20- and 50-day simple moving averages
      - volatility_30d:  30-day rolling standard deviation of daily returns
      - above_ma20/50:   boolean flags for price vs moving average crossovers
      - ma20_above_ma50: golden/death cross indicator

    Requires min_periods equal to the window size so partial windows
    (e.g. the first 19 rows for MA20) produce NaN rather than misleading values.
    """
    final_cols = [
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
        "ma20_above_ma50"
    ]

    if stock_prices_df.empty:
        return pd.DataFrame(columns=final_cols)

    df = stock_prices_df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["ticker", "date", "close"]).copy()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info("[stock_features] Input rows: %d", len(df))

    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    df["return_5d"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.pct_change(periods=5))
    )

    df["return_20d"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.pct_change(periods=20))
    )

    df["ma20"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.rolling(window=20, min_periods=20).mean())
    )

    df["ma50"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.rolling(window=50, min_periods=50).mean())
    )

    df["volatility_30d"] = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda s: s.rolling(window=30, min_periods=30).std())
    )

    df["above_ma20"] = df["close"] > df["ma20"]
    df["above_ma50"] = df["close"] > df["ma50"]
    df["ma20_above_ma50"] = df["ma20"] > df["ma50"]

    df["daily_return"] = df["daily_return"].round(6)
    df["return_5d"] = df["return_5d"].round(6)
    df["return_20d"] = df["return_20d"].round(6)
    df["ma20"] = df["ma20"].round(4)
    df["ma50"] = df["ma50"].round(4)
    df["volatility_30d"] = df["volatility_30d"].round(6)

    final_df = df[final_cols].copy()
    final_df["date"] = pd.to_datetime(final_df["date"]).dt.date
    final_df["above_ma20"] = final_df["above_ma20"].astype("boolean")
    final_df["above_ma50"] = final_df["above_ma50"].astype("boolean")
    final_df["ma20_above_ma50"] = final_df["ma20_above_ma50"].astype("boolean")

    final_df = final_df.drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)

    logger.info("[stock_features] Output rows: %d", len(final_df))
    logger.info("[stock_features] Null return_5d rows: %d", final_df["return_5d"].isna().sum())
    logger.info("[stock_features] Null return_20d rows: %d", final_df["return_20d"].isna().sum())
    logger.info("[stock_features] Null ma20 rows: %d", final_df["ma20"].isna().sum())
    logger.info("[stock_features] Null ma50 rows: %d", final_df["ma50"].isna().sum())
    logger.info("[stock_features] Null volatility_30d rows: %d", final_df["volatility_30d"].isna().sum())

    return final_df
