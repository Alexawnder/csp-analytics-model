"""
Build stock rankings from the latest feature row per ticker.
"""

import pandas as pd


FINAL_COLS = [
    "ticker",
    "ranking_date",
    "current_price",
    "ma20",
    "ma50",
    "volatility_30d",
    "return_5d",
    "return_20d",
    "above_ma20",
    "above_ma50",
    "trend_score",
    "momentum_score",
    "volatility_penalty",
    "ranking_score",
]

ROUND_COLS = [
    "current_price",
    "ma20",
    "ma50",
    "volatility_30d",
    "return_5d",
    "return_20d",
    "trend_score",
    "momentum_score",
    "volatility_penalty",
    "ranking_score",
]


def build_stock_rankings(
    stock_features_df: pd.DataFrame,
    trend_weight: float = 0.3,
    momentum_weight: float = 0.5,
    volatility_weight: float = 0.2,
) -> pd.DataFrame:
    """
    Derive a ranking score from the most-recent feature row per ticker.

    Score = trend_score * trend_weight
          + momentum_score * momentum_weight
          - volatility_penalty * volatility_weight
    """
    if stock_features_df.empty:
        return pd.DataFrame(columns=FINAL_COLS)

    df = stock_features_df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Keep only the latest row per ticker
    latest = (
        df.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .copy()
        .rename(columns={"date": "ranking_date", "close": "current_price"})
    )

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------
    latest["trend_score"] = (
        latest["above_ma20"].fillna(False).astype(int) * 10
        + latest["above_ma50"].fillna(False).astype(int) * 15
        + (latest["ma20"] > latest["ma50"]).fillna(False).astype(int) * 15
    )

    latest["momentum_score"] = (
        latest["return_5d"].fillna(0)  * 100
        + latest["return_20d"].fillna(0) * 100
    )

    latest["volatility_penalty"] = (
        latest["volatility_30d"].fillna(0).clip(lower=0, upper=1) * 50
    )

    latest["ranking_score"] = (
        latest["trend_score"]        * trend_weight
        + latest["momentum_score"]   * momentum_weight
        - latest["volatility_penalty"] * volatility_weight
    )

    # ------------------------------------------------------------------
    # Rounding & type cleanup
    # ------------------------------------------------------------------
    for col in ROUND_COLS:
        if col in latest.columns:
            latest[col] = pd.to_numeric(latest[col], errors="coerce").round(4)

    for col in FINAL_COLS:
        if col not in latest.columns:
            latest[col] = pd.NA

    final_df = latest[FINAL_COLS].copy()
    final_df["ranking_date"] = pd.to_datetime(final_df["ranking_date"]).dt.date
    final_df["above_ma20"]   = final_df["above_ma20"].astype("boolean")
    final_df["above_ma50"]   = final_df["above_ma50"].astype("boolean")

    final_df = (
        final_df.sort_values("ranking_score", ascending=False)
        .reset_index(drop=True)
    )

    return final_df