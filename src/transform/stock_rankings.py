import pandas as pd
import numpy as np


def build_stock_rankings(stock_features_df: pd.DataFrame) -> pd.DataFrame:
    final_cols = [
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
        "ranking_score"
    ]

    if stock_features_df.empty:
        return pd.DataFrame(columns=final_cols)

    df = stock_features_df.copy()
    df["date"] = pd.to_datetime(df["date"])

    latest = (
        df.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .copy()
    )

    latest = latest.rename(columns={
        "date": "ranking_date",
        "close": "current_price"
    })

    # Trend score
    latest["trend_score"] = (
        latest["above_ma20"].fillna(False).astype(int) * 10 +
        latest["above_ma50"].fillna(False).astype(int) * 15 +
        (latest["ma20"] > latest["ma50"]).fillna(False).astype(int) * 15
    )

    # Momentum score
    latest["momentum_score"] = (
        latest["return_5d"].fillna(0) * 100 +
        latest["return_20d"].fillna(0) * 100
    )

    # Volatility penalty
    latest["volatility_penalty"] = (
        latest["volatility_30d"].fillna(0).clip(lower=0, upper=1) * 50
    )

    # Final ranking score
    latest["ranking_score"] = (
        latest["trend_score"] +
        latest["momentum_score"] -
        latest["volatility_penalty"]
    )

    # Round display fields
    round_cols = [
        "current_price",
        "ma20",
        "ma50",
        "volatility_30d",
        "return_5d",
        "return_20d",
        "trend_score",
        "momentum_score",
        "volatility_penalty",
        "ranking_score"
    ]

    for col in round_cols:
        if col in latest.columns:
            latest[col] = pd.to_numeric(latest[col], errors="coerce").round(4)

    for col in final_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    final_df = latest[final_cols].copy()
    final_df["ranking_date"] = pd.to_datetime(final_df["ranking_date"]).dt.date
    final_df["above_ma20"] = final_df["above_ma20"].astype("boolean")
    final_df["above_ma50"] = final_df["above_ma50"].astype("boolean")

    final_df = final_df.sort_values("ranking_score", ascending=False).reset_index(drop=True)

    print(f"[stock_rankings] Final output rows: {len(final_df)}")

    return final_df