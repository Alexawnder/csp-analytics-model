import pandas as pd


def build_stock_rankings(
    stock_features_df: pd.DataFrame,
    trend_weight: float = 0.3,
    momentum_weight: float = 0.5,
    volatility_weight: float = 0.2
) -> pd.DataFrame:
    """
    Score and rank stocks using the latest row of features per ticker.

    Composite score:
        ranking_score = (trend_score * trend_weight)
                      + (momentum_score * momentum_weight)
                      - (volatility_penalty * volatility_weight)

    Scoring components:
      - trend_score:        points for price above MA20 (+10), above MA50 (+15),
                            and MA20 crossing above MA50 (+15). Max = 40.
      - momentum_score:     sum of 5d and 20d returns scaled by 100 so the
                            values are in a comparable range to trend_score.
      - volatility_penalty: 30d return std dev clipped to [0, 1] then scaled
                            by 50 so extreme volatility can offset strong momentum.

    Default weights (0.3 / 0.5 / 0.2) can be overridden and are applied as-is —
    normalisation to sum=1 is handled by the caller (e.g. the Streamlit dashboard).
    """
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

    latest["trend_score"] = (
        latest["above_ma20"].fillna(False).astype(int) * 10 +
        latest["above_ma50"].fillna(False).astype(int) * 15 +
        (latest["ma20"] > latest["ma50"]).fillna(False).astype(int) * 15
    )

    latest["momentum_score"] = (
        latest["return_5d"].fillna(0) * 100 +
        latest["return_20d"].fillna(0) * 100
    )

    latest["volatility_penalty"] = (
        latest["volatility_30d"].fillna(0).clip(lower=0, upper=1) * 50
    )

    latest["ranking_score"] = (
        latest["trend_score"] * trend_weight +
        latest["momentum_score"] * momentum_weight -
        latest["volatility_penalty"] * volatility_weight
    )

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

    return final_df