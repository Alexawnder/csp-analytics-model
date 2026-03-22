import numpy as np
import pandas as pd


def build_csp_candidates(
    stock_features_df: pd.DataFrame,
    options_puts_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Build scored CSP candidate dataset by combining latest stock features
    with current put options data.

    This version is tuned for Yahoo Finance data, which can be sparse or
    inconsistent for options chains.
    """

    final_cols = [
        "ticker",
        "snapshot_date",
        "expiration_date",
        "dte",
        "contract_symbol",
        "current_price",
        "strike",
        "bid",
        "ask",
        "mid_price",
        "premium_dollars",
        "collateral",
        "raw_yield",
        "annualized_yield",
        "breakeven",
        "downside_protection_pct",
        "strike_distance_pct",
        "open_interest",
        "volume",
        "implied_volatility",
        "ma20",
        "ma50",
        "volatility_30d",
        "above_ma20",
        "above_ma50",
        "candidate_score"
    ]

    if stock_features_df.empty or options_puts_df.empty:
        return pd.DataFrame(columns=final_cols)

    stock_df = stock_features_df.copy()
    puts_df = options_puts_df.copy()

    # Standardize date columns
    stock_df["date"] = pd.to_datetime(stock_df["date"])
    puts_df["snapshot_date"] = pd.to_datetime(puts_df["snapshot_date"])
    puts_df["expiration_date"] = pd.to_datetime(puts_df["expiration_date"])

    # Standardize ticker formatting
    stock_df["ticker"] = stock_df["ticker"].astype(str).str.upper()
    puts_df["ticker"] = puts_df["ticker"].astype(str).str.upper()

    # Standardize numeric columns
    numeric_cols = [
        "strike",
        "bid",
        "ask",
        "last_price",
        "volume",
        "open_interest",
        "implied_volatility"
    ]
    for col in numeric_cols:
        if col in puts_df.columns:
            puts_df[col] = pd.to_numeric(puts_df[col], errors="coerce")

    # Get latest stock feature row for each ticker
    latest_stock = (
        stock_df.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .copy()
    )

    latest_stock = latest_stock[
        [
            "ticker",
            "close",
            "ma20",
            "ma50",
            "volatility_30d",
            "above_ma20",
            "above_ma50"
        ]
    ].rename(columns={"close": "current_price"})

    # Merge latest stock features onto options data
    candidates = puts_df.merge(latest_stock, on="ticker", how="left")

    print(f"[csp_candidates] Rows after merge: {len(candidates)}")
    print(
        f"[csp_candidates] Missing current_price rows: "
        f"{candidates['current_price'].isna().sum()}"
    )

    # Drop rows where merge failed
    candidates = candidates[candidates["current_price"].notna()].copy()

    # Basic calculations
    candidates["mid_price"] = (candidates["bid"] + candidates["ask"]) / 2
    candidates["premium_dollars"] = candidates["mid_price"] * 100
    candidates["collateral"] = candidates["strike"] * 100
    candidates["dte"] = (
        candidates["expiration_date"] - candidates["snapshot_date"]
    ).dt.days

    # Early validation filters
    candidates = candidates[
        (candidates["dte"] >= 0) &
        (candidates["collateral"] > 0) &
        (candidates["current_price"] > 0) &
        (candidates["strike"] > 0)
    ].copy()

    print(f"[csp_candidates] Rows after early validation: {len(candidates)}")

    # Derived metrics
    candidates["raw_yield"] = candidates["premium_dollars"] / candidates["collateral"]

    candidates["annualized_yield"] = np.where(
        candidates["dte"] > 0,
        candidates["raw_yield"] * (365 / candidates["dte"]),
        np.nan
    )

    candidates["breakeven"] = candidates["strike"] - candidates["mid_price"]

    candidates["downside_protection_pct"] = (
        (candidates["current_price"] - candidates["breakeven"])
        / candidates["current_price"]
    )

    candidates["strike_distance_pct"] = (
        (candidates["current_price"] - candidates["strike"])
        / candidates["current_price"]
    )

    # Spread quality
    candidates["spread"] = candidates["ask"] - candidates["bid"]
    candidates["spread_pct_mid"] = np.where(
        candidates["mid_price"] > 0,
        candidates["spread"] / candidates["mid_price"],
        np.nan
    )

    # Diagnostics
    print("[csp_candidates] bid > 0:", (candidates["bid"] > 0).sum())
    print("[csp_candidates] ask > 0:", (candidates["ask"] > 0).sum())
    print("[csp_candidates] mid_price > 0:", (candidates["mid_price"] > 0).sum())
    print(
        "[csp_candidates] open_interest not null:",
        candidates["open_interest"].notna().sum()
    )
    print(
        "[csp_candidates] volume not null:",
        candidates["volume"].notna().sum()
    )
    print(
        "[csp_candidates] 0 <= dte <= 90:",
        candidates["dte"].between(0, 90).sum()
    )
    print(
        "[csp_candidates] strike <= current_price:",
        (candidates["strike"] <= candidates["current_price"]).sum()
    )

    # Looser CSP filter block for Yahoo Finance data
    candidates = candidates[
        (candidates["bid"] > 0) &
        (candidates["ask"] > 0) &
        (candidates["mid_price"] > 0) &
        (candidates["dte"].between(0, 90)) &
        (candidates["strike"] <= candidates["current_price"])
    ].copy()

    print(f"[csp_candidates] Rows after CSP filters: {len(candidates)}")

    if candidates.empty:
        print("[csp_candidates] No candidates survived filtering.")
        return pd.DataFrame(columns=final_cols)

    # Optional quality flag for sparse Yahoo data
    candidates["has_liquidity_data"] = (
        candidates["open_interest"].notna() | candidates["volume"].notna()
    )

    # Scoring components
    candidates["liquidity_score"] = np.where(
        candidates["has_liquidity_data"],
        np.log1p(candidates["open_interest"].fillna(0)) +
        np.log1p(candidates["volume"].fillna(0)),
        0
    )

    candidates["trend_score"] = (
        candidates["above_ma20"].fillna(False).astype(int) +
        candidates["above_ma50"].fillna(False).astype(int)
    )

    candidates["spread_penalty"] = (
        candidates["spread_pct_mid"]
        .fillna(999)
        .clip(lower=0, upper=5)
    )

    candidates["volatility_penalty"] = (
        candidates["volatility_30d"]
        .fillna(0)
        .clip(lower=0, upper=1)
    )

    # Final score
    candidates["candidate_score"] = (
        candidates["annualized_yield"].fillna(0) * 40
        + candidates["downside_protection_pct"].fillna(0) * 30
        + candidates["liquidity_score"] * 2
        + candidates["trend_score"] * 5
        - candidates["spread_penalty"] * 10
        - candidates["volatility_penalty"] * 10
    )

    # Round derived metrics only
    round_cols = [
        "current_price",
        "mid_price",
        "premium_dollars",
        "collateral",
        "raw_yield",
        "annualized_yield",
        "breakeven",
        "downside_protection_pct",
        "strike_distance_pct",
        "implied_volatility",
        "ma20",
        "ma50",
        "volatility_30d",
        "candidate_score"
    ]

    for col in round_cols:
        if col in candidates.columns:
            candidates[col] = pd.to_numeric(
                candidates[col], errors="coerce"
            ).round(4)

    # Final sort
    candidates = candidates.sort_values(
        ["snapshot_date", "candidate_score"],
        ascending=[False, False]
    ).reset_index(drop=True)

    for col in final_cols:
        if col not in candidates.columns:
            candidates[col] = pd.NA

    final_df = candidates[final_cols].copy()

    # Final type cleanup for PostgreSQL
    final_df["snapshot_date"] = pd.to_datetime(final_df["snapshot_date"]).dt.date
    final_df["expiration_date"] = pd.to_datetime(final_df["expiration_date"]).dt.date
    final_df["above_ma20"] = final_df["above_ma20"].astype("boolean")
    final_df["above_ma50"] = final_df["above_ma50"].astype("boolean")

    print(f"[csp_candidates] Final output rows: {len(final_df)}")

    return final_df