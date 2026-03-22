from datetime import datetime
from typing import List

import pandas as pd
import yfinance as yf


def fetch_options_puts(tickers: List[str], max_expirations: int = 8) -> pd.DataFrame:
    """
    Fetch put options data for a list of tickers.

    Args:
        tickers: List of ticker symbols.
        max_expirations: Number of nearest expiration dates to fetch per ticker.

    Returns:
        A cleaned pandas DataFrame with standardized columns:
        ticker, snapshot_date, expiration_date, contract_symbol, strike,
        bid, ask, last_price, volume, open_interest, implied_volatility, in_the_money
    """
    all_puts = []
    snapshot_date = pd.to_datetime(datetime.today().date())

    desired_cols = [
        "ticker",
        "snapshot_date",
        "expiration_date",
        "contractSymbol",
        "strike",
        "bid",
        "ask",
        "lastPrice",
        "volume",
        "openInterest",
        "impliedVolatility",
        "inTheMoney"
    ]

    rename_map = {
        "contractSymbol": "contract_symbol",
        "lastPrice": "last_price",
        "openInterest": "open_interest",
        "impliedVolatility": "implied_volatility",
        "inTheMoney": "in_the_money"
    }

    for ticker in tickers:
        print(f"Downloading options data for {ticker}...")

        try:
            stock = yf.Ticker(ticker)
            expirations = stock.options
        except Exception as e:
            print(f"Could not get expirations for {ticker}: {e}")
            continue

        if not expirations:
            print(f"No expirations found for {ticker}")
            continue

        for expiration in expirations[:max_expirations]:
            try:
                chain = stock.option_chain(expiration)
                puts = chain.puts.copy()

                if puts.empty:
                    print(f"No put contracts found for {ticker} {expiration}")
                    continue

                puts["ticker"] = ticker
                puts["snapshot_date"] = snapshot_date
                puts["expiration_date"] = pd.to_datetime(expiration)

                available_cols = [col for col in desired_cols if col in puts.columns]
                puts = puts[available_cols].copy()

                all_puts.append(puts)

            except Exception as e:
                print(f"Could not get puts for {ticker} {expiration}: {e}")

    if not all_puts:
        return pd.DataFrame(columns=[
            "ticker",
            "snapshot_date",
            "expiration_date",
            "contract_symbol",
            "strike",
            "bid",
            "ask",
            "last_price",
            "volume",
            "open_interest",
            "implied_volatility",
            "in_the_money"
        ])

    final_puts = pd.concat(all_puts, ignore_index=True)

    final_puts = final_puts.rename(columns=rename_map)

    expected_cols = [
        "ticker",
        "snapshot_date",
        "expiration_date",
        "contract_symbol",
        "strike",
        "bid",
        "ask",
        "last_price",
        "volume",
        "open_interest",
        "implied_volatility",
        "in_the_money"
    ]

    for col in expected_cols:
        if col not in final_puts.columns:
            final_puts[col] = pd.NA

    final_puts = final_puts[expected_cols].copy()

    final_puts["ticker"] = final_puts["ticker"].astype(str)
    final_puts["snapshot_date"] = pd.to_datetime(final_puts["snapshot_date"]).dt.date
    final_puts["expiration_date"] = pd.to_datetime(final_puts["expiration_date"]).dt.date
    final_puts["contract_symbol"] = final_puts["contract_symbol"].astype(str)

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
        final_puts[col] = pd.to_numeric(final_puts[col], errors="coerce")

    if "in_the_money" in final_puts.columns:
        final_puts["in_the_money"] = final_puts["in_the_money"].astype("boolean")

    final_puts = final_puts.drop_duplicates(
        subset=["contract_symbol", "snapshot_date"]
    ).reset_index(drop=True)

    return final_puts