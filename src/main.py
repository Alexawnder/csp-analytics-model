from extract.stock_prices import fetch_stock_prices
from extract.options_puts import fetch_options_puts
from transform.stock_features import build_stock_features
from transform.csp_candidates import build_csp_candidates
from utils.db import get_engine
from utils.config import TICKERS
from load.postgres_loader import load_dataframe


def main():
    print(" Starting CSP data pipeline...")

    tickers = TICKERS
    engine = get_engine()

    print("\n Extracting data...")
    stock_prices_df = fetch_stock_prices(tickers)
    options_puts_df = fetch_options_puts(tickers)

    print(f"Stock prices shape: {stock_prices_df.shape}")
    print(f"Options puts shape: {options_puts_df.shape}")

    print("\n Transforming data...")
    stock_features_df = build_stock_features(stock_prices_df)
    csp_candidates_df = build_csp_candidates(stock_features_df, options_puts_df)

    print(f"Stock features shape: {stock_features_df.shape}")
    print(f"CSP candidates shape: {csp_candidates_df.shape}")

    print("\n Loading data into PostgreSQL...")

    # Optional: replace for testing (safe during development)
    load_dataframe(stock_prices_df, "stock_prices", engine)
    load_dataframe(options_puts_df, "options_puts", engine)
    load_dataframe(stock_features_df, "stock_features", engine)
    load_dataframe(csp_candidates_df, "csp_candidates", engine)

    print("\n Pipeline completed successfully!")


if __name__ == "__main__":
    main()