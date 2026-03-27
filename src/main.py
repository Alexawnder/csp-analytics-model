import argparse

from utils.db import get_engine
from utils.config import TICKERS
from pipeline.full_refresh import run_full_refresh_pipeline
from pipeline.daily_incremental import run_daily_incremental_pipeline


def main():
    parser = argparse.ArgumentParser(description="Run stock ranking pipeline.")
    parser.add_argument(
        "--mode",
        choices=["full", "daily"],
        default="daily",
        help="Pipeline mode: full refresh or daily incremental"
    )
    args = parser.parse_args()

    engine = get_engine()

    if args.mode == "full":
        run_full_refresh_pipeline(engine, TICKERS)
    elif args.mode == "daily":
        run_daily_incremental_pipeline(engine, TICKERS)
    else:
        raise ValueError(f"Unsupported pipeline mode: {args.mode}")


if __name__ == "__main__":
    main()