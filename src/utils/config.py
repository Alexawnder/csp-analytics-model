"""
Central configuration for the stock ranking pipeline.
All tuneable constants live here — import from this module, never hardcode elsewhere.
"""

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
TICKERS = [
    # Big Tech
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
    # Banks / Financial
    "JPM", "BAC", "WFC", "GS", "MS",
    # Healthcare
    "UNH", "JNJ", "PFE", "MRK", "ABBV",
    # Consumer
    "WMT", "COST", "HD", "MCD", "NKE", "SBUX",
    # Industrials
    "CAT", "BA", "GE", "HON",
    # Energy
    "XOM", "CVX",
    # Tech
    "ORCL", "ADBE", "CRM", "INTC", "CSCO",
    # Other
    "DIS", "NFLX", "PEP", "KO", "T", "VZ",
]

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
PRICE_HISTORY_PERIOD = "3mo"   # used by full-refresh period-based fetch
PRICE_INTERVAL = "1d"

# ---------------------------------------------------------------------------
# Pipeline / loader
# ---------------------------------------------------------------------------
UPSERT_CHUNK_SIZE = 500        # rows per DB batch

# ---------------------------------------------------------------------------
# Database table names
# ---------------------------------------------------------------------------
DB_TABLE_STOCK_PRICES = "stock_prices"
DB_TABLE_STOCK_FEATURES = "stock_features"
DB_TABLE_STOCK_RANKINGS = "stock_rankings"