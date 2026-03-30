# Tickers from interesting companies including leveraged
TICKERS = [
    # Big Tech Tickers
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",

    # Bank/Financial Tickers
    "JPM", "BAC", "WFC", "GS", "MS",

    # Healthcare Tickers
    "UNH", "JNJ", "PFE", "MRK", "ABBV",

    # Consumer Tickers
    "WMT", "COST", "HD", "MCD", "NKE", "SBUX",

    # Industrials Tickers
    "CAT", "BA", "GE", "HON",

    # Energy Tickers
    "XOM", "CVX",

    # Tech Tickers
    "ORCL", "ADBE", "CRM", "INTC", "CSCO",

    # Other tickers
    "DIS", "NFLX", "PEP", "KO", "T", "VZ"
]

# Extraction settings
PRICE_HISTORY_PERIOD = "6mo"
PRICE_INTERVAL = "1d"

# Database
DB_TABLE_STOCK_PRICES = "stock_prices"
DB_TABLE_STOCK_FEATURES = "stock_features"
DB_TABLE_STOCK_RANKINGS = "stock_rankings"