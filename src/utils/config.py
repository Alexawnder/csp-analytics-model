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
PRICE_HISTORY_PERIOD = "3mo"
PRICE_INTERVAL = "1d"
MAX_OPTION_EXPIRATIONS = 8

# Candidate filtering
MIN_DTE = 0
MAX_DTE = 90
REQUIRE_POSITIVE_BID_ASK = True
MAX_STRIKE_TO_PRICE_RATIO = 1.00   # strike <= current_price

# Optional scoring / display filters
MIN_PREMIUM_DOLLARS = 20
MIN_DOWNSIDE_PROTECTION_PCT = 0.00

# Database
DB_TABLE_STOCK_PRICES = "stock_prices"
DB_TABLE_OPTIONS_PUTS = "options_puts"
DB_TABLE_STOCK_FEATURES = "stock_features"
DB_TABLE_CSP_CANDIDATES = "csp_candidates"
DB_TABLE_STOCK_RANKINGS = "stock_rankings"