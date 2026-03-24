# Tickers from interesting companies including leveraged
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AMD", "TSLA", "SOFI", "PLTR",
    "SPY", "QQQ", "IWM", "DIA","TQQQ", "SQQQ", "UPRO", "SPXU", "SOXL", "SOXS",
    "NVDL", "TSLL"
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