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
    "XOM", "CVX", "COP", "OXY", "SLB",

    # Tech Tickers
    "ORCL", "ADBE", "CRM", "INTC", "CSCO",

    # Other tickers
    "DIS", "NFLX", "PEP", "KO", "T", "VZ",

    # Market benchmark (used for relative strength + regime detection, excluded from rankings)
    "SPY"
]

# Sector mapping (GICS classifications)
TICKER_SECTORS = {
    "AAPL": "Technology",             "MSFT": "Technology",
    "NVDA": "Technology",             "AMD":  "Technology",
    "ORCL": "Technology",             "ADBE": "Technology",
    "CRM":  "Technology",             "INTC": "Technology",
    "CSCO": "Technology",
    "AMZN": "Retail & Leisure",        "TSLA": "Retail & Leisure",
    "HD":   "Retail & Leisure",        "MCD":  "Retail & Leisure",
    "NKE":  "Retail & Leisure",        "SBUX": "Retail & Leisure",
    "WMT":  "Consumer Products",       "COST": "Consumer Products",
    "PEP":  "Consumer Products",       "KO":   "Consumer Products",
    "META": "Communication Services", "GOOGL": "Communication Services",
    "DIS":  "Communication Services", "NFLX": "Communication Services",
    "T":    "Communication Services", "VZ":   "Communication Services",
    "JPM":  "Financials",             "BAC":  "Financials",
    "WFC":  "Financials",             "GS":   "Financials",
    "MS":   "Financials",
    "UNH":  "Healthcare",             "JNJ":  "Healthcare",
    "PFE":  "Healthcare",             "MRK":  "Healthcare",
    "ABBV": "Healthcare",
    "CAT":  "Industrials",            "BA":   "Industrials",
    "GE":   "Industrials",            "HON":  "Industrials",
    "XOM":  "Energy",                 "CVX":  "Energy",
    "COP":  "Energy",                 "OXY":  "Energy",
    "SLB":  "Energy",
}

# Extraction settings
PRICE_HISTORY_PERIOD = "6mo"
PRICE_INTERVAL = "1d"

# Database
DB_TABLE_STOCK_PRICES = "stock_prices"
DB_TABLE_STOCK_FEATURES = "stock_features"
DB_TABLE_STOCK_RANKINGS = "stock_rankings"