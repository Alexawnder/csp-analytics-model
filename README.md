# Stock Ranking Data Pipeline

A production-style end-to-end data pipeline that ingests daily stock price data, engineers technical features, ranks equities by a configurable scoring model, and surfaces results in an interactive dashboard.

Built to simulate a real-world ETL workflow — from raw API ingestion through feature engineering, PostgreSQL persistence, and a live Streamlit front-end.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-NeonDB-336791?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-FF4B4B?logo=streamlit&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-ETL-150458?logo=pandas&logoColor=white)

---

## What it does

- Fetches daily OHLCV price data for 40+ tickers via a batched `yfinance` API call
- Engineers rolling features: moving averages (MA20/MA50), momentum returns (5d/20d), and 30-day volatility
- Scores and ranks each stock using a configurable trend + momentum + volatility model
- Persists all data to PostgreSQL using upsert logic so the pipeline is fully idempotent
- Displays results in a Streamlit dashboard with live filtering, weight adjustment, and per-ticker drill-down

---

## Architecture

```
yfinance API
     │
     ▼
┌─────────────┐
│   Extract   │  Batched OHLCV download (stock_prices.py)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Transform  │  Feature engineering + ranking (stock_features.py, stock_rankings.py)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Load     │  Upsert to PostgreSQL via SQLAlchemy (postgres_loader.py)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Dashboard  │  Streamlit + Plotly interactive UI (app.py)
└─────────────┘
```

---

## Project structure

```
src/
├── extract/
│   └── stock_prices.py        # yfinance batch download + incremental fetch
├── transform/
│   ├── stock_features.py      # rolling MA, returns, volatility
│   └── stock_rankings.py      # scoring model
├── load/
│   └── postgres_loader.py     # upsert, insert, truncate, query helpers
├── pipeline/
│   ├── full_refresh.py        # wipe + reload all data
│   └── daily_incremental.py   # fetch only new rows, recompute affected tickers
├── utils/
│   ├── db.py                  # SQLAlchemy engine (single source of truth)
│   └── config.py              # tickers, table names, pipeline constants
├── app.py                     # Streamlit dashboard
└── main.py                    # CLI entrypoint (--mode full | daily)
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Data ingestion | yfinance |
| Data processing | pandas, NumPy |
| Database | PostgreSQL (NeonDB serverless) |
| ORM / queries | SQLAlchemy |
| Dashboard | Streamlit, Plotly |
| Environment | python-dotenv |

---

## Ranking model

Each stock receives a composite score:

```
ranking_score = (trend_score × trend_weight)
              + (momentum_score × momentum_weight)
              - (volatility_penalty × volatility_weight)
```

| Signal | How it's calculated |
|---|---|
| Trend score | Points for trading above MA20, MA50, and MA20 > MA50 crossover |
| Momentum score | Weighted sum of 5-day and 20-day price returns |
| Volatility penalty | 30-day rolling standard deviation of daily returns |

Default weights (0.3 / 0.5 / 0.2) are adjustable live in the dashboard sidebar — all weights are auto-normalised to sum to 1.

---

## Pipeline modes

The pipeline supports two run modes:

**Full refresh** — truncates all tables and reloads the full price history from scratch:
```bash
python src/main.py --mode full
```

**Daily incremental** — fetches only new price rows since the last stored date per ticker, then recomputes features and rankings for affected tickers only:
```bash
python src/main.py --mode daily
```

---

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/yourusername/csp-analytics-model.git
cd csp-analytics-model
```

**2. Create and activate a virtual environment**
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS / Linux
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure environment variables**

Create a `.env` file in the project root:
```
DATABASE_URL=postgresql+psycopg2://user:password@host/dbname
```

If using NeonDB, use the **pooled connection string** from your Neon dashboard (hostname contains `-pooler`) for best performance.

**5. Run the pipeline**
```bash
# First time — full load
python src/main.py --mode full

# Every day after
python src/main.py --mode daily
```

**6. Launch the dashboard**
```bash
streamlit run src/app.py
```

---

## Dashboard features

- **Leaderboard** — top-ranked stocks with sortable columns
- **Live weight sliders** — adjust trend/momentum/volatility weights and re-rank in real time
- **Filters** — max price, minimum score, exclude leveraged/inverse ETFs
- **Ticker detail** — full breakdown of scores, MA levels, and why a stock ranked where it did
- **Price chart** — close price with MA20 and MA50 overlaid

---

## Future improvements

- Expand universe to S&P 500
- Add GitHub Actions workflow for automated daily runs
- Add structured pipeline logging (log file + run history table)
- Deploy dashboard to Streamlit Cloud
- Add additional signals: RSI, volume trend, earnings proximity

---

## Screenshot

*(Add dashboard screenshot here)*