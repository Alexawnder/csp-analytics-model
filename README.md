# Stock Ranking Data Pipeline

An end-to-end data pipeline that ingests daily stock price data, engineers technical features, ranks equities by a configurable scoring model, and surfaces results in an interactive dashboard.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-NeonDB-336791?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-FF4B4B?logo=streamlit&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-ETL-150458?logo=pandas&logoColor=white)
![GitHub Actions](https://github.com/Alexawnder/stock-ranking-pipeline/actions/workflows/daily-pipeline.yml/badge.svg)

---

## Screenshot

*(Add dashboard screenshot here)*

---

## What it does

- Fetches daily OHLCV price data for 40+ tickers via `yfinance`
- Engineers rolling technical features: moving averages (MA20/MA50), momentum returns (5d/20d), and 30-day volatility
- Scores and ranks each stock using a configurable trend + momentum + volatility model
- Persists all data to PostgreSQL using upsert logic so the pipeline is fully idempotent
- Runs automatically each weekday via GitHub Actions
- Displays results in a Streamlit dashboard with live filtering, weight adjustment, and per-ticker drill-down

---

## Architecture

```
yfinance API
     │
     ▼
┌─────────────┐
│   Extract   │  OHLCV download — full or incremental (stock_prices.py)
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
│   └── stock_prices.py        # yfinance download — full and incremental modes
├── transform/
│   ├── stock_features.py      # rolling MA, returns, volatility
│   └── stock_rankings.py      # composite scoring model
├── load/
│   └── postgres_loader.py     # upsert, insert, truncate, query helpers
├── pipeline/
│   ├── full_refresh.py        # wipe + reload all historical data
│   └── daily_incremental.py   # fetch only new rows, recompute affected tickers
├── utils/
│   ├── db.py                  # SQLAlchemy engine singleton
│   └── config.py              # tickers, table names, pipeline settings
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
| Scheduling | GitHub Actions |
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
| Trend score | +10 for price above MA20, +15 for above MA50, +15 for MA20 > MA50 crossover (max 40) |
| Momentum score | 5-day and 20-day returns each scaled by 100 to match trend score range |
| Volatility penalty | 30-day rolling std dev of daily returns, clipped and scaled by 50 |

Default weights (0.3 / 0.5 / 0.2) are adjustable live in the dashboard sidebar — weights are auto-normalised to sum to 1.

---

## Pipeline modes

**Full refresh** — truncates all tables and reloads the full 6-month price history:
```bash
python src/main.py --mode full
```

**Daily incremental** — fetches only new rows since the last stored date per ticker, then recomputes features and rankings for affected tickers only:
```bash
python src/main.py --mode daily
```

Pipeline activity is logged to `pipeline.log` in the project root.

---

## Automated scheduling

The daily incremental pipeline runs automatically at 1 AM UTC on weekdays via GitHub Actions (`.github/workflows/daily-pipeline.yml`). This is after US market close (4 PM ET / 9 PM UTC), giving yfinance time to publish the day's data.

To enable it in your own fork, add your `DATABASE_URL` as a repository secret under **Settings → Secrets and variables → Actions**.

---

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/Alexawnder/stock-ranking-pipeline.git
cd stock-ranking-pipeline
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

- **Leaderboard** — top-ranked stocks with filterable, sortable columns
- **Live weight sliders** — adjust trend/momentum/volatility weights and re-rank in real time
- **Filters** — max price, minimum score, exclude leveraged/inverse ETFs
- **Ticker detail** — full score breakdown, MA levels, and plain-language reasoning for the ranking
- **Price chart** — close price with MA20 and MA50 overlaid

---

## Future improvements

- Expand universe to S&P 500
- Deploy dashboard to Streamlit Cloud
- Add additional signals: RSI, volume trend, earnings proximity
- Add unit tests for feature engineering and scoring logic
