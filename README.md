# Stock Ranking Data Pipeline

This project is a simple end-to-end data pipeline that pulls stock data, builds features, ranks stocks, and displays the results in a dashboard.

I built this to simulate a real-world data workflow — from raw data ingestion all the way to something you can actually use to explore insights.

---

## What it does

* pulls daily stock price data using `yfinance`
* stores it in PostgreSQL
* builds features like:

  * moving averages (MA20, MA50)
  * short-term momentum (5d, 20d returns)
  * volatility
* ranks stocks based on trend + momentum + risk
* displays everything in a Streamlit dashboard

---

## Tech stack

* Python
* PostgreSQL
* SQLAlchemy
* pandas
* Streamlit
* Plotly

---

## How it works

### 1. Extract

Fetches daily OHLCV data for a list of tickers.

### 2. Transform

Builds features per ticker:

* returns
* moving averages
* volatility
* trend signals (above/below MA)

### 3. Load

Stores data into:

* `stock_prices`
* `stock_features`
* `stock_rankings`

Uses upserts so the pipeline can run repeatedly without duplicating data.

---

## Ranking logic

Each stock gets a score based on:

* momentum (5d / 20d returns)
* trend (above MA20 / MA50)
* volatility (penalty)

Higher score = stronger recent performance with reasonable risk.

---

## Dashboard

The Streamlit app lets you:

* see top-ranked stocks
* filter by price and score
* exclude leveraged ETFs
* inspect individual tickers
* view price charts with moving averages

---

## Running the project

Clone the repo:

```bash
git clone https://github.com/yourusername/your-repo.git
cd your-repo
```

Set up environment:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file:

```
DB_USER=...
DB_PASSWORD=...
DB_HOST=localhost
DB_PORT=5432
DB_NAME=...
```

Run pipeline:

```bash
python main.py
```

Start dashboard:

```bash
streamlit run app.py
```

---

## Notes / things I learned

* handling schema changes (adding new feature columns)
* building idempotent upsert logic in Postgres
* working with time-series data in SQL + pandas
* separating raw vs derived data
* building a simple UI on top of a data pipeline

---

## Future improvements

* expand to S&P 500
* add scheduled daily runs
* add pipeline run logging
* deploy database + dashboard

---

## Screenshot

(Add your dashboard screenshot here)
