# 📊 Stock Ranking Data Pipeline

> Built a production-style data pipeline that ingests market data, engineers financial features, and ranks equities based on trend, momentum, and risk.

---

## 🚀 Overview

This project simulates a **real-world data engineering workflow** for financial analytics.

It automates the full pipeline:

* Ingest daily stock price data
* Store data in a relational database (PostgreSQL)
* Engineer features (momentum, trend, volatility)
* Rank stocks using a scoring model
* Serve results through an interactive Streamlit dashboard

The system is designed to demonstrate:

* ETL pipeline design
* idempotent data loading
* time-series feature engineering
* end-to-end analytics delivery

---

## 🧠 Problem Statement

Financial data is widely available, but extracting **actionable insights** requires:

* structured storage
* consistent feature engineering
* ranking logic to prioritize opportunities

This project builds a system that transforms raw price data into a **ranked list of tradeable candidates**, enabling faster decision-making.

---

## 🏗️ Architecture

```id="arch1"
          +------------------+
          |   yfinance API   |
          +------------------+
                    ↓
          +------------------+
          |  Extract Layer   |
          | (Python scripts) |
          +------------------+
                    ↓
          +------------------+
          |   PostgreSQL DB  |
          |------------------|
          | stock_prices     |
          | stock_features   |
          | stock_rankings   |
          +------------------+
                    ↓
          +------------------+
          | Transform Layer  |
          | Feature Engineering |
          +------------------+
                    ↓
          +------------------+
          | Streamlit App    |
          | (Dashboard UI)   |
          +------------------+
```

---

## ⚙️ Tech Stack

* **Python** – ETL pipeline & feature engineering
* **PostgreSQL** – relational database for structured storage
* **SQLAlchemy** – database connection + upsert logic
* **pandas** – data transformation
* **yfinance** – financial data ingestion
* **Streamlit** – interactive dashboard
* **Plotly** – time-series visualization

---

## 🔄 Data Pipeline

### 1. Extract

* Pulls daily OHLCV data using `yfinance`
* Configurable ticker universe
* Handles missing data and schema normalization

### 2. Transform

Feature engineering includes:

* Daily returns
* 5-day and 20-day momentum
* Moving averages (MA20, MA50)
* 30-day rolling volatility
* Trend indicators (price vs moving averages)

### 3. Load

* Data stored in PostgreSQL:

  * `stock_prices`
  * `stock_features`
  * `stock_rankings`
* Uses **idempotent upserts**:

  * Prevents duplicates
  * Ensures consistent updates
  * Supports repeatable pipeline runs

---

## 🧮 Ranking Model

Each stock is scored using:

* **Trend Score**

  * Above MA20 / MA50

* **Momentum Score**

  * 5-day return
  * 20-day return

* **Volatility Penalty**

  * Higher volatility reduces score

### Final Score:

```id="score1"
ranking_score =
    momentum_score * 0.5 +
    trend_score * 0.3 -
    volatility_penalty * 0.2
```

This prioritizes stocks with:

* strong upward momentum
* positive trend alignment
* manageable risk

---

## 🗄️ Database Schema

### stock_prices

| Column              | Description    |
| ------------------- | -------------- |
| ticker              | Stock symbol   |
| date                | Trading date   |
| open/high/low/close | Price data     |
| volume              | Trading volume |

### stock_features

| Column                  | Description         |
| ----------------------- | ------------------- |
| ticker                  | Stock symbol        |
| date                    | Trading date        |
| return_5d / return_20d  | Momentum indicators |
| ma20 / ma50             | Moving averages     |
| volatility_30d          | Rolling volatility  |
| above_ma20 / above_ma50 | Trend flags         |

### stock_rankings

| Column             | Description        |
| ------------------ | ------------------ |
| ticker             | Stock symbol       |
| ranking_date       | Ranking date       |
| ranking_score      | Final score        |
| trend_score        | Trend component    |
| momentum_score     | Momentum component |
| volatility_penalty | Risk adjustment    |

---

## 📊 Dashboard Features

The Streamlit dashboard enables:

### 🔝 Leaderboard

* Top-ranked stocks
* Sortable and filterable

### 🎯 Filters

* Max price (budget-aware filtering)
* Minimum ranking score
* Exclude leveraged/inverse ETFs

### 📈 Ticker Analysis

* Price vs MA20 / MA50 chart
* Trend and momentum breakdown
* “Why this ranked here” explanation

### 🔄 Live Data

* Pulls directly from PostgreSQL
* Reflects latest pipeline run

---

## 🖼️ Example Dashboard

*(Add screenshot here)*

---

## ▶️ How to Run

### 1. Clone repo

```bash id="run1"
git clone https://github.com/yourusername/your-repo.git
cd your-repo
```

### 2. Set up environment

```bash id="run2"
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure database

Create `.env` file:

```id="env1"
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db
```

### 4. Run pipeline

```bash id="run3"
python main.py
```

### 5. Launch dashboard

```bash id="run4"
streamlit run app.py
```

---

## 📈 Future Improvements

* Expand to S&P 500 universe
* Add pipeline run logging (observability)
* Schedule automated daily refresh
* Deploy PostgreSQL to cloud (Neon / Supabase)
* Add alerting for top-ranked stocks
* Incorporate additional financial signals

---

## 💡 Key Takeaways

* Designed a **modular ETL pipeline**
* Implemented **idempotent database loading**
* Built a **time-series feature engineering system**
* Created an **interactive analytics dashboard**
* Simulated a **real-world data engineering workflow**

---

## 📌 Why This Project Matters

This project demonstrates the ability to:

* design scalable data pipelines
* work with relational databases and time-series data
* transform raw data into actionable insights
* deliver results through a user-facing application

It reflects skills directly applicable to **data engineering and data analyst roles**.
