import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Stock Ranking Dashboard", layout="wide")

st.title("Stock Ranking Dashboard")
st.caption("Daily-ranked stock and ETF candidates from PostgreSQL")

# -----------------------------
# Database connection
# -----------------------------
@st.cache_resource
def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME")

    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_url)


@st.cache_data(ttl=300)
def load_rankings():
    query = """
        SELECT
            ticker,
            ranking_date,
            current_price,
            ma20,
            ma50,
            volatility_30d,
            return_5d,
            return_20d,
            above_ma20,
            above_ma50,
            trend_score,
            momentum_score,
            volatility_penalty,
            ranking_score
        FROM stock_rankings
        ORDER BY ranking_score DESC
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


df = load_rankings()

if df.empty:
    st.warning("No ranking data found in stock_rankings.")
    st.stop()

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.header("Filters")

exclude_leveraged = st.sidebar.checkbox("Exclude leveraged / inverse ETFs", value=True)

max_price = st.sidebar.slider(
    "Max price",
    min_value=1.0,
    max_value=float(max(df["current_price"].max(), 10)),
    value=float(max(df["current_price"].max(), 10))
)

min_score = st.sidebar.slider(
    "Minimum ranking score",
    min_value=float(df["ranking_score"].min()),
    max_value=float(df["ranking_score"].max()),
    value=float(df["ranking_score"].min())
)

top_n = st.sidebar.slider("Top rows", min_value=5, max_value=50, value=15)

leveraged_inverse = {"TQQQ", "SQQQ", "UPRO", "SPXU", "SOXL", "SOXS", "NVDL", "TSLL"}

filtered_df = df.copy()

if exclude_leveraged:
    filtered_df = filtered_df[~filtered_df["ticker"].isin(leveraged_inverse)]

filtered_df = filtered_df[
    (filtered_df["current_price"] <= max_price) &
    (filtered_df["ranking_score"] >= min_score)
].copy()

filtered_df = filtered_df.sort_values("ranking_score", ascending=False).head(top_n)

# -----------------------------
# Top table
# -----------------------------
st.subheader("Top Ranked Names")

display_cols = [
    "ticker",
    "ranking_date",
    "current_price",
    "return_5d",
    "return_20d",
    "volatility_30d",
    "ranking_score"
]

st.dataframe(
    filtered_df[display_cols],
    use_container_width=True,
    hide_index=True
)

# -----------------------------
# Ticker detail
# -----------------------------
st.subheader("Ticker Detail")

ticker_list = filtered_df["ticker"].tolist() if not filtered_df.empty else df["ticker"].tolist()
selected_ticker = st.selectbox("Select a ticker", ticker_list)

detail_df = df[df["ticker"] == selected_ticker].sort_values("ranking_date", ascending=False).head(1)

if not detail_df.empty:
    row = detail_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Current Price", f"{row['current_price']:.2f}")
    col2.metric("Ranking Score", f"{row['ranking_score']:.2f}")
    col3.metric("5D Return", f"{row['return_5d']:.2%}")
    col4.metric("20D Return", f"{row['return_20d']:.2%}")

    st.write("### Trend / Risk Breakdown")
    st.dataframe(
        pd.DataFrame([{
            "ticker": row["ticker"],
            "ranking_date": row["ranking_date"],
            "ma20": row["ma20"],
            "ma50": row["ma50"],
            "above_ma20": row["above_ma20"],
            "above_ma50": row["above_ma50"],
            "trend_score": row["trend_score"],
            "momentum_score": row["momentum_score"],
            "volatility_penalty": row["volatility_penalty"],
            "volatility_30d": row["volatility_30d"]
        }]),
        use_container_width=True,
        hide_index=True
    )