import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Stock Ranking Dashboard", layout="wide")

st.title("Stock Ranking Dashboard")
st.caption("Daily-ranked stock and ETF candidates from PostgreSQL")

LEVERAGED_INVERSE = {"TQQQ", "SQQQ", "UPRO", "SPXU", "SOXL", "SOXS", "NVDL", "TSLL"}


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

    if not df.empty:
        df["ranking_date"] = pd.to_datetime(df["ranking_date"])
    return df


@st.cache_data(ttl=300)
def load_ticker_history(ticker: str):
    query = """
        SELECT
            date,
            close,
            ma20,
            ma50
        FROM stock_features
        WHERE ticker = :ticker
        ORDER BY date
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"ticker": ticker})

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# -----------------------------
# Refresh button
# -----------------------------
col_refresh_left, col_refresh_right = st.columns([8, 1])
with col_refresh_right:
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()


df = load_rankings()

if df.empty:
    st.warning("No ranking data found in stock_rankings.")
    st.stop()

latest_date = df["ranking_date"].max().date()

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.header("Filters")

exclude_leveraged = st.sidebar.checkbox("Exclude leveraged / inverse ETFs", value=True)

max_price = st.sidebar.slider(
    "Maximum stock price",
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

filtered_df = df.copy()

if exclude_leveraged:
    filtered_df = filtered_df[~filtered_df["ticker"].isin(LEVERAGED_INVERSE)]

filtered_df = filtered_df[
    (filtered_df["current_price"] <= max_price) &
    (filtered_df["ranking_score"] >= min_score)
].copy()

filtered_df = filtered_df.sort_values("ranking_score", ascending=False).head(top_n)

# -----------------------------
# Top metric cards
# -----------------------------
st.subheader("Top Ranked Snapshot")

top_cards = filtered_df.head(3).copy()
card_cols = st.columns(3)

for i, (_, r) in enumerate(top_cards.iterrows()):
    with card_cols[i]:
        st.metric(
            label=f"{r['ticker']} | Score",
            value=f"{r['ranking_score']:.2f}",
            delta=f"20D Return: {r['return_20d']:.2%}"
        )
        st.caption(f"Price: ${r['current_price']:.2f}")

# -----------------------------
# Tabs
# -----------------------------
tab1, tab2, tab3 = st.tabs(["Leaderboard", "Ticker Detail", "Price Chart"])

# -----------------------------
# Leaderboard tab
# -----------------------------
with tab1:
    st.subheader("Top Ranked Names")

    display_df = filtered_df.copy()
    display_df["ranking_date"] = display_df["ranking_date"].dt.date

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
        display_df[display_cols],
        width='stretch',
        hide_index=True,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker"),
            "ranking_date": st.column_config.DateColumn("Ranking Date"),
            "current_price": st.column_config.NumberColumn("Current Price", format="$%.2f"),
            "return_5d": st.column_config.NumberColumn("5D Return", format="%.2f%%"),
            "return_20d": st.column_config.NumberColumn("20D Return", format="%.2f%%"),
            "volatility_30d": st.column_config.NumberColumn("30D Volatility", format="%.2f%%"),
            "ranking_score": st.column_config.NumberColumn("Ranking Score", format="%.2f"),
        }
    )

# -----------------------------
# Shared ticker selection
# -----------------------------
ticker_list = filtered_df["ticker"].tolist() if not filtered_df.empty else df["ticker"].tolist()
selected_ticker = st.selectbox("Select a ticker", ticker_list)

detail_df = df[df["ticker"] == selected_ticker].sort_values("ranking_date", ascending=False).head(1)
history_df = load_ticker_history(selected_ticker)

# -----------------------------
# Ticker detail tab
# -----------------------------
with tab2:
    st.subheader("Ticker Detail")

    if not detail_df.empty:
        row = detail_df.iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Current Price", f"${row['current_price']:.2f}")
        col2.metric("Ranking Score", f"{row['ranking_score']:.2f}")
        col3.metric("5D Return", f"{row['return_5d']:.2%}")
        col4.metric("20D Return", f"{row['return_20d']:.2%}")

        st.write("### Trend / Risk Breakdown")
        breakdown_df = pd.DataFrame([{
            "ticker": row["ticker"],
            "ranking_date": row["ranking_date"].date(),
            "ma20": row["ma20"],
            "ma50": row["ma50"],
            "above_ma20": row["above_ma20"],
            "above_ma50": row["above_ma50"],
            "trend_score": row["trend_score"],
            "momentum_score": row["momentum_score"],
            "volatility_penalty": row["volatility_penalty"],
            "volatility_30d": row["volatility_30d"]
        }])

        st.dataframe(
            breakdown_df,
            width='stretch',
            hide_index=True,
            column_config={
                "ma20": st.column_config.NumberColumn("MA20", format="$%.2f"),
                "ma50": st.column_config.NumberColumn("MA50", format="$%.2f"),
                "trend_score": st.column_config.NumberColumn("Trend Score", format="%.2f"),
                "momentum_score": st.column_config.NumberColumn("Momentum Score", format="%.2f"),
                "volatility_penalty": st.column_config.NumberColumn("Volatility Penalty", format="%.2f"),
                "volatility_30d": st.column_config.NumberColumn("30D Volatility", format="%.2f%%"),
            }
        )

        st.write("### Why It Ranked Here")
        reasons = []

        if bool(row["above_ma20"]):
            reasons.append("Trading above MA20")
        else:
            reasons.append("Trading below MA20")

        if bool(row["above_ma50"]):
            reasons.append("Trading above MA50")
        else:
            reasons.append("Trading below MA50")

        if pd.notna(row["return_5d"]):
            if row["return_5d"] > 0:
                reasons.append(f"Positive 5-day momentum ({row['return_5d']:.2%})")
            else:
                reasons.append(f"Negative 5-day momentum ({row['return_5d']:.2%})")

        if pd.notna(row["return_20d"]):
            if row["return_20d"] > 0:
                reasons.append(f"Positive 20-day momentum ({row['return_20d']:.2%})")
            else:
                reasons.append(f"Negative 20-day momentum ({row['return_20d']:.2%})")

        if pd.notna(row["volatility_30d"]):
            if row["volatility_30d"] < 0.02:
                reasons.append("Low recent volatility")
            elif row["volatility_30d"] < 0.04:
                reasons.append("Moderate recent volatility")
            else:
                reasons.append("Higher recent volatility")

        for reason in reasons:
            st.write(f"- {reason}")

# -----------------------------
# Price chart tab
# -----------------------------
with tab3:
    st.subheader(f"{selected_ticker} Price Chart")

    if history_df.empty:
        st.warning(f"No price history found for {selected_ticker}.")
    else:
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=history_df["date"],
                y=history_df["close"],
                mode="lines",
                name="Close"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=history_df["date"],
                y=history_df["ma20"],
                mode="lines",
                name="MA20"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=history_df["date"],
                y=history_df["ma50"],
                mode="lines",
                name="MA50"
            )
        )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Price",
            hovermode="x unified",
            height=500,
            margin=dict(l=20, r=20, t=20, b=20)
        )

        st.plotly_chart(fig, width='stretch')

        st.write("### Recent Price Data")
        chart_df = history_df.copy()
        chart_df["date"] = chart_df["date"].dt.date

        st.dataframe(
            chart_df.tail(20),
            width='stretch',
            hide_index=True,
            column_config={
                "close": st.column_config.NumberColumn("Close", format="$%.2f"),
                "ma20": st.column_config.NumberColumn("MA20", format="$%.2f"),
                "ma50": st.column_config.NumberColumn("MA50", format="$%.2f"),
            }
        )

st.caption(f"Latest ranking date: {latest_date} | Total ranked names: {len(df)}")