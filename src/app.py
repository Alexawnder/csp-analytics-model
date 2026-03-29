"""
Stock Ranking Dashboard — Streamlit frontend.
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlalchemy import text

from utils.db import get_engine

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Ranking Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Tighten top padding */
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    /* Section divider */
    .section-divider {
        border: none;
        border-top: 1px solid rgba(255,255,255,0.08);
        margin: 1rem 0;
    }

    /* Metric card tweaks */
    [data-testid="metric-container"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 8px;
        padding: 0.75rem 1rem;
    }

    /* Subtle tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid rgba(255,255,255,0.08);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0;
        padding: 0.4rem 1rem;
    }

    /* Reason bullets */
    .reason-item {
        padding: 0.3rem 0;
        padding-left: 1rem;
        border-left: 2px solid rgba(255, 75, 75, 0.4);
        margin-bottom: 0.4rem;
        font-size: 0.9rem;
        color: rgba(255,255,255,0.85);
    }

    /* Score badge */
    .score-badge {
        display: inline-block;
        background: rgba(255,75,75,0.15);
        border: 1px solid rgba(255,75,75,0.3);
        border-radius: 4px;
        padding: 0.1rem 0.5rem;
        font-size: 0.8rem;
        color: rgba(255,255,255,0.7);
    }
</style>
""", unsafe_allow_html=True)

LEVERAGED_INVERSE = {"TQQQ", "SQQQ", "UPRO", "SPXU", "SOXL", "SOXS", "NVDL", "TSLL"}

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_resource
def _get_engine():
    return get_engine()


@st.cache_data(ttl=300)
def load_rankings():
    query = """
        SELECT ticker, ranking_date, current_price, ma20, ma50,
               volatility_30d, return_5d, return_20d, above_ma20, above_ma50,
               trend_score, momentum_score, volatility_penalty, ranking_score
        FROM stock_rankings
        ORDER BY ranking_score DESC
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn)
    if not df.empty:
        df["ranking_date"] = pd.to_datetime(df["ranking_date"])
    return df


@st.cache_data(ttl=300)
def load_ticker_history(ticker: str):
    query = """
        SELECT date, close, ma20, ma50
        FROM stock_features
        WHERE ticker = :ticker
        ORDER BY date
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn, params={"ticker": ticker})
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_refresh = st.columns([9, 1])
with col_title:
    st.title("Stock Ranking Dashboard")
    st.caption("Daily-ranked equities scored by trend, momentum, and volatility")
with col_refresh:
    st.write("")
    if st.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_rankings()

if df.empty:
    st.warning("No ranking data found in stock_rankings.")
    st.stop()

latest_date = df["ranking_date"].max().date()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    exclude_leveraged = st.checkbox("Exclude leveraged / inverse ETFs", value=True)

    max_price = st.slider(
        "Maximum stock price",
        min_value=1.0,
        max_value=float(max(df["current_price"].max(), 10)),
        value=float(max(df["current_price"].max(), 10)),
    )

    top_n = st.slider("Top N results", min_value=5, max_value=50, value=15)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    st.subheader("Ranking Weights")
    st.caption("Values are normalised to sum to 1.")

    trend_w    = st.slider("Trend",              0.0, 1.0, 0.3, 0.05)
    momentum_w = st.slider("Momentum",           0.0, 1.0, 0.5, 0.05)
    volatility_w = st.slider("Volatility Penalty", 0.0, 1.0, 0.2, 0.05)

    total = trend_w + momentum_w + volatility_w
    if total > 0:
        trend_w /= total; momentum_w /= total; volatility_w /= total
    else:
        trend_w, momentum_w, volatility_w = 0.3, 0.5, 0.2

    st.info(f"**Trend** {trend_w:.2f}  ·  **Momentum** {momentum_w:.2f}  ·  **Volatility** {volatility_w:.2f}")

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered_df = df.copy()

filtered_df["ranking_score"] = (
    filtered_df["trend_score"].fillna(0)          * trend_w
    + filtered_df["momentum_score"].fillna(0)     * momentum_w
    - filtered_df["volatility_penalty"].fillna(0) * volatility_w
).round(4)

with st.sidebar:
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    min_score = st.slider(
        "Minimum ranking score",
        min_value=float(filtered_df["ranking_score"].min()),
        max_value=float(filtered_df["ranking_score"].max()),
        value=float(filtered_df["ranking_score"].min()),
    )

if exclude_leveraged:
    filtered_df = filtered_df[~filtered_df["ticker"].isin(LEVERAGED_INVERSE)]

filtered_df = filtered_df[
    (filtered_df["current_price"] <= max_price)
    & (filtered_df["ranking_score"] >= min_score)
].sort_values("ranking_score", ascending=False).head(top_n).copy()

# ── Top 3 snapshot ────────────────────────────────────────────────────────────
st.subheader("Top Ranked Snapshot")
top3 = filtered_df.head(3)
cols = st.columns(3)

for i, (_, r) in enumerate(top3.iterrows()):
    with cols[i]:
        st.metric(
            label=f"#{i+1}  {r['ticker']}",
            value=f"{r['ranking_score']:.2f}",
            delta=f"20D: {r['return_20d']*100:.2f}%",
        )
        st.caption(f"Price: **${r['current_price']:.2f}**  ·  5D: {r['return_5d']*100:.2f}%")

st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Leaderboard", "Ticker Detail", "Price Chart"])

# ── Leaderboard ───────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Top Ranked Stocks")

    display_df = filtered_df.copy()
    display_df["ranking_date"] = display_df["ranking_date"].dt.date
    # Multiply by 100 so format string "%.2f%%" renders correctly
    display_df["return_5d"]      = display_df["return_5d"]      * 100
    display_df["return_20d"]     = display_df["return_20d"]     * 100
    display_df["volatility_30d"] = display_df["volatility_30d"] * 100

    st.dataframe(
        display_df[[
            "ticker", "ranking_date", "current_price",
            "return_5d", "return_20d", "volatility_30d", "ranking_score"
        ]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "ticker":         st.column_config.TextColumn("Ticker"),
            "ranking_date":   st.column_config.DateColumn("Date"),
            "current_price":  st.column_config.NumberColumn("Price",        format="$%.2f"),
            "return_5d":      st.column_config.NumberColumn("5D Return",    format="%.2f%%"),
            "return_20d":     st.column_config.NumberColumn("20D Return",   format="%.2f%%"),
            "volatility_30d": st.column_config.NumberColumn("30D Vol",      format="%.2f%%"),
            "ranking_score":  st.column_config.NumberColumn("Score",        format="%.2f"),
        },
    )

# ── Shared ticker selector ────────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
ticker_list     = filtered_df["ticker"].tolist() or df["ticker"].tolist()
selected_ticker = st.selectbox("Select a ticker to inspect", ticker_list)

detail_df  = (
    filtered_df[filtered_df["ticker"] == selected_ticker]
    .sort_values("ranking_date", ascending=False)
    .head(1)
)
history_df = load_ticker_history(selected_ticker)

# ── Ticker detail ─────────────────────────────────────────────────────────────
with tab2:
    if detail_df.empty:
        st.info("Select a ticker from the leaderboard.")
    else:
        row = detail_df.iloc[0]

        st.subheader(f"{selected_ticker} — Detail View")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current Price",  f"${row['current_price']:.2f}")
        m2.metric("Ranking Score",  f"{row['ranking_score']:.2f}")
        m3.metric("5D Return",      f"{row['return_5d']*100:.2f}%")
        m4.metric("20D Return",     f"{row['return_20d']*100:.2f}%")

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown("**Trend & Risk Breakdown**")

        breakdown_df = pd.DataFrame([{
            "MA20":               row["ma20"],
            "MA50":               row["ma50"],
            "Above MA20":         bool(row["above_ma20"]),
            "Above MA50":         bool(row["above_ma50"]),
            "Trend Score":        row["trend_score"],
            "Momentum Score":     row["momentum_score"],
            "Volatility Penalty": row["volatility_penalty"],
            "30D Volatility":     row["volatility_30d"] * 100,
        }])

        st.dataframe(
            breakdown_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "MA20":               st.column_config.NumberColumn("MA20",               format="$%.2f"),
                "MA50":               st.column_config.NumberColumn("MA50",               format="$%.2f"),
                "Trend Score":        st.column_config.NumberColumn("Trend Score",        format="%.2f"),
                "Momentum Score":     st.column_config.NumberColumn("Momentum Score",     format="%.2f"),
                "Volatility Penalty": st.column_config.NumberColumn("Volatility Penalty", format="%.2f"),
                "30D Volatility":     st.column_config.NumberColumn("30D Volatility",     format="%.2f%%"),
            },
        )

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown("**Why It Ranked Here**")

        reasons = []
        reasons.append("Above MA20" if bool(row["above_ma20"]) else "Below MA20")
        reasons.append("Above MA50" if bool(row["above_ma50"]) else "Below MA50")

        if pd.notna(row["return_5d"]):
            icon = "▲" if row["return_5d"] > 0 else "▼"
            reasons.append(f"{icon} 5-day momentum: {row['return_5d']*100:.2f}%")

        if pd.notna(row["return_20d"]):
            icon = "▲" if row["return_20d"] > 0 else "▼"
            reasons.append(f"{icon} 20-day momentum: {row['return_20d']*100:.2f}%")

        if pd.notna(row["volatility_30d"]):
            if row["volatility_30d"] < 0.02:
                reasons.append("Low recent volatility")
            elif row["volatility_30d"] < 0.04:
                reasons.append("Moderate recent volatility")
            else:
                reasons.append("Higher recent volatility")

        for reason in reasons:
            st.markdown(f'<div class="reason-item">{reason}</div>', unsafe_allow_html=True)

# ── Price chart ───────────────────────────────────────────────────────────────
with tab3:
    if history_df.empty:
        st.warning(f"No price history found for {selected_ticker}.")
    else:
        st.subheader(f"{selected_ticker} — Price Chart")

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["close"],
            mode="lines", name="Close",
            line=dict(color="#FF4B4B", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["ma20"],
            mode="lines", name="MA20",
            line=dict(color="#4B9EFF", width=1.5, dash="dot")
        ))
        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["ma50"],
            mode="lines", name="MA50",
            line=dict(color="#FFB74B", width=1.5, dash="dash")
        ))

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Price (USD)",
            hovermode="x unified",
            height=460,
            margin=dict(l=10, r=10, t=20, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Recent Price Data**")
        chart_df = history_df.copy()
        chart_df["date"]  = chart_df["date"].dt.date
        chart_df["close"] = chart_df["close"]

        st.dataframe(
            chart_df.tail(20)[["date", "close", "ma20", "ma50"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "date":  st.column_config.DateColumn("Date"),
                "close": st.column_config.NumberColumn("Close", format="$%.2f"),
                "ma20":  st.column_config.NumberColumn("MA20",  format="$%.2f"),
                "ma50":  st.column_config.NumberColumn("MA50",  format="$%.2f"),
            },
        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.caption(f"Latest ranking date: **{latest_date}**  ·  Total ranked names: **{len(df)}**")