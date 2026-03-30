"""
Stock Ranking Dashboard — Streamlit frontend.
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import text

from utils.db import get_engine
from utils.config import TICKER_SECTORS

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Ranking Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    .section-divider {
        border: none;
        border-top: 1px solid rgba(255,255,255,0.08);
        margin: 1rem 0;
    }

    [data-testid="metric-container"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 8px;
        padding: 0.75rem 1rem;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid rgba(255,255,255,0.08);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0;
        padding: 0.4rem 1rem;
    }

    .reason-positive {
        padding: 0.3rem 0;
        padding-left: 1rem;
        border-left: 2px solid rgba(75, 200, 120, 0.6);
        margin-bottom: 0.4rem;
        font-size: 0.9rem;
        color: rgba(255,255,255,0.85);
    }

    .reason-negative {
        padding: 0.3rem 0;
        padding-left: 1rem;
        border-left: 2px solid rgba(255, 75, 75, 0.5);
        margin-bottom: 0.4rem;
        font-size: 0.9rem;
        color: rgba(255,255,255,0.85);
    }

    .reason-neutral {
        padding: 0.3rem 0;
        padding-left: 1rem;
        border-left: 2px solid rgba(255, 183, 75, 0.5);
        margin-bottom: 0.4rem;
        font-size: 0.9rem;
        color: rgba(255,255,255,0.85);
    }
</style>
""", unsafe_allow_html=True)

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
def load_spy_metrics():
    """Fetch SPY's latest return_20d and MA50 position for regime + relative strength."""
    query = """
        SELECT return_20d, above_ma50
        FROM stock_features
        WHERE ticker = 'SPY'
        ORDER BY date DESC
        LIMIT 1
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


@st.cache_data(ttl=300)
def load_ticker_history(ticker: str):
    query = """
        SELECT date, close, volume, ma20, ma50
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
    if st.button("Refresh", width="stretch"):
        st.cache_data.clear()
        st.rerun()

st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_rankings()

if df.empty:
    st.warning("No ranking data found in stock_rankings.")
    st.stop()

latest_date = df["ranking_date"].max().date()

# ── SPY metrics (used for relative strength scoring) ──────────────────────────
spy = load_spy_metrics()

if not spy.empty:
    spy_return_20d = float(spy["return_20d"].iloc[0]) if pd.notna(spy["return_20d"].iloc[0]) else 0.0
else:
    spy_return_20d = 0.0

# ── Normalise score components to [0, 1] ──────────────────────────────────────
# Done once on the full dataset so scores are comparable across all stocks
# regardless of which filters are applied later.
#
# trend_score:       discrete 0–40  → divide by max possible (40)
# volatility_penalty 0–50           → divide by max possible (50)
# momentum_score:    unbounded      → min-max scale across all loaded stocks

df["trend_norm"] = (df["trend_score"] / 40).clip(0, 1)

mom_min, mom_max = df["momentum_score"].min(), df["momentum_score"].max()
if mom_max > mom_min:
    df["momentum_norm"] = ((df["momentum_score"] - mom_min) / (mom_max - mom_min)).clip(0, 1)
else:
    df["momentum_norm"] = 0.0

df["volatility_norm"] = (df["volatility_penalty"] / 50).clip(0, 1)

# ── Mean reversion signal ─────────────────────────────────────────────────────
# Measures how far price has stretched from MA20.
# Peaks at ~3% above MA20 (slight extension in uptrend = ideal entry).
# Falls off toward 0 as price becomes overextended (>15%) or drops far below MA20.
df["ma20_distance"] = ((df["current_price"] - df["ma20"]) / df["ma20"]).fillna(0)
df["mean_reversion_norm"] = (
    df["ma20_distance"]
    .apply(lambda x: max(0.0, 1 - abs(x - 0.03) / 0.12))
    .clip(0, 1)
)

# ── Relative strength vs SPY ──────────────────────────────────────────────────
# Measures whether the stock is outperforming or underperforming the market.
# Normalised across all stocks so the best relative performer scores 1.0.
df["relative_strength"] = df["return_20d"].fillna(0) - spy_return_20d
rs_min, rs_max = df["relative_strength"].min(), df["relative_strength"].max()
if rs_max > rs_min:
    df["rs_norm"] = ((df["relative_strength"] - rs_min) / (rs_max - rs_min)).clip(0, 1)
else:
    df["rs_norm"] = 0.5

# Blend new signals into existing normalised components.
# Mean reversion enhances trend (rewards optimal MA distance, not just direction).
# Relative strength enhances momentum (filters out market-wide tailwinds).
df["trend_norm"]    = (0.65 * df["trend_norm"]    + 0.35 * df["mean_reversion_norm"])
df["momentum_norm"] = (0.65 * df["momentum_norm"] + 0.35 * df["rs_norm"])

# ── Weight slider state ────────────────────────────────────────────────────────
_WEIGHT_KEYS = ["trend_pct", "momentum_pct", "vol_pct"]
_WEIGHT_DEFAULTS = {"trend_pct": 30, "momentum_pct": 50, "vol_pct": 20}

for _k, _v in _WEIGHT_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


def _redistribute_weights(changed_key: str) -> None:
    """
    When one weight slider moves, proportionally rescale the other two
    so all three always sum to exactly 100.
    """
    new_val = st.session_state[changed_key]
    other_keys = [k for k in _WEIGHT_KEYS if k != changed_key]
    remaining = 100 - new_val

    other_total = sum(st.session_state[k] for k in other_keys)

    if other_total == 0:
        # Both others are at 0 — split the remainder evenly
        share = remaining // len(other_keys)
        for k in other_keys:
            st.session_state[k] = share
        st.session_state[other_keys[0]] += remaining - share * len(other_keys)
    else:
        # Redistribute proportionally, then fix any rounding drift
        new_vals = {
            k: round(st.session_state[k] / other_total * remaining)
            for k in other_keys
        }
        drift = remaining - sum(new_vals.values())
        if drift != 0:
            largest = max(other_keys, key=lambda k: new_vals[k])
            new_vals[largest] += drift
        for k, v in new_vals.items():
            st.session_state[k] = max(0, v)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    max_price = st.slider(
        "Maximum stock price",
        min_value=1.0,
        max_value=float(max(df["current_price"].max(), 10)),
        value=float(max(df["current_price"].max(), 10)),
    )

    all_sectors = sorted({s for s in TICKER_SECTORS.values()})
    sector_choice = st.selectbox("Sector", options=["All Sectors"] + all_sectors)

    uptrend_only = st.checkbox("Uptrend only (above MA20 & MA50)", value=False)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    st.subheader("Ranking Weights")
    st.caption("Adjusting one slider automatically rebalances the others to keep the total at 100%.")

    st.slider("Trend",              0, 100, step=5, format="%d%%",
              key="trend_pct",
              on_change=_redistribute_weights, args=("trend_pct",))
    st.slider("Momentum",           0, 100, step=5, format="%d%%",
              key="momentum_pct",
              on_change=_redistribute_weights, args=("momentum_pct",))
    st.slider("Volatility Penalty", 0, 100, step=5, format="%d%%",
              key="vol_pct",
              on_change=_redistribute_weights, args=("vol_pct",))

    trend_w      = st.session_state["trend_pct"]    / 100
    momentum_w   = st.session_state["momentum_pct"] / 100
    volatility_w = st.session_state["vol_pct"]      / 100

# ── Apply filters & recompute score ───────────────────────────────────────────
filtered_df = df.copy()

# Recompute ranking score using normalised components and current weights.
# Score is in [0, 1] range (before volatility drag), so moving any slider
# produces a meaningful change in the final ranking.
filtered_df["ranking_score"] = (
    (
        filtered_df["trend_norm"]      * trend_w
        + filtered_df["momentum_norm"] * momentum_w
        - filtered_df["volatility_norm"] * volatility_w
    ) * 100
).round(2)

with st.sidebar:
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    min_score = st.slider(
        "Minimum ranking score",
        min_value=float(filtered_df["ranking_score"].min()),
        max_value=float(filtered_df["ranking_score"].max()),
        value=float(filtered_df["ranking_score"].min()),
        format="%.2f",
    )

filtered_df["sector"] = filtered_df["ticker"].map(TICKER_SECTORS)

filtered_df = filtered_df[
    (filtered_df["current_price"] <= max_price)
    & (filtered_df["ranking_score"] >= min_score)
    & (filtered_df["sector"].isin(all_sectors if sector_choice == "All Sectors" else [sector_choice]))
]

if uptrend_only:
    filtered_df = filtered_df[
        filtered_df["above_ma20"].fillna(False)
        & filtered_df["above_ma50"].fillna(False)
    ]

filtered_df = filtered_df.sort_values("ranking_score", ascending=False).copy()

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

# ── Ticker selector ───────────────────────────────────────────────────────────
ticker_list     = filtered_df["ticker"].tolist() or df["ticker"].tolist()
selected_ticker = st.selectbox(
    "Select a ticker to explore →",
    ticker_list,
    label_visibility="visible",
)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["Leaderboard", "Ticker Detail", "Price Chart", "Sector Breakdown"])

# ── Leaderboard ───────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Top Ranked Stocks")

    display_df = filtered_df.copy()
    display_df["ranking_date"]  = display_df["ranking_date"].dt.date
    display_df["return_5d"]      = display_df["return_5d"]      * 100
    display_df["return_20d"]     = display_df["return_20d"]     * 100
    display_df["volatility_30d"] = display_df["volatility_30d"] * 100

    st.dataframe(
        display_df[[
            "ticker", "ranking_date", "current_price",
            "return_5d", "return_20d", "volatility_30d", "ranking_score"
        ]],
        width="stretch",
        hide_index=True,
        column_config={
            "ticker":         st.column_config.TextColumn("Ticker"),
            "ranking_date":   st.column_config.DateColumn("Date"),
            "current_price":  st.column_config.NumberColumn("Price",      format="$%.2f"),
            "return_5d":      st.column_config.NumberColumn("5D Return",  format="%.2f%%"),
            "return_20d":     st.column_config.NumberColumn("20D Return", format="%.2f%%"),
            "volatility_30d": st.column_config.NumberColumn("30D Vol",    format="%.2f%%"),
            "ranking_score":  st.column_config.NumberColumn("Score",      format="%.2f"),
        },
    )

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
        m1.metric("Current Price", f"${row['current_price']:.2f}")
        m2.metric("Ranking Score", f"{row['ranking_score']:.2f}")
        m3.metric("5D Return",     f"{row['return_5d']*100:.2f}%")
        m4.metric("20D Return",    f"{row['return_20d']*100:.2f}%")

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # ── Score breakdown bar chart ──────────────────────────────────────────
        st.markdown("**Score Breakdown**")
        st.caption("Weighted contribution of each normalised signal to the final ranking score.")

        trend_contrib      = row["trend_norm"]      * trend_w      * 100
        momentum_contrib   = row["momentum_norm"]   * momentum_w   * 100
        volatility_contrib = row["volatility_norm"] * volatility_w * 100

        fig_breakdown = go.Figure()

        fig_breakdown.add_trace(go.Bar(
            name="Trend",
            x=[trend_contrib],
            y=["Score"],
            orientation="h",
            marker_color="#4B9EFF",
            text=[f"{trend_contrib:.2f}"],
            textposition="inside",
        ))
        fig_breakdown.add_trace(go.Bar(
            name="Momentum",
            x=[momentum_contrib],
            y=["Score"],
            orientation="h",
            marker_color="#4BCC80",
            text=[f"{momentum_contrib:.2f}"],
            textposition="inside",
        ))
        fig_breakdown.add_trace(go.Bar(
            name="Volatility Drag",
            x=[-volatility_contrib],
            y=["Score"],
            orientation="h",
            marker_color="#FF4B4B",
            text=[f"-{volatility_contrib:.2f}"],
            textposition="inside",
        ))

        fig_breakdown.update_layout(
            barmode="relative",
            height=120,
            margin=dict(l=0, r=0, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                showgrid=True,
                gridcolor="rgba(255,255,255,0.05)",
                zeroline=True,
                zerolinecolor="rgba(255,255,255,0.15)",
            ),
            yaxis=dict(showticklabels=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="left", x=0),
        )

        st.plotly_chart(fig_breakdown, width="stretch")

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # ── MA levels & raw scores table ───────────────────────────────────────
        st.markdown("**Moving Averages & Raw Scores**")

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
            width="stretch",
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

        # ── Why It Ranked Here ─────────────────────────────────────────────────
        st.markdown("**Why It Ranked Here**")

        reasons = []
        reasons.append(("Above MA20", "positive") if bool(row["above_ma20"]) else ("Below MA20", "negative"))
        reasons.append(("Above MA50", "positive") if bool(row["above_ma50"]) else ("Below MA50", "negative"))

        if pd.notna(row["return_5d"]):
            icon      = "▲" if row["return_5d"] > 0 else "▼"
            sentiment = "positive" if row["return_5d"] > 0 else "negative"
            reasons.append((f"{icon} 5-day momentum: {row['return_5d']*100:.2f}%", sentiment))

        if pd.notna(row["return_20d"]):
            icon      = "▲" if row["return_20d"] > 0 else "▼"
            sentiment = "positive" if row["return_20d"] > 0 else "negative"
            reasons.append((f"{icon} 20-day momentum: {row['return_20d']*100:.2f}%", sentiment))

        if pd.notna(row["volatility_30d"]):
            if row["volatility_30d"] < 0.02:
                reasons.append(("Low recent volatility", "positive"))
            elif row["volatility_30d"] < 0.04:
                reasons.append(("Moderate recent volatility", "neutral"))
            else:
                reasons.append(("Higher recent volatility", "negative"))

        for text, sentiment in reasons:
            st.markdown(f'<div class="reason-{sentiment}">{text}</div>', unsafe_allow_html=True)

# ── Price chart ───────────────────────────────────────────────────────────────
with tab3:
    if history_df.empty:
        st.warning(f"No price history found for {selected_ticker}.")
    else:
        st.subheader(f"{selected_ticker} — Price Chart")

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.75, 0.25],
        )

        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["close"],
            mode="lines", name="Close",
            line=dict(color="#FF4B4B", width=2),
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["ma20"],
            mode="lines", name="MA20",
            line=dict(color="#4B9EFF", width=1.5, dash="dot"),
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=history_df["date"], y=history_df["ma50"],
            mode="lines", name="MA50",
            line=dict(color="#FFB74B", width=1.5, dash="dash"),
        ), row=1, col=1)

        fig.add_trace(go.Bar(
            x=history_df["date"], y=history_df["volume"],
            name="Volume",
            marker_color="rgba(255,255,255,0.12)",
            showlegend=False,
        ), row=2, col=1)

        fig.update_layout(
            hovermode="x unified",
            height=520,
            margin=dict(l=10, r=10, t=20, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
        fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
        fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
        fig.update_yaxes(title_text="Volume",      row=2, col=1)

        st.plotly_chart(fig, width="stretch")

        st.markdown("**Recent Price Data**")
        chart_df = history_df.copy()
        chart_df["date"] = chart_df["date"].dt.date

        st.dataframe(
            chart_df.tail(20)[["date", "close", "ma20", "ma50"]],
            width="stretch",
            hide_index=True,
            column_config={
                "date":  st.column_config.DateColumn("Date"),
                "close": st.column_config.NumberColumn("Close", format="$%.2f"),
                "ma20":  st.column_config.NumberColumn("MA20",  format="$%.2f"),
                "ma50":  st.column_config.NumberColumn("MA50",  format="$%.2f"),
            },
        )

# ── Sector breakdown ──────────────────────────────────────────────────────────
with tab4:
    st.subheader("Sector Breakdown")
    st.caption("Average ranking score and ticker count per sector, based on the current filtered set.")

    # Map each ticker to its sector, drop any without a mapping
    sector_df = filtered_df.copy()
    sector_df["sector"] = sector_df["ticker"].map(TICKER_SECTORS)
    sector_df = sector_df.dropna(subset=["sector"])

    if sector_df.empty:
        st.info("No sector data available for the current filter selection.")
    else:
        agg = (
            sector_df.groupby("sector")
            .agg(
                avg_score=("ranking_score", "mean"),
                avg_return_5d=("return_5d", "mean"),
                avg_return_20d=("return_20d", "mean"),
                ticker_count=("ticker", "count"),
                tickers=("ticker", lambda x: ", ".join(sorted(x))),
            )
            .reset_index()
            .sort_values("avg_score", ascending=True)  # ascending for horizontal bar readability
        )

        # Color bars by score — low scores red, high scores green
        max_score = agg["avg_score"].max()
        min_score_sec = agg["avg_score"].min()
        score_range = max_score - min_score_sec if max_score != min_score_sec else 1

        bar_colors = [
            f"rgba({int(255 * (1 - (s - min_score_sec) / score_range))}, "
            f"{int(200 * ((s - min_score_sec) / score_range))}, "
            f"80, 0.8)"
            for s in agg["avg_score"]
        ]

        fig_sector = go.Figure(go.Bar(
            x=agg["avg_score"].round(2),
            y=agg["sector"],
            orientation="h",
            marker_color=bar_colors,
            text=[f"{s:.1f}  ({c} ticker{'s' if c != 1 else ''})" for s, c in zip(agg["avg_score"], agg["ticker_count"])],
            textposition="outside",
            customdata=agg[["avg_return_5d", "avg_return_20d", "tickers"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Avg Score: %{x:.2f}<br>"
                "Avg 5D Return: %{customdata[0]:.2%}<br>"
                "Avg 20D Return: %{customdata[1]:.2%}<br>"
                "Tickers: %{customdata[2]}<extra></extra>"
            ),
        ))

        fig_sector.update_layout(
            height=max(300, len(agg) * 55),
            margin=dict(l=10, r=80, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                title="Avg Ranking Score",
                showgrid=True,
                gridcolor="rgba(255,255,255,0.05)",
            ),
            yaxis=dict(showgrid=False),
        )

        st.plotly_chart(fig_sector, width="stretch")

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # Summary table with avg returns per sector
        summary_df = agg[["sector", "ticker_count", "avg_score", "avg_return_5d", "avg_return_20d"]].copy()
        summary_df["avg_return_5d"]  *= 100
        summary_df["avg_return_20d"] *= 100
        summary_df = summary_df.sort_values("avg_score", ascending=False)

        st.dataframe(
            summary_df,
            width="stretch",
            hide_index=True,
            column_config={
                "sector":         st.column_config.TextColumn("Sector"),
                "ticker_count":   st.column_config.NumberColumn("Tickers", format="%d"),
                "avg_score":      st.column_config.NumberColumn("Avg Score",     format="%.2f"),
                "avg_return_5d":  st.column_config.NumberColumn("Avg 5D Return", format="%.2f%%"),
                "avg_return_20d": st.column_config.NumberColumn("Avg 20D Return", format="%.2f%%"),
            },
        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.caption(f"Latest ranking date: **{latest_date}**  ·  Total ranked names: **{len(df)}**")
