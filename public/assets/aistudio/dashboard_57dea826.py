"""Live dashboard:  streamlit run dashboard.py"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

import storage

st.set_page_config(page_title="Transit pulse", page_icon="🚌", layout="wide")

COLORS = {"positive": "#2a9d8f", "neutral": "#9aa5b1", "negative": "#e4572e"}
ORDER = ["positive", "neutral", "negative"]
DOT = {"positive": "🟢", "neutral": "⚪", "negative": "🔴"}

st.markdown(
    """
    <style>
    .block-container {padding-top: 2.2rem; max-width: 1300px;}
    [data-testid="stMetricValue"] {font-size: 2.1rem; font-weight: 650;}
    h1 {letter-spacing: -0.02em;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---- sidebar controls -------------------------------------------------------
WINDOWS = {"Last 5 minutes": 5, "Last 30 minutes": 30, "Last 2 hours": 120, "All data": None}
with st.sidebar:
    st.header("View")
    window_label = st.selectbox("Time window", list(WINDOWS), index=1)
    modes = st.multiselect("Transport mode", ["bus", "metro", "train"], default=["bus", "metro", "train"])
    refresh = st.slider("Refresh every (seconds)", 2, 30, 3)
    st.divider()
    st.caption(
        "Data path: producer, raw topic, processor, analyzed topic, sink, "
        "SQLite, this dashboard. Times are shown in UTC."
    )

st.title("Transit pulse")
st.caption("How passengers feel about the service, updated as feedback streams through Kafka.")


@st.fragment(run_every=refresh)
def live():
    minutes = WINDOWS[window_label]
    since = None
    if minutes:
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(timespec="seconds")
    df = storage.load_feedback(since, modes)

    if df.empty:
        st.info(
            "No feedback in this window yet. Start the pipeline in separate terminals: "
            "`python producer.py`, `python processor.py` and `python sink.py`."
        )
        return

    # ---- headline numbers ----------------------------------------------------
    share = df["sentiment"].value_counts(normalize=True)
    cols = st.columns(5)
    cols[0].metric("Messages", f"{len(df):,}")
    cols[1].metric("Positive", f"{share.get('positive', 0):.0%}")
    cols[2].metric("Neutral", f"{share.get('neutral', 0):.0%}")
    cols[3].metric("Negative", f"{share.get('negative', 0):.0%}")
    cols[4].metric(
        "Net sentiment", f"{df['score'].mean():+.2f}",
        help="Average score from -1 (everything negative) to +1 (everything positive).",
    )

    def tidy(fig, height=320):
        fig.update_layout(
            height=height, margin=dict(l=0, r=0, t=40, b=0), legend_title_text="",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        )
        return fig

    # ---- trend + overall split -----------------------------------------------
    span = df["timestamp"].max() - df["timestamp"].min()
    freq = "15s" if span <= pd.Timedelta(minutes=10) else "1min" if span <= pd.Timedelta(hours=3) else "10min"
    trend = (
        df.assign(bucket=df["timestamp"].dt.floor(freq))
        .groupby(["bucket", "sentiment"]).size().reset_index(name="messages")
    )
    left, right = st.columns([3, 1.4])
    with left:
        fig = px.line(
            trend, x="bucket", y="messages", color="sentiment", markers=True,
            color_discrete_map=COLORS, category_orders={"sentiment": ORDER},
            title="Messages over time", labels={"bucket": "", "messages": ""},
        )
        st.plotly_chart(tidy(fig))
    with right:
        fig = px.pie(
            df, names="sentiment", hole=0.55, color="sentiment",
            color_discrete_map=COLORS, category_orders={"sentiment": ORDER},
            title="Overall mood",
        )
        fig.update_traces(textinfo="percent", sort=False)
        st.plotly_chart(tidy(fig))

    # ---- what people talk about + which routes suffer ----------------------------
    left, right = st.columns(2)
    with left:
        topics = df.groupby(["topic", "sentiment"]).size().reset_index(name="messages")
        fig = px.bar(
            topics, y="topic", x="messages", color="sentiment", orientation="h",
            color_discrete_map=COLORS, category_orders={"sentiment": ORDER},
            title="What passengers talk about", labels={"topic": "", "messages": ""},
        )
        fig.update_yaxes(categoryorder="total ascending")
        st.plotly_chart(tidy(fig))
    with right:
        routes = (
            df.groupby("route").agg(score=("score", "mean"), messages=("id", "count"))
            .reset_index().sort_values("score")
        )
        routes["tone"] = routes["score"].map(lambda s: "positive" if s >= 0 else "negative")
        fig = px.bar(
            routes, y="route", x="score", orientation="h", color="tone",
            color_discrete_map=COLORS, hover_data={"messages": True, "tone": False},
            title="Average sentiment by route (worst at top)", labels={"route": "", "score": ""},
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(showlegend=False)
        st.plotly_chart(tidy(fig))

    # ---- alerts + live feed -----------------------------------------------------
    left, right = st.columns([1, 2])
    with left:
        st.subheader("Alerts")
        alerts = storage.load_alerts()
        if alerts.empty:
            st.caption(
                "None so far. An alert is raised when most of a route's recent "
                "feedback turns negative within a short window."
            )
        for _, a in alerts.iterrows():
            when = pd.to_datetime(a["timestamp"]).strftime("%H:%M:%S")
            st.markdown(
                f"🔴 **{a['route']}** at {when}  \n"
                f"{a['negative_ratio']:.0%} of {a['messages']} messages negative, "
                f"mostly about {a['top_topic'].lower()}"
            )
    with right:
        st.subheader("Latest feedback")
        feed = df.tail(12).iloc[::-1]
        st.dataframe(
            pd.DataFrame({
                "Time": feed["timestamp"].dt.strftime("%H:%M:%S"),
                "Route": feed["route"],
                "Mood": feed["sentiment"].map(lambda s: f"{DOT[s]} {s}"),
                "Topic": feed["topic"],
                "Feedback": feed["text"],
            }),
            hide_index=True,
        )


live()
