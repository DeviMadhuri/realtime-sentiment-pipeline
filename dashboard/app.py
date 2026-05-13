"""
Streamlit dashboard.

Reads the Delta table written by the streaming job, refreshes every
few seconds, and shows live sentiment metrics + recent reviews.
"""

import time
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

st.set_page_config(
    page_title="Realtime Sentiment",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Realtime Sentiment Dashboard")
st.caption("Live sentiment analysis on product reviews flowing through Kafka → Spark → Delta.")

# Path is relative to the project root, not the dashboard folder
DELTA_PATH = Path(__file__).resolve().parent.parent / "data" / "delta" / "reviews_sentiment"

placeholder = st.empty()


def load_delta() -> pd.DataFrame:
    """Load the latest Delta snapshot, or return an empty DataFrame if it's not ready yet."""
    if not DELTA_PATH.exists():
        return pd.DataFrame()
    try:
        return DeltaTable(str(DELTA_PATH)).to_pandas()
    except Exception:
        return pd.DataFrame()


while True:
    df = load_delta()

    if df.empty:
        placeholder.info("Waiting for the streaming job to write its first batch...")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp", ascending=False)

        with placeholder.container():
            # Top-level metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total reviews", len(df))
            pos_pct = (df["sentiment_label"] == "POSITIVE").mean() * 100
            neg_pct = (df["sentiment_label"] == "NEGATIVE").mean() * 100
            col2.metric("Positive", f"{pos_pct:.1f}%")
            col3.metric("Negative", f"{neg_pct:.1f}%")

            # Sentiment over time chart
            st.subheader("Sentiment over time")
            df["minute"] = df["timestamp"].dt.floor("1min")
            chart_data = (
                df.groupby(["minute", "sentiment_label"])
                .size()
                .reset_index(name="count")
            )
            chart = (
                alt.Chart(chart_data)
                .mark_area(opacity=0.7)
                .encode(
                    x=alt.X("minute:T", title="Time"),
                    y=alt.Y("count:Q", title="Reviews"),
                    color=alt.Color("sentiment_label:N", title="Sentiment"),
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)

            # Recent reviews
            st.subheader("Most recent reviews")
            display_cols = [
                "timestamp",
                "product",
                "review_text",
                "sentiment_label",
                "sentiment_score",
            ]
            st.dataframe(df[display_cols].head(20), use_container_width=True)

    time.sleep(3)
    st.rerun()
