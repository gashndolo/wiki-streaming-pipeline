import datetime as dt

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads
import streamlit as st
from deltalake import DeltaTable

from spark_jobs import config

REFRESH_SECONDS = 10
MINUTE_COLS = ["window_start", "wiki", "events", "bot_events", "human_events", "updated_at"]
PAGE_COLS = ["window_start", "wiki", "title", "edits", "net_bytes"]

st.set_page_config(page_title="Wikipedia edits, live", layout="wide")


def read_recent(path: str, since: dt.datetime, columns: list[str]) -> pd.DataFrame:
    """Rows with window_start >= since, read straight from Delta (no Spark involved)."""
    try:
        dataset = DeltaTable(path).to_pyarrow_dataset()
        cutoff = pa.scalar(since, type=pa.timestamp("us", tz="UTC"))
        table = dataset.to_table(filter=pads.field("window_start") >= cutoff,
                                 columns=columns)
        return table.to_pandas()
    except Exception as exc:
        st.warning(f"Could not read {path}: {exc}")
        return pd.DataFrame(columns=columns)


@st.fragment(run_every=REFRESH_SECONDS)
def live_view(window_minutes: int, top_n: int):
    now = dt.datetime.now(dt.UTC)
    since = now - dt.timedelta(minutes=window_minutes)

    minutes = read_recent(config.GOLD_MINUTE_PATH, since, MINUTE_COLS)
    if minutes.empty:
        st.info("Waiting for gold data. Is the pipeline running?")
        return

    # The current minute is still filling up, so leave it out of rate calculations
    current_minute = pd.Timestamp(now).floor("min")
    per_minute = (minutes.groupby("window_start")[["events", "bot_events", "human_events"]]
                  .sum().sort_index())
    complete = per_minute[per_minute.index < current_minute]

    total = int(minutes["events"].sum())
    bots = int(minutes["bot_events"].sum())
    recent_rate = complete["events"].tail(5).mean() if len(complete) else float("nan")
    freshness = (now - minutes["updated_at"].max()).total_seconds()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"Events, last {window_minutes} min", f"{total:,}")
    c2.metric("Events per minute (last 5 full)",
              f"{recent_rate:,.0f}" if recent_rate == recent_rate else "n/a")
    c3.metric("Bot share", f"{100 * bots / total:.1f}%" if total else "n/a")
    c4.metric("Data freshness", f"{freshness:.0f} s")

    st.subheader("Events per minute (UTC)")
    chart_df = per_minute[["human_events", "bot_events"]].rename(
        columns={"human_events": "Human", "bot_events": "Bot"})
    chart_df.index = chart_df.index.tz_convert("UTC").tz_localize(None)
    st.bar_chart(chart_df)

    by_wiki = (minutes.groupby("wiki")[["events", "bot_events"]].sum()
               .sort_values("events", ascending=False).head(top_n))
    by_wiki["Bot share (%)"] = 100 * by_wiki["bot_events"] / by_wiki["events"]

    left, right = st.columns(2)
    left.subheader("Busiest wikis")
    left.bar_chart(by_wiki["events"])
    right.subheader("Bot share by wiki (%)")
    right.bar_chart(by_wiki["Bot share (%)"])

    st.subheader("Most-edited pages, last 15 minutes")
    pages = read_recent(config.GOLD_PAGE_PATH, now - dt.timedelta(minutes=15), PAGE_COLS)
    if pages.empty:
        st.caption("No page has reached 2 edits within a 5-minute window yet.")
    else:
        top_pages = (pages.groupby(["wiki", "title"], as_index=False)[["edits", "net_bytes"]]
                     .sum().sort_values("edits", ascending=False).head(10))
        st.dataframe(top_pages, hide_index=True)

    st.caption(f"Updated {now:%H:%M:%S} UTC. Refreshes every {REFRESH_SECONDS} s. "
               "Pages are listed only once they reach 2 edits in a 5-minute window.")


st.title("Wikipedia edits, live")
st.caption("Wikimedia EventStreams → Kafka → Spark Structured Streaming → Delta Lake")

window = st.sidebar.select_slider("Time window (minutes)",
                                  options=[15, 30, 60, 120, 240], value=60)
top_n = st.sidebar.slider("Wikis to show", 3, 15, 8)
st.sidebar.caption("Only aggregates and public page titles are shown. "
                   "Individual editors are never displayed.")

live_view(window, top_n)