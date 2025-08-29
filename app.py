import os, sqlite3, pandas as pd, plotly.express as px
import streamlit as st

DB_PATH = os.environ.get("VIEWER_DB", "data/viewer.db")
st.set_page_config(page_title="Real-Time Viewer Dashboard", layout="wide")

@st.cache_data(ttl=3)
def load_df():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame(columns=["id","ts","viewer_id","video_id","event_type","country"])
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM events", con, parse_dates=["ts"])
    con.close()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df

st.title("📺 Real-Time Viewer Behavior")

df = load_df()
if df.empty:
    st.info("Waiting for events… In another terminal, run:  `python event_sim.py`")
    st.stop()

now = pd.Timestamp.now(tz="UTC")

# KPIs
last_min = now - pd.Timedelta(seconds=60)
active_viewers = df[df["ts"] >= last_min]["viewer_id"].nunique()

last_10s = now - pd.Timedelta(seconds=10)
eps = len(df[df["ts"] >= last_10s]) / 10.0

# dwell estimation (last 30 min)
win = df[df["ts"] >= (now - pd.Timedelta(minutes=30))].sort_values("ts")
starts = win[win["event_type"] == "view_start"].groupby("viewer_id")["ts"].min()
ends = win[win["event_type"] == "view_end"].groupby("viewer_id")["ts"].max()
aligned = pd.concat([starts.rename("start"), ends.rename("end")], axis=1)
aligned["end"] = aligned["end"].fillna(now)
aligned["dwell_sec"] = (aligned["end"] - aligned["start"]).dt.total_seconds().clip(lower=0)
avg_dwell = aligned["dwell_sec"].mean() if len(aligned) else 0

col1, col2, col3 = st.columns(3)
col1.metric("Concurrent viewers (≈60s window)", f"{active_viewers:,}")
col2.metric("Events / sec (last 10s)", f"{eps:.1f}")
col3.metric("Avg dwell (last 30m)", f"{avg_dwell/60:.1f} min")

# Events/second (5 min)
five = df[df["ts"] >= (now - pd.Timedelta(minutes=5))].copy()
five["sec"] = five["ts"].dt.floor("s")
ts_counts = five.groupby("sec")["id"].count().reset_index(name="events")
fig_ts = px.line(ts_counts, x="sec", y="events", title="Events/second (last 5 min)")
st.plotly_chart(fig_ts, use_container_width=True)

# Concurrent viewers over time (15 min, rolling 60s)
fifteen = df[df["ts"] >= (now - pd.Timedelta(minutes=15))].copy()
if not fifteen.empty:
    start = fifteen["ts"].min().floor("s")
    end = now.ceil("s")
    timeline = pd.date_range(start, end, freq="S")

    def concurrent_at(t):
        return fifteen[fifteen["ts"] >= (t - pd.Timedelta(seconds=60))]["viewer_id"].nunique()

    conc = pd.DataFrame({"sec": timeline})
    conc["concurrent"] = [concurrent_at(t) for t in timeline]
    fig_conc = px.line(conc, x="sec", y="concurrent", title="Concurrent viewers (rolling 60s)")
    st.plotly_chart(fig_conc, use_container_width=True)

# Top countries (15 min)
top = fifteen.groupby("country")["viewer_id"].nunique().sort_values(ascending=False).head(10).reset_index(name="active_viewers")
st.subheader("Top Countries (unique viewers, last 15 min)")
st.dataframe(top, use_container_width=True)

st.caption(f"DB: {DB_PATH} • auto-refresh ~3s")
