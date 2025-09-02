import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.db import ENGINE
from src.models.survival import dwell_label, fit_km
from src.models.timeseries import starts_per_minute, prophet_forecast

load_dotenv()
st.set_page_config(page_title="Real-Time Viewer Dashboard (Kafka → Postgres)", layout="wide")
st.title("Real-Time Viewer Behavior")

@st.cache_data(ttl=3)
def load_events():
    # last 24h from Postgres
    return pd.read_sql(
        "SELECT * FROM events WHERE ts > now() - interval '24 hours'",
        ENGINE,
        parse_dates=['ts']
    )

df = load_events()
if df.empty:
    st.info("No data yet. Start the Kafka producer & consumer to load events.")
    st.stop()

now = pd.Timestamp.now(tz="UTC")

tab_live, tab_surv, tab_fore = st.tabs(["Live Metrics", "Survival (Python)", "Forecast (CTR proxy)"])

# LIVE METRICS
with tab_live:
    # KPIs
    last_min = now - pd.Timedelta(seconds=60)
    active = df[df["ts"] >= last_min]["viewer_id"].nunique()

    last_10s = now - pd.Timedelta(seconds=10)
    eps = len(df[df["ts"] >= last_10s]) / 10.0

    # dwell (30m window)
    win = df[df["ts"] >= (now - pd.Timedelta(minutes=30))].sort_values("ts")
    starts = win[win["event_type"] == "view_start"].groupby("viewer_id")["ts"].min()
    ends   = win[win["event_type"] == "view_end"].groupby("viewer_id")["ts"].max()
    aligned = pd.concat([starts.rename("start"), ends.rename("end")], axis=1)
    aligned["end"] = aligned["end"].fillna(now)
    aligned["dwell_sec"] = (aligned["end"] - aligned["start"]).dt.total_seconds().clip(lower=0)
    avg_dwell = aligned["dwell_sec"].mean() if len(aligned) else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Concurrent (≈60s)", f"{active:,}")
    c2.metric("Events/sec (10s)", f"{eps:.1f}")
    c3.metric("Avg dwell (30m)", f"{avg_dwell/60:.1f} min")

    # EPS chart (5 min)
    five = df[df["ts"] >= (now - pd.Timedelta(minutes=5))].copy()
    if not five.empty:
        five["sec"] = five["ts"].dt.floor("s")  # lower-case 's'
        ts_counts = five.groupby("sec")["id"].count().reset_index(name="events")
        st.plotly_chart(px.line(ts_counts, x="sec", y="events", title="Events/sec (last 5 min)"),
                        width="stretch")

    # Concurrency (15 min)
    fifteen = df[df["ts"] >= (now - pd.Timedelta(minutes=15))].copy()
    if not fifteen.empty:
        start = fifteen["ts"].min().floor("s")
        end   = now.ceil("s")
        timeline = pd.date_range(start, end, freq="s")  # lower-case 's'
        conc = pd.DataFrame({"sec": timeline})
        conc["concurrent"] = [
            fifteen[fifteen["ts"] >= (t - pd.Timedelta(seconds=60))]["viewer_id"].nunique()
            for t in timeline
        ]
        st.plotly_chart(px.line(conc, x="sec", y="concurrent", title="Concurrent viewers (rolling 60s)"),
                        width="stretch")

    # Top countries
    if not fifteen.empty:
        top = (fifteen.groupby("country")["viewer_id"]
               .nunique().sort_values(ascending=False).head(10)
               .reset_index(name="active_viewers"))
    else:
        top = pd.DataFrame(columns=["country", "active_viewers"])
    st.subheader("Top Countries (unique, last 15 min)")
    st.dataframe(top, use_container_width=True)

# SURVIVAL
with tab_surv:
    st.markdown("**Kaplan–Meier survival** of dwell duration (last 24h).")
    dwell_df = dwell_label(df[["ts", "viewer_id", "event_type"]].copy(), now=now)

    if dwell_df.empty:
        st.info("Not enough data yet to compute survival.")
    else:
        km, sf = fit_km(dwell_df)  # returns (None, None) if not enough clean data
        if km is None or sf is None or sf.empty:
            st.info("Not enough clean dwell data yet to fit a survival curve.")
        else:
            fig = px.line(sf, x="sec", y="survival", title="Survival curve (probability still engaged)")
            fig.update_layout(yaxis=dict(range=[0, 1]))
            st.plotly_chart(fig, width="stretch")
            st.caption(
                f"N={len(dwell_df)} viewers • "
                f"churned={int(dwell_df['churned'].sum())} • "
                f"censored={int((1 - dwell_df['churned']).sum())}"
            )

# FORECAST
with tab_fore:
    st.markdown("**Starts per minute** as a CTR-like proxy, with simple forecast.")
    spm = starts_per_minute(df)
    if spm.empty or len(spm) < 10:
        st.info("Not enough starts to build a forecast yet.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=spm["ts"], y=spm["starts"], mode="lines", name="Observed"))

        try:
            fc = prophet_forecast(spm, periods=60)  # forecast next 60 minutes
            fig.add_trace(go.Scatter(x=fc["ds"], y=fc["yhat"], mode="lines", name="Forecast"))
            fig.add_trace(go.Scatter(
                x=pd.concat([fc["ds"], fc["ds"][::-1]]),
                y=pd.concat([fc["yhat_upper"], fc["yhat_lower"][::-1]]),
                fill="toself", line=dict(width=0), name="Forecast interval", opacity=0.2
            ))
        except ImportError:
            st.warning("Prophet not installed; showing observed only. `pip install prophet` to enable forecast.")

        fig.update_layout(title="Starts/min (observed & forecast)", xaxis_title="Time", yaxis_title="Starts")
        st.plotly_chart(fig, width="stretch")
