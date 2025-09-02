import os, pandas as pd
from fastapi import FastAPI
from db import ENGINE
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Viewer KPIs API")

@app.get("/kpis")
def kpis():
    df = pd.read_sql("SELECT * FROM events WHERE ts > now() - interval '30 minutes'", ENGINE, parse_dates=["ts"])
    now = pd.Timestamp.now(tz="UTC")
    last_min = now - pd.Timedelta(seconds=60)
    active = df[df["ts"] >= last_min]["viewer_id"].nunique()
    last_10s = now - pd.Timedelta(seconds=10)
    eps = len(df[df["ts"] >= last_10s]) / 10.0

    starts = df[df.event_type=="view_start"].groupby("viewer_id")["ts"].min()
    ends = df[df.event_type=="view_end"].groupby("viewer_id")["ts"].max()
    aligned = pd.concat([starts.rename("start"), ends.rename("end")], axis=1)
    aligned["end"] = aligned["end"].fillna(now)
    dwell = aligned["end"] - aligned["start"]
    avg_dwell_min = (dwell.dt.total_seconds().clip(lower=0).mean() or 0)/60
    return {"active_viewers": int(active), "events_per_sec": round(eps,2), "avg_dwell_min": round(avg_dwell_min,2)}

@app.get("/concurrency")
def concurrency():
    df = pd.read_sql("SELECT * FROM events WHERE ts > now() - interval '15 minutes'", ENGINE, parse_dates=["ts"])
    if df.empty: return []
    now = pd.Timestamp.now(tz="UTC")
    timeline = pd.date_range(df["ts"].min().floor("s"), now.ceil("s"), freq="S")
    vals = []
    for t in timeline:
        count = df[df["ts"] >= (t - pd.Timedelta(seconds=60))]["viewer_id"].nunique()
        vals.append({"sec": t.isoformat(), "concurrent": int(count)})
    return vals

@app.get("/countries")
def countries():
    df = pd.read_sql("SELECT * FROM events WHERE ts > now() - interval '15 minutes'", ENGINE, parse_dates=["ts"])
    if df.empty: return []
    top = df.groupby("country")["viewer_id"].nunique().sort_values(ascending=False).head(10)
    return [{"country": c, "active_viewers": int(v)} for c, v in top.items()]
