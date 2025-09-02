# src/models/survival.py
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

def dwell_label(events: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
    """
    Build per-view dwell durations and churn flags from raw events.
    Expects columns: ['ts','viewer_id','event_type'] with event_type in {'view_start','view_end'}.
    Returns a DataFrame with: ['viewer_id','start','end','dwell_sec','churned'].
      - dwell_sec: seconds watched for that viewer in the window
      - churned: 1 if we saw a 'view_end' (i.e., observed end), 0 if still active (right-censored)
    Notes:
      - If multiple starts/ends exist per viewer in the window, we take the earliest start
        and the latest end to form one session-like interval (simple & robust).
      - Active viewers (no end yet) get end=now and churned=0.
    """
    if events is None or events.empty:
        return pd.DataFrame(columns=["viewer_id", "start", "end", "dwell_sec", "churned"])

    df = events.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # earliest start per viewer
    starts = (
        df[df["event_type"] == "view_start"]
        .groupby("viewer_id")["ts"].min()
        .rename("start")
    )

    # latest end per viewer (if any)
    ends = (
        df[df["event_type"] == "view_end"]
        .groupby("viewer_id")["ts"].max()
        .rename("end")
    )

    aligned = pd.concat([starts, ends], axis=1).reset_index()

    # if no start for a viewer, drop (cannot compute dwell)
    aligned = aligned.dropna(subset=["start"])

    # fill missing end with 'now' (still active → right-censored)
    aligned["end"] = aligned["end"].fillna(now)

    # ensure utc + correct ordering
    aligned["start"] = pd.to_datetime(aligned["start"], utc=True)
    aligned["end"]   = pd.to_datetime(aligned["end"],   utc=True)

    # compute dwell
    aligned["dwell_sec"] = (aligned["end"] - aligned["start"]).dt.total_seconds()

    # clean bad rows
    aligned = aligned.replace([np.inf, -np.inf], np.nan)
    aligned = aligned.dropna(subset=["dwell_sec"])
    aligned = aligned[aligned["dwell_sec"] >= 0]

    # churned flag: 1 if we actually saw an end event
    aligned["churned"] = (aligned["end"] != now).astype(int)

    return aligned[["viewer_id", "start", "end", "dwell_sec", "churned"]]

def fit_km(dwell_df: pd.DataFrame):
    """
    Fit Kaplan–Meier on dwell_df with columns ['dwell_sec','churned'].
    Returns (kmf, survival_df) or (None, None) if not enough clean data.
    """
    if dwell_df is None or dwell_df.empty:
        return None, None

    df = dwell_df.copy()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["dwell_sec", "churned"])
    df["dwell_sec"] = df["dwell_sec"].astype(float)
    df = df[df["dwell_sec"] >= 0]

    if len(df) < 3 or df["dwell_sec"].sum() <= 0:
        return None, None

    kmf = KaplanMeierFitter()
    kmf.fit(durations=df["dwell_sec"], event_observed=df["churned"])

    sf = (
        kmf.survival_function_.reset_index()
        .rename(columns={kmf.survival_function_.columns[0]: "survival",
                         "timeline": "sec"})
    )
    return kmf, sf
