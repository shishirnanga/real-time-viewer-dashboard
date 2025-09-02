# src/models/timeseries.py
import pandas as pd

def starts_per_minute(events: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 'view_start' events to 1-minute buckets.
    Returns columns: ['ts','starts'] where ts is *naive* (no tz) per Prophet needs.
    """
    if events is None or events.empty:
        return pd.DataFrame(columns=["ts", "starts"])

    df = events.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df = df[df["event_type"] == "view_start"].copy()

    # floor to minute
    df["minute"] = df["ts"].dt.floor("min")

    # aggregate
    spm = (df.groupby("minute")["viewer_id"]
             .count()
             .rename("starts")
             .reset_index()
             .rename(columns={"minute": "ts"}))

    # Prophet wants tz-naive 'ds'
    # Convert UTC→naive by dropping tz
    if pd.api.types.is_datetime64tz_dtype(spm["ts"]):
        spm["ts"] = spm["ts"].dt.tz_convert(None)

    return spm


def prophet_forecast(spm: pd.DataFrame, periods: int = 60) -> pd.DataFrame:
    """
    Simple Prophet forecast on starts/min.
    Input spm columns: ['ts','starts'] with ts tz-naive.
    Returns dataframe with ['ds','yhat','yhat_lower','yhat_upper'].
    """
    from prophet import Prophet  # lazy import

    if spm is None or spm.empty:
        return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])

    df_p = spm.rename(columns={"ts": "ds", "starts": "y"}).copy()

    # Ensure tz-naive for Prophet
    df_p["ds"] = pd.to_datetime(df_p["ds"], errors="coerce")
    # If any are tz-aware, drop tz
    if pd.api.types.is_datetime64tz_dtype(df_p["ds"]):
        df_p["ds"] = df_p["ds"].dt.tz_convert(None)
    # Some pandas versions return object dtype after tz ops; enforce datetime64[ns]
    df_p["ds"] = pd.to_datetime(df_p["ds"]).dt.tz_localize(None)

    # Guard against duplicates & sort
    df_p = df_p.dropna(subset=["ds", "y"])
    df_p = df_p.groupby("ds", as_index=False)["y"].sum().sort_values("ds")

    if len(df_p) < 10:
        # Not enough history to fit
        return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])

    m = Prophet(interval_width=0.8, daily_seasonality=True, weekly_seasonality=True)
    m.fit(df_p)

    # Build naive-datetime future minutes
    future = m.make_future_dataframe(periods=periods, freq="min", include_history=True)
    # make_future_dataframe already produces tz-naive; enforce just in case:
    future["ds"] = pd.to_datetime(future["ds"]).dt.tz_localize(None)

    fc = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    return fc
