import os
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

PG_USER = os.getenv("POSTGRES_USER","viewer")
PG_PW   = os.getenv("POSTGRES_PASSWORD","viewerpass")
PG_DB   = os.getenv("POSTGRES_DB","viewerdb")
PG_HOST = os.getenv("POSTGRES_HOST","localhost")
PG_PORT = os.getenv("POSTGRES_PORT","5432")

ENGINE = sa.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PW}@{PG_HOST}:{PG_PORT}/{PG_DB}", pool_pre_ping=True)

CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  viewer_id TEXT NOT NULL,
  video_id TEXT NOT NULL,
  event_type TEXT NOT NULL,  -- view_start | heartbeat | view_end
  country TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_viewer ON events(viewer_id);
"""

CREATE_ROLLUPS_SQL = """
CREATE TABLE IF NOT EXISTS rollup_hourly AS
SELECT now()::timestamptz AS ts, 0::int AS events, 0::int AS active_viewers
WHERE FALSE;
"""

def ensure_schema():
    with ENGINE.begin() as conn:
        conn.execute(text(CREATE_EVENTS_SQL))
        conn.execute(text(CREATE_ROLLUPS_SQL))
