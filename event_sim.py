# Simulates viewer events and writes to SQLite
import random, time, uuid, sqlite3, os
from datetime import datetime, timezone

DB_PATH = os.environ.get("VIEWER_DB", "data/viewer.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

con = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = con.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  viewer_id TEXT NOT NULL,
  video_id TEXT NOT NULL,
  event_type TEXT NOT NULL,   -- view_start | heartbeat | view_end
  country TEXT NOT NULL
)
""")
con.commit()

COUNTRIES = ["US","IN","BR","DE","GB","CA","AU","JP","MX","ZA","FR","IT","ES","AE","SG"]
VIDEO_IDS = [f"video_{i}" for i in range(1,6)]
ACTIVE = {}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def new_viewer():
    v = str(uuid.uuid4())
    ACTIVE[v] = {"video_id": random.choice(VIDEO_IDS), "country": random.choice(COUNTRIES)}
    return v

def insert(event):
    cur.execute(
        "INSERT INTO events(ts, viewer_id, video_id, event_type, country) VALUES(?,?,?,?,?)",
        (event["ts"], event["viewer_id"], event["video_id"], event["event_type"], event["country"])
    )
    con.commit()

print(f"Simulating events -> {DB_PATH}")
try:
    while True:
        # randomly start new viewers
        if random.random() < 0.35 or not ACTIVE:
            v = new_viewer()
            m = ACTIVE[v]
            insert({"ts": now_iso(), "viewer_id": v, "video_id": m["video_id"], "event_type": "view_start", "country": m["country"]})

        # heartbeats for some active viewers
        for v in list(ACTIVE.keys()):
            if random.random() < 0.65:
                m = ACTIVE[v]
                insert({"ts": now_iso(), "viewer_id": v, "video_id": m["video_id"], "event_type": "heartbeat", "country": m["country"]})

        # randomly end sessions
        for v in list(ACTIVE.keys()):
            if random.random() < 0.12:
                m = ACTIVE[v]
                insert({"ts": now_iso(), "viewer_id": v, "video_id": m["video_id"], "event_type": "view_end", "country": m["country"]})
                ACTIVE.pop(v, None)

        time.sleep(1)  # 1-second tick
except KeyboardInterrupt:
    pass
finally:
    con.close()
