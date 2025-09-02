# src/kafka_consumer.py
import os, json, time
from confluent_kafka import Consumer
from sqlalchemy import text
from dotenv import load_dotenv
from src.db import ENGINE, ensure_schema

load_dotenv()
ensure_schema()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "viewer.events")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_SEC = float(os.getenv("FLUSH_SEC", "2.0"))

print(f"[debug] KAFKA_BOOTSTRAP = {BOOTSTRAP}")

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "viewer-consumer",
    "auto.offset.reset": "earliest",
})

c.subscribe([TOPIC])

INSERT_SQL = text("""
INSERT INTO events (ts, viewer_id, video_id, event_type, country)
VALUES (:ts, :viewer_id, :video_id, :event_type, :country)
""")

print(f"Consuming from {BOOTSTRAP} topic={TOPIC} → Postgres (Ctrl+C to stop)")

rows = []
last_flush = time.time()
inserted = 0

def flush_rows():
    """Insert buffered rows and commit the transaction."""
    global rows, inserted, last_flush
    if not rows:
        return
    try:
        with ENGINE.begin() as conn:      
            conn.execute(INSERT_SQL, rows)  
        inserted += len(rows)
        print(f"[ingest] inserted {len(rows)} (total={inserted})")
        rows = []
        last_flush = time.time()
    except Exception as ex:
        print("[ingest] batch insert error:", ex)

try:
    while True:
        msg = c.poll(1.0)  
        if msg is None:
            if time.time() - last_flush >= FLUSH_SEC:
                flush_rows()
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            e = json.loads(msg.value().decode("utf-8"))
            rows.append({
                "ts": e["ts"],                    
                "viewer_id": e["viewer_id"],
                "video_id": e["video_id"],
                "event_type": e["event_type"],
                "country": e.get("country", "US"), 
            })
        except Exception as ex:
            print("[parse] bad message:", ex, "payload=", msg.value())

        if len(rows) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SEC:
            flush_rows()

except KeyboardInterrupt:
    pass
finally:
    flush_rows()
    c.close()
