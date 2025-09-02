# src/kafka_producer.py
import os, json, time, math, random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "viewer.events")

#Tunable knobs 
BASE_ARRIVAL_RATE = 15
DIURNAL_AMPLITUDE = 0.6
HEARTBEAT_EVERY = (5, 10)

BOUNCE_PROB = 0.08          # fewer instant exits
LOGNORM_MEAN = 5.2          # << was 2.2; ln-median ~ 5.2 → median ~ e^5.2 ≈ 180s (~3 min)
LOGNORM_SIGMA = 0.60        # moderate tail
MAX_DWELL_SEC = 45 * 60     # cap at 45 min
# 

COUNTRIES = ["US","IN","BR","GB","DE","FR","SG","AU","ES","IT","MX","CA","JP"]
COUNTRY_P =   [0.18,0.16,0.10,0.06,0.06,0.05,0.05,0.05,0.08,0.07,0.06,0.04,0.04]
VIDEOS = [f"v{n:03d}" for n in range(1, 201)]

p = Producer({"bootstrap.servers": BOOTSTRAP})
print(f"[debug] KAFKA_BOOTSTRAP = {BOOTSTRAP}")
print(f"Producing to {BOOTSTRAP} topic={TOPIC} (Ctrl+C to stop)")

@dataclass
class Session:
    viewer_id: str
    video_id: str
    country: str
    start_ts: datetime
    end_ts: datetime
    next_hb: datetime

active: Dict[str, Session] = {}

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def poiss(lmbda: float) -> int:
    # simple Poisson sampler
    L = math.exp(-lmbda)
    k, prod = 0, 1.0
    while prod > L:
        k += 1
        prod *= random.random()
    return k - 1

def diurnal_multiplier(t: datetime) -> float:
    # smooth 8–12 minute “day/night” cycle so you can see waves quickly
    period_sec = 10 * 60        # 10 min period (short for demo)
    phase = (t.timestamp() % period_sec) / period_sec * 2 * math.pi
    return 1.0 + DIURNAL_AMPLITUDE * math.sin(phase)

def draw_dwell_seconds() -> int:
    if random.random() < BOUNCE_PROB:
        return random.randint(5, 15)  # quick bounces
    # lognormal with a realistic tail
    dwell = random.lognormvariate(LOGNORM_MEAN, LOGNORM_SIGMA)
    dwell = min(int(dwell), MAX_DWELL_SEC)
    return max(dwell, 10)

def emit(event: dict):
    event["ts"] = now_utc().isoformat()
    p.produce(TOPIC, json.dumps(event).encode("utf-8"))

def maybe_start_new_sessions(t: datetime):
    rate = BASE_ARRIVAL_RATE * diurnal_multiplier(t)
    for _ in range(poiss(rate)):
        viewer = f"u{random.randint(100000, 999999)}"
        video = random.choice(VIDEOS)
        country = random.choices(COUNTRIES, COUNTRY_P, k=1)[0]
        start = t
        dwell = draw_dwell_seconds()
        end = start + timedelta(seconds=dwell)
        hb_in = random.randint(*HEARTBEAT_EVERY)
        sess = Session(viewer, video, country, start, end, start + timedelta(seconds=hb_in))
        active[viewer] = sess
        emit({"event_type":"view_start","viewer_id":viewer,"video_id":video,"country":country})

def advance_heartbeats_and_ends(t: datetime):
    finished: List[str] = []
    for viewer, s in active.items():
        # heartbeats
        if t >= s.next_hb and t < s.end_ts:
            emit({"event_type":"heartbeat","viewer_id":s.viewer_id,"video_id":s.video_id,"country":s.country})
            s.next_hb = t + timedelta(seconds=random.randint(*HEARTBEAT_EVERY))
        # ends
        if t >= s.end_ts:
            emit({"event_type":"view_end","viewer_id":s.viewer_id,"video_id":s.video_id,"country":s.country})
            finished.append(viewer)
    for v in finished:
        active.pop(v, None)

try:
    while True:
        t = now_utc()
        maybe_start_new_sessions(t)
        advance_heartbeats_and_ends(t)
        p.poll(0)               # flush background delivery callbacks
        time.sleep(1.0)         # 1-second ticks
except KeyboardInterrupt:
    pass
finally:
    p.flush(5)
