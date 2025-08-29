# Real-Time Viewer Dashboard

Live analytics for concurrent viewers, events/sec, dwell time, and top countries.

## Stack
- Streamlit (UI)
- SQLite (local store)
- Python event simulator

## Run
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python event_sim.py         # terminal 1
streamlit run app.py        # terminal 2
