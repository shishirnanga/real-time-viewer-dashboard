# Real-Time Viewer Dashboard  

End-to-end data engineering & analytics pipeline for real-time video engagement insights.  

This project simulates millions of viewer events (starts, stops, clicks) and streams them through **Kafka → Postgres → Airflow → Streamlit**, with predictive analytics (survival & forecasting) for churn and CTR-like metrics.  

## Tech Stack  
- **Kafka (Redpanda)** – streaming ingestion  
- **Postgres** – persistent event store  
- **Airflow** – scheduled ETL/DAGs  
- **Streamlit** – real-time dashboard  
- **FastAPI** – API layer (optional extension)  
- **Python (Pandas, Plotly, SQLAlchemy)** – data processing & visualization  
- **Lifelines** – churn/survival modeling  
- **Prophet** – CTR forecasting  

---

## Features  
- **Live metrics**: concurrent viewers, events/sec, average dwell time  
- **Geo insights**: top countries by active viewers  
- **Forecasting**: CTR proxy via starts/minute with Prophet  
- **Churn modeling**: Kaplan–Meier survival analysis on dwell duration  
- **Streaming pipeline**: Kafka → Postgres consumer/producer  

---

## Architecture  


## Run
```bash
1)  cd infra
    docker compose up -d
2)  Start the Kafka consumer
    export KAFKA_BOOTSTRAP=127.0.0.1:19092
    python -m src.kafka_consumer
3)  Start the Kafka producer (event simulator)
    python -m src.kafka_producer
4)  Launch the Streamlit dashboard
    streamlit run app_pg.py
    Visit http://localhost:8501
