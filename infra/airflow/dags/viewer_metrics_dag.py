from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner":"airflow","retries":1,"retry_delay": timedelta(minutes=2)}
with DAG(
    dag_id="viewer_hourly_rollup",
    start_date=datetime(2024,1,1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
) as dag:

    # simple hourly rollup: events + unique viewers last hour
    rollup = PostgresOperator(
        task_id="rollup_hour",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO rollup_hourly(ts, events, active_viewers)
        SELECT date_trunc('hour', now()) as ts,
               COUNT(*) as events,
               COUNT(DISTINCT viewer_id) FILTER (WHERE ts > now() - interval '60 minutes') as active_viewers
        FROM events
        WHERE ts > now() - interval '60 minutes';
        """,
    )
