from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Import your external scripts
from scripts.extract_hdhi import extract_hdhi
from scripts.load_hdhi import load_raw_hdhi

default_args = {
    "owner": "joaquin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="hdhi_elt_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="ELT pipeline for HDHI admission dataset"
) as dag:

    extract = PythonOperator(
        task_id="extract_hdhi",
        python_callable=extract_hdhi,
    )

    load = PythonOperator(
        task_id="load_raw_hdhi",
        python_callable=load_raw_hdhi,
    )

    transform = PostgresOperator(
        task_id="transform_hdhi",
        postgres_conn_id="postgres_default",
        sql="scripts/transform_hdhi.sql",
    )

    extract >> load >> transform
