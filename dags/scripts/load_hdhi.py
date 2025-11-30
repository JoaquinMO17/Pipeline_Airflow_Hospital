import pandas as pd
import logging
from sqlalchemy import create_engine

POSTGRES_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"

def load_raw_hdhi(**context):
    """Loads raw CSV directly into a raw Postgres table."""
    raw_path = context["ti"].xcom_pull(key="raw_path")
    df = pd.read_csv(raw_path)

    logging.info(f"Loading raw HDHI dataset ({df.shape[0]} rows)")

    engine = create_engine(POSTGRES_CONN)
    df.to_sql("hdhi_raw", engine, if_exists="replace", index=False)

    logging.info("Raw data loaded into table 'hdhi_raw'")
