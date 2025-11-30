import os
import logging

DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/HDHI_Admission data.csv"

def extract_hdhi(**context):
    """Validates that dataset exists and pushes raw path via XCom."""
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"Dataset missing: {RAW_FILE}")

    logging.info(f"Dataset found at {RAW_FILE}")
    context["ti"].xcom_push(key="raw_path", value=RAW_FILE)
