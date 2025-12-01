from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime
import os
import sys

SRC_PATH = "/opt/airflow/src"
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# from scripts.pipeline import run_pipeline
# # garantir que /opt/airflow/src está no PYTHONPATH
# sys.path.append("/opt/airflow/src")
from scripts.pipeline import run_pipeline  # sua função já existente

def run_pipeline_wrapper():
    data_folder = "/opt/airflow/data"
    run_pipeline(data_folder=data_folder)

with DAG(
    dag_id="orders_pipeline",
    start_date = datetime(2025, 12, 1, 0, 0),
    schedule = "* * * * *",
    catchup=False,
) as dag:

    run_orders = PythonOperator(
        task_id="run_orders_pipeline",
        python_callable=run_pipeline_wrapper,
    )
    