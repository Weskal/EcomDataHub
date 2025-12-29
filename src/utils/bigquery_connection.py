from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

TABLE = "orders"
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_DATASET = os.getenv("GCP_DATASET")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not all([GCP_PROJECT_ID, GCP_DATASET, CREDENTIALS_PATH]):
    raise EnvironmentError("Variáveis de ambiente do BigQuery não configuradas corretamente")

client = bigquery.Client(
    project=GCP_PROJECT_ID,
    credentials=bigquery.Client.from_service_account_json(
        CREDENTIALS_PATH
    )._credentials
)

# def select_orders_source_files() -> list[str]:
#     query = f"""
#         SELECT source_file
#         FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.{TABLE}`
#     """
#     job = client.query(query)
#     return [row.source_file for row in job.result()]

def bq_get_existing_files(candidates):
    query = f"""
        SELECT source_file
        FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.{TABLE}`
        WHERE source_file IN UNNEST(@files)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("files", "STRING", candidates)
        ]
    )
    job = client.query(query, job_config=job_config)
    return {row.source_file for row in job.result()}


def bq_insert_data(data):
    if not data:
        

def run_query(query: str):
    job = client.query(query)
    job.result() 
    return job
