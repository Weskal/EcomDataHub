from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
import uuid
import time
import json
import pytz
from airflow.decorators import dag, task #type:ignore
import io

PIPELINE_ID = 2

SRC_PATH = "/opt/airflow/src"
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
    
from utils.minio_connection import connect_minio
from utils.bigquery_connection import bq_get_existing_files, bq_insert_data
from utils.postgre_connection import update_pipeline_status

BUCKET = "bronze-layer"
DATA_FOLDER = "/opt/airflow/data"

@dag(
    dag_id="bigquery_ingestion_v1",
    start_date = datetime(2025, 12, 1, 0, 0), 
    schedule_interval='*/15 * * * *', # Roda a cada 15min
    catchup=False
)
def bigquery_ingestion():
    
    @task
    def t_read_minio_files():
        
        client = connect_minio()
        
        #Lê arquivos Parquet do MinIO (bronze) 
        objects = client.list_objects(
                    BUCKET,
                    recursive=True
                )
        
        pre_candidates = [obj.object_name for obj in objects]

        pre_candidates_order_files = []

        # 1 - Filtrar o que são ordens e o que é metadado

        if pre_candidates:
            
            for pre_candidate in pre_candidates:
                if isinstance(pre_candidate, str):
                    if not pre_candidate.endswith(".json"):
                        pre_candidates_order_files.append(pre_candidate)
        
        
        # 2 - Filtrar arquivos dos últimos 15 minutos
        
        now = datetime.now()
        limit = now - timedelta(minutes=15)
        
        candidate_files = []
        
        for candidate in pre_candidates_order_files:
            candidate_str = candidate.split("_")[1].replace(".parquet", "")
            candidate_datetime = datetime.strptime(candidate_str, "%d-%m-%Y - %H:%M:%S")
            #candidate = candidate.split("-")[-1].split(".")[0]
            if limit <= candidate_datetime <= now:
                candidate_files.append(candidate)
            
        
        # Devolver uma lista formatada com as ordens criadas nos últimos 15 minutos
        return candidate_files
    
    @task
    def t_prepare_candidates(candidates):
        
        minio_client = connect_minio()
        
        # getobject dos arquivos necessários
        
        existing_files = bq_get_existing_files(candidates)

        # novos arquivos que ainda não estão no BigQuery
        files_to_insert_bq = [f for f in candidates if f not in existing_files]

        # pegar os arquivos do MinIO conforme files_to_insert_bq e passar eles para a próxima etapa em forma de df
        # 1 - Pegar arquivos do MinIO
        # 2 - Converter esses arquivos para dataframe + salvar o nome
        # 3 - retornar df e file_name
        
        results = {}
        
        for file_name in files_to_insert_bq:
            try:
                response = minio_client.get_object(BUCKET, file_name)
                df = pd.read_parquet(io.BytesIO(response.read()))
                results[file_name] = df
            except Exception as e:
                print(f"Erro no arquivo {file_name}: {e}")
        
        return results
    
    @task
    def t_transform_data(file_dfs):
        processed_dfs = {}
        for file_name, df in file_dfs.items():
            
            df = df.astype({
            'order_id': str,
            'product_id': int,
            'product_title': str,
            'price': float,
            'description': str,
            'category': str,
            'qty': int,
             })
            
            df["created_at"] = pd.to_datetime(df['created_at'], format="%d-%m-%Y - %H:%M:%S", errors='coerce')
            
            if (df["price"] < 0).any():
                raise ValueError(f"Preços negativos encontrados em {file_name}")
            
            df["total_value"] = (df["qty"] * df["price"]).round(2)
            
            df = df[df["qty"] > 0]
            
            df['category'] = df['category'].str.lower()
            
            df['source_file'] = file_name

            # Reorganizar na ordem da tabela do bq
            
            safe_name = file_name.replace(":", "-").replace(" ", "_")
            output_path = os.path.join(DATA_FOLDER, "processed", safe_name + ".parquet")
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            df.to_parquet(output_path, index=False)
        
        processed_dfs[file_name] = df
        
        # Saída Silver
        return processed_dfs
    
    # @task
    # def save_local():
    #     #salva Parquet em data/processed para inspeção
    #     pass
    
    @task
    def send_to_bq(data):
        bq_insert_data(data)
        #Carrega os dados para silver.orders no BigQuery
        pass
    
bigquery_ingestion()