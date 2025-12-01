from minio import Minio
import dotenv
import os
import pathlib

script_dir = pathlib.Path(__file__).parent

env_path = (script_dir / ".." / "..").resolve() / ".env"

dotenv.load_dotenv(env_path)

access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")

def connect_minio():
    
    client = Minio(
            "minio:9000",
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
    
    return client