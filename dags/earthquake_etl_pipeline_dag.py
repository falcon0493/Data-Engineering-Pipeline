
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from datetime import timedelta
from dotenv import load_dotenv
import os
import sys



sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.data_extraction import extract_data
from src.data_transformation import transform_data
from dags.src.data_loading import load_data


try:
    load_dotenv()
except:
    print("some issue with .env file...")


url = os.getenv('BASE_URL')
driver = os.getenv('ODBC_DRIVER')
mssql_host = os.getenv('MSSQL_HOST')
database = os.getenv('DATABASE')
table = os.getenv('TABLE')
user = os.getenv('USER')
password = os.getenv('PASSWORD')


conn_str = (f"mssql+pyodbc://{user}:{password}@{mssql_host}:1433/{database}?driver={driver}"+"&Encrypt=no"+"&TrustServerCertificate=yes")



def extract_task(**kwargs):
    return extract_data(url=url)




def transform_task(**kwargs):
    ti = kwargs["ti"]

    raw_files = ti.xcom_pull(task_ids="extract")

    # XCom returns a list (even if it's one file)
    if not raw_files:
        raise ValueError("No raw files received from extract task")

    raw_file_path = raw_files[0]  # take the first file

    return transform_data(raw_file_path)




def load_task(**kwargs):
    ti = kwargs["ti"]

    transformed_file_path = ti.xcom_pull(task_ids="transform")

    if not transformed_file_path:
        raise ValueError("No transformed data received")
    
    if isinstance(transformed_file_path, list):
        transformed_file_path = transformed_file_path[0]

    load_data(file_path=transformed_file_path, table=table, conn_str=conn_str)



default_args = {
    "owner": "@falcon0493",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(

    dag_id="earthquake_etl_pipeline_dag",
    description="ETL pipeline for earthquake data",
    schedule="@daily",                       
    start_date=timezone.datetime(2025, 12, 4),
    catchup=False,
    default_args=default_args,

) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )



    extract >> transform >> load