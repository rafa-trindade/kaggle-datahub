from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'Rafael Trindade',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SCRIPTS_PATH = "/opt/airflow/scripts"

def process_feminicidio_br():
    os.system("python /opt/airflow/scripts/process/process_feminicidio_br.py")

def load_kaggle_feminicidio_br():
    os.system("python /opt/airflow/scripts/load/load_kaggle_feminicidio_br.py")

# ----------------------------
# DAG
# ----------------------------
with DAG(
    'fetch_weekly_kaggle_feminicidio',
    default_args=default_args,
    description='Pipeline Semanal de Coleta e Transformação de Dados',
    schedule_interval="0 3 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    process_task_feminicidio = PythonOperator(
        task_id='process_feminicidio_br',
        python_callable=process_feminicidio_br
    )
    
    load_kaggle_feminicidio = PythonOperator(
        task_id='load_kaggle_feminicidio_br',
        python_callable=load_kaggle_feminicidio_br
    )

    process_task_feminicidio >> load_kaggle_feminicidio