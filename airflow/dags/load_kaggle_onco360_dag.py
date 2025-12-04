from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import subprocess

default_args = {
    'owner': 'Rafael Trindade',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SCRIPTS_PATH = "/opt/airflow/scripts"


# ----------------------------
# Sync Raw to Bucket / Kaggle
# ----------------------------
def load_raw_to_bucket(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "load/load_raw_to_bucket_onco360.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="bucket_updated", value=updated)
    return updated

def load_raw_to_kaggle_if_bucket_updated(ti=None):
    bucket_updated = ti.xcom_pull(key="bucket_updated", task_ids="load_raw_to_bucket")
    if bucket_updated:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'load/load_kaggle_onco360.py')}")
    else:
        print("[INFO] Nenhum arquivo novo enviado ao bucket. Pulando upload para Kaggle.")

# ----------------------------
# DAG
# ----------------------------

with DAG(
    dag_id='fetch_daily_kaggle_onco360',
    default_args=default_args,
    description='Pipeline Diária Kaggle Onco-360',
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    sync_raw_to_bucket = PythonOperator(
        task_id='load_raw_to_bucket',
        python_callable=load_raw_to_bucket
    )

    sync_raw_to_kaggle = PythonOperator(
        task_id='load_raw_to_kaggle',
        python_callable=load_raw_to_kaggle_if_bucket_updated
    )

    sync_raw_to_bucket >> sync_raw_to_kaggle