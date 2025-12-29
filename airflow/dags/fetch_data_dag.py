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
# Fetch / Process Painel de Oncologia
# ----------------------------

def fetch_painel_oncologia(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_painel_oncologia.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_painel_oncologia_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_painel_oncologia")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_painel_oncologia.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

# ----------------------------
# Fetch / Process Sistema de Informação sobre Mortalidade (SIM) - Consolidados
# ----------------------------

def fetch_sim_declaracao_obito(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_sim_declaracao_obito.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_sim_declaracao_obito_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_sim_declaracao_obito")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_sim_declaracao_obito.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

# ----------------------------
# Fetch / Process Sistema de Informação sobre Mortalidade (SIM) - Preliminares
# ----------------------------

def fetch_sim_declaracao_obito_prelim(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_sim_declaracao_obito_prelim.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_sim_declaracao_obito_prelim_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_sim_declaracao_obito_prelim")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_sim_declaracao_obito_prelim.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

def fetch_sim_causas_externas(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_sim_causas_externas.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_sim_causas_externas_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_sim_causas_externas")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_sim_causas_externas.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

def fetch_sim_causas_externas_prelim(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_sim_causas_externas_prelim.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_sim_causas_externas_prelim_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_sim_causas_externas_prelim")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_sim_causas_externas_prelim.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

# ----------------------------
# Fetch CNES
# ----------------------------
def fetch_cnes():
    os.system("python /opt/airflow/scripts/extract/dados_abertos/fetch_cnes_estabelecimentos.py")

# ----------------------------
# Fetch Macroregião de Saúde
# ----------------------------
def fetch_macroregiao():
    os.system("python /opt/airflow/scripts/extract/dados_abertos/fetch_macroregiao_de_saude.py")

# ----------------------------
# Fetch SIOPS
# ----------------------------
def fetch_siops():
    os.system("python /opt/airflow/scripts/extract/dados_abertos/fetch_siops_orcamento_publico.py")

# ----------------------------
# DAG
# ----------------------------
with DAG(
    'fetch_daily_data_pipeline',
    default_args=default_args,
    description='Pipeline Diária de Coleta de Dados',
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fetch_task_datasus_po = PythonOperator(
        task_id='fetch_painel_oncologia',
        python_callable=fetch_painel_oncologia
    )

    process_task_datasus_po = PythonOperator(
        task_id='process_painel_oncologia',
        python_callable=process_painel_oncologia_if_updated
    )

    fetch_task_datasus_sim = PythonOperator(
        task_id='fetch_sim_declaracao_obito',
        python_callable=fetch_sim_declaracao_obito
    )

    process_task_datasus_sim = PythonOperator(
        task_id='process_sim_declaracao_obito',
        python_callable=process_sim_declaracao_obito_if_updated
    )

    fetch_task_datasus_sim_prelim = PythonOperator(
        task_id='fetch_sim_declaracao_obito_prelim',
        python_callable=fetch_sim_declaracao_obito_prelim
    )

    process_task_datasus_sim_prelim = PythonOperator(
        task_id='process_sim_declaracao_obito_prelim',
        python_callable=process_sim_declaracao_obito_prelim_if_updated
    )

    fetch_task_sim_causas_externas = PythonOperator(
        task_id='fetch_sim_causas_externas',
        python_callable=fetch_sim_causas_externas
    )

    process_task_sim_causas_externas = PythonOperator(
        task_id='process_sim_causas_externas',
        python_callable=process_sim_causas_externas_if_updated
    )

    fetch_task_sim_causas_externas_prelim = PythonOperator(
        task_id='fetch_sim_causas_externas_prelim',
        python_callable=fetch_sim_causas_externas_prelim
    )

    process_task_sim_causas_externas_prelim = PythonOperator(
        task_id='process_sim_causas_externas_prelim',
        python_callable=process_sim_causas_externas_prelim_if_updated
    )

    fetch_task_cnes = PythonOperator(
        task_id='fetch_cnes',
        python_callable=fetch_cnes
    )

    fetch_task_macroregiao = PythonOperator(
        task_id='fetch_macroregiao',
        python_callable=fetch_macroregiao
    )

    fetch_task_siops = PythonOperator(
        task_id='fetch_siops',
        python_callable=fetch_siops
    )

    fetch_task_datasus_po >> process_task_datasus_po >> \
    fetch_task_datasus_sim >> process_task_datasus_sim >> \
    fetch_task_datasus_sim_prelim >> process_task_datasus_sim_prelim >> \
    fetch_task_sim_causas_externas >> process_task_sim_causas_externas >> \
    fetch_task_sim_causas_externas_prelim >> process_task_sim_causas_externas_prelim >> \
    fetch_task_cnes >> fetch_task_macroregiao >> fetch_task_siops