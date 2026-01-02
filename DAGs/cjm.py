from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from functions import execute_sql_script

OWNER = "sergey_r"

SQL_SCRIPTS = [
    'cjm_authorisation',
    'cjm_deleted',
    'cjm_email',
    'cjm_purchases',
    'cjm_registration',
    'cjm_sms',
]

with DAG(
    dag_id=f'cjm_{OWNER}',
    start_date=datetime(2025, 11, 30),
    schedule_interval='0 9,15 * * *',
    catchup=False,
    tags=[OWNER],
    default_args={
        "owner": OWNER
    }
) as dag:

    tasks = []

    for script in SQL_SCRIPTS:
        task = PythonOperator(
            task_id=script.replace('.sql', ''),
            python_callable=execute_sql_script,
            op_kwargs={
                'file_path': f'/opt/airflow/dags/{OWNER}/dags/sql/{script}'
            }
        )
        tasks.append(task)
