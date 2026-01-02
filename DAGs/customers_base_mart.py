from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 
from functions import execute_sql_script    

OWNER = "{{ OWNER }}" #обеспечивает уникальность дагов по ученикам для деплоя

with DAG(
          dag_id=f'customers_base_mart_{OWNER}', #меняет название дага до _dag
          start_date = datetime(2025, 11, 30),
          schedule_interval='0 9 * * *', #меняем расписание запуска дага
          catchup=False,
          tags=[OWNER],
          default_args={
                    "owner": OWNER
          }
) as dag:

          customers_base_mart = PythonOperator( #меняем название задачи
                  task_id = 'customers_base_mart', #меняем название задачи
                  python_callable=execute_sql_script,
                  op_kwargs={'file_path': f'/opt/airflow/dags/{OWNER}/dags/sql/customers_base_mart'} #меняем название sql скрипта
          )

customers_base_mart #меняем название задачи
