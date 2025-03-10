from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

def run_dbt(target):
    command = f"bash -c 'source /home/admin/airflow_env/bin/activate && cd /home/admin/Projetos/pipe_vendas/dbt_pipe_vendas && dbt run --target {target}'"
    subprocess.run(command, shell=True, check=True)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 3, 10),
}

dag = DAG(
    'dbt_dynamic_target_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

run_raw_target = PythonOperator(
    task_id='run_raw_target',
    python_callable=run_dbt,
    op_args=['raw'],
    dag=dag,
)

run_trusted_target = PythonOperator(
    task_id='run_trusted_target',
    python_callable=run_dbt,
    op_args=['trusted'],
    dag=dag,
)

run_refined_target = PythonOperator(
    task_id='run_refined_target',
    python_callable=run_dbt,
    op_args=['refined'],
    dag=dag,
)

run_raw_target >> run_trusted_target >> run_refined_target