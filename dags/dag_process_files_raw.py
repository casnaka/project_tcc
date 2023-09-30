from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

type = "json"
raw_file_path = "/opt/data_set/iris/"
table_name = "teste_1"
ds = "{{ds}}"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_process_file",
    default_args=default_args,
    schedule_interval="0 1 * * *", #roda todos os dias a 1 hora da manha
    catchup=False,
)

task_run_bash_script = BashOperator(
    task_id="run_bash_script",
    bash_command=f"spark-submit  /opt/script/spark_file_process.py --type {type} --raw_file_path {raw_file_path}  --table_name {table_name} --ds {ds} ", 
    dag=dag,
)
