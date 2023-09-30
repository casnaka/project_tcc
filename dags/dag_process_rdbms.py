from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

database = "prod"
user = "admin"
save_path = "/opt/process/"
password = "admin"
table_name = "teste_1"
query = "select id, name, sales from sales.sales_prod where date = <filter>"
ds = "{{ds}}"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_process_rdbms",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
)

task_run_bash_script = BashOperator(
    task_id="run_bash_script",
    bash_command=f"spark-submit  /opt/script/spark_rdbms_process.py --database {database} --user {user} --save_path {save_path} --table_name {table_name} --password {password} -query {query} --ds {ds} ", 
    dag=dag,
)
