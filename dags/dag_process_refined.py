from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

parquet_path = "/opt/datalake/raw/raw_table_1"
table_name = "refined_table_1"
query = "select count(*), current_date as date, EXTRACT from temp_view"
partition = '''["year", "month", "date"]'''

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_process_refined",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
)

task_run_bash_script = BashOperator(
    task_id="run_bash_script",
    bash_command=f"spark-submit  /opt/scripts/spark_refined.py --parquet_path {parquet_path} --query {query}  --table_name {table_name} --partition {partition}", 
    dag=dag,
)
