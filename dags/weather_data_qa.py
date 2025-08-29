from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime,timedelta
from gx.qa import set_expectations

default_args = {
   'owner': 'airflow',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}

with DAG(
   dag_id='weather_qa',
   description='QA for PostgreSQL database',
   start_date=datetime(2025,8,29),
   schedule='@daily'
) as dag:
   task1 = PythonOperator(
      task_id='qa',
      python_callable=set_expectations
   )

   task1