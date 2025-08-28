from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime,timedelta
from etl.extract import extract_weather
from etl.transform_load import transform_load_weather

default_args = {
   'owner': 'airflow',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}

with DAG(
   dag_id='weather_etl',
   description='ETL operation for writing OpenWeather API data to a PostgreSQL database',
   start_date=datetime(2025,8,25),
   schedule='@daily'
) as dag:
   task1 = PythonOperator(
      task_id='extract',
      python_callable=extract_weather
   )

   task2 = PythonOperator(
      task_id='transform_load',
      python_callable=transform_load_weather
   )

   task1 >> task2