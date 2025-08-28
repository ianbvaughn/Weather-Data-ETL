import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, insert, inspect

def extract_weather():
   load_dotenv()
   api_key = os.getenv("OPEN_WEATHER_API")
   lat = 37.7749
   lon = -122.4194

   base_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={api_key}"

   try:
      response = requests.get(base_url)
      response.raise_for_status()
      
   except requests.exceptions.HTTPError as http_err:
      print(f"HTTP error occurred: {http_err}")
   except requests.exceptions.RequestException as err:
      print(f"Error occurred: {err}")

   etl_metadata = MetaData()
   engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_etl")
   etl_table = Table("weather_raw",etl_metadata,autoload_with=engine,schema="public")
   with engine.connect() as conn:
     conn.execute(insert(etl_table).values({etl_table.c.load_ts:datetime.now(timezone.utc),etl_table.c.json_response:response.text}))