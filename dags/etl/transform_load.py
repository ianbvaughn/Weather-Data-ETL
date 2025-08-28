import pandas as pd
import json
from sqlalchemy import create_engine, Table, MetaData, select, insert, inspect

def transform_load_weather():

   etl_metadata = MetaData()
   engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_etl")
   stage_table = Table("weather_raw",etl_metadata,autoload_with=engine,schema="public")
   with engine.connect() as conn:
      return_val = conn.execute(select(stage_table).order_by(stage_table.c.load_ts))
      rows = return_val.all()

   df = pd.DataFrame(rows, columns=["timestamp","json_response_raw"])
   data = json.loads(df["json_response_raw"][0])
   data_hourly = pd.json_normalize(data["hourly"])
   data_hourly_clean = pd.DataFrame({
      "dt": pd.to_datetime(data_hourly["dt"], unit="s"),
      "temp_c": data_hourly["temp"] - 273.15,
      "feels_like_c": data_hourly["feels_like"] - 273.15
   })

   data_hourly_clean.to_sql(
      'fact_weather',
      con=engine,
      if_exists="append",
      index=False
   )