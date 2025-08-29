import pandas as pd
import json
from sqlalchemy import create_engine, Table, MetaData, select, Column, DateTime, Numeric, text

def transform_load_weather():

   etl_metadata = MetaData()
   engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/postgres")
   
   with engine.begin() as conn:

      conn.execute(text("CREATE SCHEMA IF NOT EXISTS fact"))

   stg_table = Table(
      "stg_weather_data",
      etl_metadata,
      autoload_with=engine,
      schema="stage"
   )

   fact_table = Table(
      "fact_weather_data_hourly",
      etl_metadata,
      Column("ts", DateTime, primary_key=True, unique=True),
      Column("temp_c", Numeric(5,2), nullable=False),
      Column("feels_like_c", Numeric(5,2), nullable=False),
      schema="fact"
   )

   etl_metadata.create_all(engine)

   with engine.begin() as conn:

      return_val = conn.execute(select(stg_table.c.load_ts, stg_table.c.json_response).order_by(stg_table.c.load_ts))
      rows = return_val.all()

   # Convert to DataFrame and transform
   df = pd.DataFrame(rows, columns=["timestamp","json_response_raw"])
   data = json.loads(df["json_response_raw"][0])
   data_hourly = pd.json_normalize(data["hourly"])
   data_hourly_clean = pd.DataFrame({
      "ts": pd.to_datetime(data_hourly["dt"], unit="s"),
      "temp_c": data_hourly["temp"] - 273.15,
      "feels_like_c": data_hourly["feels_like"] - 273.15
   })

   # Load to fact table
   data_hourly_clean.to_sql(
      'fact_weather_data_hourly',
      con=engine,
      if_exists="append",
      index=False,
      schema="fact"
   )