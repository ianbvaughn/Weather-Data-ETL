import requests
from datetime import datetime, timezone
import os
from sqlalchemy import create_engine, Table, MetaData, insert, text, Text, Column, Integer, DateTime

def extract_weather():

   # Load API key from environment variable
   api_key = os.getenv("OPEN_WEATHER_API")

   # San Francisco coordinates
   lat = 37.7749
   lon = -122.4194

   base_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={api_key}"

   try:
      response = requests.get(base_url)
      response.raise_for_status()
      
   except requests.exceptions.HTTPError as http_err:
      print(f"HTTP error occurred: {http_err}")
      return
   except requests.exceptions.RequestException as err:
      print(f"Error occurred: {err}")
      return

   # Define PG connection
   etl_metadata = MetaData()
   engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/postgres")

   # Define table
   stg_table = Table(
      "stg_weather_data",
      etl_metadata,
      Column("id", Integer, primary_key=True, autoincrement=True),
      Column("load_ts", DateTime(timezone=True), nullable=False),
      Column("json_response", Text, nullable=False),
      schema="stage"
   )

   with engine.begin() as conn:
     # Create stage schema and table if they don't exist
     conn.execute(text("CREATE SCHEMA IF NOT EXISTS stage"))
     etl_metadata.create_all(engine)

     # Insert the JSON response into the table
     conn.execute(
        insert(stg_table).values(
           {stg_table.c.load_ts:datetime.now(timezone.utc),
            stg_table.c.json_response:response.text}
         )
      )