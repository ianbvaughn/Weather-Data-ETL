# Weather Data ETL
## Version 1.0

This project grabs hourly weather data for San Francisco, cleans it, and writes it to a database.

This project is built Apache Airflow and Postgres. The ETL DAG runs **daily** as follows:

1. Grab hourly weather data using OpenWeather API. You will need to provide your own API key by creating a `.env` file in the root directory and adding the following line:

```python
OPEN_WEATHER_API='<YOUR_API_KEY>'
```

2. Use pandas to grab datetime at hourly interval, current temperature (c) and 'feels like' temperature (c).
3. Write the dataframe to a Postgres container.

Before the DAG will execute, you will need to connect to the Postgres instance, using either psql or a tool such as DBeaver, and manually create the `airflow_etl` DB, `raw` and `fact` tables required. Consult the `extract.py` and `transform_load.py` files to determine which columns to create. I will document this better in future iterations. 
