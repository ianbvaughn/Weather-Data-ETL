import great_expectations as gx
from great_expectations import expectations as gxe

def set_expectations():

    context = gx.get_context()
    print(context.root_directory)

    data_source = context.data_sources.add_postgres(
        name="postgres_db", 
        connection_string="postgresql+psycopg2://airflow:airflow@postgres/postgres"
    )

    table_asset = data_source.add_table_asset(
        name="weather_data_hourly",
        schema_name="fact",
        table_name="fact_weather_data_hourly"
    )

    full_table_batch_definition = table_asset.add_batch_definition_whole_table(
        name="full_table"
    )

    ts_unique = gxe.ExpectColumnValuesToBeUnique(
        column="ts"
    )

    batch = full_table_batch_definition.get_batch()

    print(batch.validate(ts_unique))