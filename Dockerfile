# Use Airflow 3.0.5 as base
FROM apache/airflow:3.0.5

# Switch to root to install packages
USER root

# Install system dependencies (optional, needed for some GE backends)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install Great Expectations and optional extras for SQL and Pandas
RUN pip install --no-cache-dir "great-expectations" \
    && pip install airflow-provider-great-expectations

USER airflow
RUN airflow db init
RUN airflow db upgrade

# Set work directory (optional)
WORKDIR /opt/airflow

# By default, the image will still run Airflow webserver
# CMD ["webserver"]
