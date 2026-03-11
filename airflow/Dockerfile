FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies for Snowflake connector
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages: dbt-snowflake + ingestion dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
