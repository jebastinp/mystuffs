import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Default arguments for the DAG
ARGS = {
    "owner": "Jebastin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
}

# Project and dataset/table configuration
PROJECT_ID = "studied-beanbag-462316-g3"
RAW_DATASET_ID = "raw_dataset"
TRANSFORMED_DATASET_ID = "insight_dataset"
RAW_TABLE_NAME = "customer_raw_data"
TRANSFORMED_TABLE_NAME = "customer_transformed_data"
GCS_BUCKET_NAME = "casestudy1data1"
GCS_OBJECT_PATH = "Customer_Data.csv"

# Full BigQuery table paths
RAW_BQ_TABLE = f"{PROJECT_ID}.{RAW_DATASET_ID}.{RAW_TABLE_NAME}"
TRANSFORMED_BQ_TABLE = f"{PROJECT_ID}.{TRANSFORMED_DATASET_ID}.{TRANSFORMED_TABLE_NAME}"

# SQL query to transform data
QUERY = f"""
    CREATE OR REPLACE TABLE `{TRANSFORMED_BQ_TABLE}` AS
    SELECT
        customer_nbr,
        customer_desc,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', start_ts)) AS start_ts,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', end_ts)) AS end_ts,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', create_ts)) AS create_ts,
        create_user_id,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', last_update_ts)) AS last_update_ts,
        last_update_user_id,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', start_ts) AS start_timestamp,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', end_ts) AS end_timestamp,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', create_ts) AS create_timestamp,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', last_update_ts) AS last_update_timestamp,
        client_id
    FROM
        `{RAW_BQ_TABLE}`
"""

# Define the DAG
with DAG(
    dag_id="casestudy1",
    schedule_interval="*/15 * * * *",
    default_args=ARGS,
    catchup=False,
    tags=["bigquery", "etl", "customer_data"],
) as dag:

    # Task 1: Load CSV from GCS to BigQuery raw table
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bq_raw",
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_OBJECT_PATH],
        destination_project_dataset_table=RAW_BQ_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        schema_fields=[
            {"name": "customer_nbr", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_desc", "type": "STRING", "mode": "NULLABLE"},
            {"name": "start_ts", "type": "STRING", "mode": "NULLABLE"},
            {"name": "end_ts", "type": "STRING", "mode": "NULLABLE"},
            {"name": "create_ts", "type": "STRING", "mode": "NULLABLE"},
            {"name": "create_user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update_ts", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update_user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Task 2: Transform and load data into final BigQuery table
    transform_and_load_data = BigQueryInsertJobOperator(
        task_id="transform_and_load_customer_data",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Set task dependencies
    load_csv_to_bq >> transform_and_load_data
