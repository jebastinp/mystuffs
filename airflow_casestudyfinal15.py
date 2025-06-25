import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.utils.trigger_rule import TriggerRule
 
# --- Configuration Variables ---
# IMPORTANT: Ensure these match your GCP project, bucket, and desired dataset/table
PROJECT_ID = "studied-beanbag-462316-g3"
REGION = "us-central1" # Or your desired region
CLUSTER_NAME = "casestudy215" # Name for your Dataproc cluster
BUCKET_NAME = "casestudy2testing" # Your GCS bucket for data and scripts
BQ_DATASET = "casestudy2" # BigQuery dataset name
BQ_TABLE = "transactions" # BigQuery table name
 
# GCS paths for PySpark script and processed Parquet output
PYSPARK_URI = f"gs://casestudy2testing/pyspark15.py"
# The "*.parquet" is crucial here, as Spark writes multiple part files.
PARQUET_OUTPUT_URI = f"gs://{BUCKET_NAME}/processed/*.parquet"
 
# Default arguments for the DAG
default_args = {
    "owner": "jebastin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}
 
# --- DAG Definition ---
with DAG(
    "casestudy2final", # Unique DAG ID (changed to avoid conflicts with previous broken versions)
    default_args=default_args,
    schedule_interval=None, # Set to None for manual triggers or a cron expression for scheduled runs
    catchup=False, # Prevents running for past missed schedules
    tags=["dataproc", "pyspark", "bigquery", "etl"],
) as dag:
 
    # Task 1: Create a Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2", # Consider 'e2-standard-2' for lower cost
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2", # Consider 'e2-standard-2' for lower cost
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
            },
        },
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
 
    # Task 2: Submit the PySpark Job to the Dataproc Cluster
    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )
 
    # Task 3: Create (or ensure existence of) the BigQuery destination table
    # This task now defines both time partitioning and clustering when the table is created.
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        project_id=PROJECT_ID,
        # Explicitly adding dataset_id and table_id as top-level arguments as required by the operator
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BQ_DATASET,
                "tableId": BQ_TABLE,
            },
            "schema": {
                "fields": [
                    {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "category_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
                    {"name": "total_sales", "type": "FLOAT", "mode": "NULLABLE"},
                ]
            },
            "timePartitioning": {"type": "DAY", "field": "transaction_date"},
            "clustering": {"fields": ["store_id", "category_id"]},
        },
        exists_ok=True,
    )
 
    # Task 4: Load the processed Parquet data from GCS into BigQuery
    # This configuration now perfectly matches the table creation in create_bq_table.
    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [PARQUET_OUTPUT_URI],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "timePartitioning": {"type": "DAY", "field": "transaction_date"},
                "clustering": {"fields": ["store_id", "category_id"]},
                "autodetect": True,
            }
        },
    )
 
    # Task 5: Delete the Dataproc Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
 
    # --- Task Dependencies ---
    create_cluster >> submit_job
    submit_job >> create_bq_table
    create_bq_table >> load_to_bq
    [submit_job, load_to_bq] >> delete_cluster