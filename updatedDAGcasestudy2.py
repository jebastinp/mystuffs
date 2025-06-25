from airflow import DAG
from datetime import timedelta
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
PROJECT_ID = "studied-beanbag-462316-g3"
REGION = "us-central1"
CLUSTER_NAME = "testing-cluster"
BUCKET_NAME = "casestudy2final"
BQ_DATASET = "testingcasestudy2"
BQ_TABLE = "transactions"

# Updated GCS paths
PYSPARK_URI = f"gs://casestudy2final/pysparkcasestudy2.py"
PARQUET_OUTPUT_URI = f"gs://casestudy2final*.parquet"

default_args = {
    "owner": "Jebastin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

with DAG(
    "casestudy2finaltesing",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "pyspark", "bigquery", "etl"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
            },
        },
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
        },
    )

    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        project_id=PROJECT_ID,
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

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task dependencies
    create_cluster >> submit_job
    submit_job >> create_bq_table
    create_bq_table >> load_to_bq
    [submit_job, load_to_bq] >> delete_cluster
