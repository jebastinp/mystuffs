import airflow
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

PROJECT_ID = "studied-beanbag-462316-g3"
REGION = "us-east1"
CLUSTER_NAME = "cluster-demo-2"
BUCKET_NAME = "casestudy2final"
BQ_DATASET = "testingcasestudy2"
BQ_TABLE = "transactions"
PYSPARK_URI = f"gs://{BUCKET_NAME}/pysparkcasestudy2final.py"
PARQUET_OUTPUT_URI = f"gs://{BUCKET_NAME}/processed/*.parquet"
SUBNETWORK_URI = "projects/studied-beanbag-462316-g3/regions/us-east1/subnetworks/YOUR_SUBNETWORK_NAME"

default_args = {
    "owner": "jebastin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

with DAG(
    "pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="cluster_creation",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "config": {
                "gce_cluster_config": {
                    "subnetwork_uri": SUBNETWORK_URI,
                },
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {
                        "boot_disk_type": "pd-balanced",
                        "boot_disk_size_gb": 32,
                    },
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {
                        "boot_disk_type": "pd-balanced",
                        "boot_disk_size_gb": 32,
                    },
                },
            }
        },
    )

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

    create_cluster >> submit_job >> create_bq_table >> load_to_bq >> delete_cluster
