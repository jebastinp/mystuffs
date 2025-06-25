
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Jebastin",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with models.DAG(
    dag_id='batch_clickstream_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run Dataflow batch job and validate BigQuery data',
    tags=['clickstream', 'batch', 'dataflow']
) as dag:

    start_dataflow_job = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_batch',
        py_file='gs://casestudy3data/batch_pipeline.py',
        location='us-central1',
        dataflow_default_options={
            'project': 'studied-beanbag-462316-g3',
            'tempLocation': 'gs://casestudy3data/temp',
            'stagingLocation': 'gs://casestudy3data/staging',
            'zone': 'us-central1-c'
        }
    )

    check_data_quality = BigQueryCheckOperator(
        task_id='check_data_quality',
        sql="""
            SELECT COUNT(*) FROM `studied-beanbag-462316-g3.ecommerce.ecommerce_trends`
            WHERE event_date = CURRENT_DATE()
        """,
        use_legacy_sql=False
    )

    start_dataflow_job >> check_data_quality
