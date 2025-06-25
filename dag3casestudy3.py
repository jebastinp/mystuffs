from airflow import models
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id='run_clickstream_beam_dataflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run clickstream analysis Beam pipeline on Dataflow using BashOperator',
) as dag:

    run_clickstream_dataflow = BashOperator(
        task_id='run_clickstream_dataflow_pipeline',
        bash_command="""
        gsutil cp gs://casestudy3data1/casestudy3.py /tmp/casestudy3.py &&
        python3 /tmp/casestudy3.py \
            --runner=DataflowRunner \
            --project=jun-jebastinp-16jun-cts \
            --region=us-east1 \
            --job_name=clickstream-analysis-job \
            --staging_location=gs://casestudy3data1/staging \
            --temp_location=gs://casestudy3data1/temp
        """
    )
