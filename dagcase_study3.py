from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess



# DAG configuration
default_args = {
    'owner': 'jebastin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'casestudy3',
    default_args=default_args,
    description='to automate the casestudy3', schedule_interval='@daily', catchup=False
)



# Define the Beam pipeline execution function
def run_beam_pipeline():
    local_path = 'casestudy3.py'
    gcs_path = 'gs://casestudy3data1/casestudy3.py'



# Step 1: Download the script from GCS
subprocess.run(['gsutil', 'cp', gcs_path, local_path], check=True)



# Step 2: Run the script
subprocess.run(['python', local_path], check=True)



# Create the PythonOperator
run_pipeline_task = PythonOperator(task_id='casestudy_beam_pipeline',
    python_callable=run_beam_pipeline,
    dag=dag
)