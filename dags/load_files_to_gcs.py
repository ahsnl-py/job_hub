import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path,**kwargs):
    data_folder = data_folder
    bucket_name = 'job-hub-bucket'  # Your GCS bucket name
    gcs_conn_id = 'job-hub-gcp'
    # List all CSV files in the data folder
    files = [file for file in os.listdir(data_folder) if file.endswith('.parquet')]

    # Upload each CSV file to GCS
    for p_file in files:
        local_file_path = os.path.join(data_folder, p_file)
        gcs_file_path = f"{gcs_path}/{p_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id='load_parquet',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)

# Define your DAG
dag = DAG(
    'upload_files_to_gcs',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set your desired schedule interval or None for manual triggering
    catchup=False,  # Set to True if you want historical DAG runs upon creation
)

loaddate = '14-01-2024'
# Define the PythonOperator to run the upload_to_gcs function
load_parquet_file = PythonOperator(
    task_id='load_parquet',
    python_callable=upload_to_gcs,
    op_args=[f'/usr/local/airflow/include/data/{loaddate}/', f'stg/{loaddate}'],
    provide_context=True,
    dag=dag,
)

# Define your DAG dependencies
load_parquet_file