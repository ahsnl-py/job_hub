from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['jobposts'],
)
def load_jobpost():

    _gcp_conn_id='job-hub-gcp'
    loaddate = '14-01-2024'

    upload_jobpost_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_jobpost_to_gcs',
        src=f'include/data/{loaddate}/jobcz.parquet',
        dst=f'stg/{loaddate}/jobcz.parquet',
        bucket='job-hub-bucket',
        gcp_conn_id=_gcp_conn_id,
        mime_type='parquet',
    )

    upload_jobpost_detail_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_jobpost_detail_to_gcs',
        src=f'include/data/{loaddate}/job_detail.parquet',
        dst=f'stg/{loaddate}/job_detail.parquet',
        bucket='job-hub-bucket',
        gcp_conn_id=_gcp_conn_id,
        mime_type='parquet',
    )

    create_jobpost_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_jobposts_dataset',
        dataset_id='jobhub',
        gcp_conn_id=_gcp_conn_id,
    )

    gcs_to_stg_jobpost = aql.load_file(
        task_id='gcs_to_stg_jobpost',
        input_file=File(
            f'gs://job-hub-bucket/stg/{loaddate}/jobcz.parquet',
            conn_id=_gcp_conn_id,
            filetype=FileType.PARQUET,
        ),
        output_table=Table(
            name='stg_jobpost',
            conn_id=_gcp_conn_id,
            metadata=Metadata(schema='jobhub')
        ),
        use_native_support=False,
    
    )
    gcs_to_stg_jobpost_detail = aql.load_file(
        task_id='gcs_to_stg_jobpost_detail',
        input_file=File(
            f'gs://job-hub-bucket/stg/{loaddate}/job_detail.parquet',
            conn_id=_gcp_conn_id,
            filetype=FileType.PARQUET,
        ),
        output_table=Table(
            name='stg_jobpost_detail',
            conn_id=_gcp_conn_id,
            metadata=Metadata(schema='jobhub')
        ),
        use_native_support=False,
    
    )

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    chain(
        [upload_jobpost_to_gcs, upload_jobpost_detail_to_gcs],
        create_jobpost_dataset,
        [gcs_to_stg_jobpost, gcs_to_stg_jobpost_detail],
        transform
    )

load_jobpost()
