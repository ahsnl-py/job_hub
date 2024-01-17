from datetime import datetime, timedelta
from airflow.decorators import dag, task
from scripts.data_service import DataService, DataProviderJobCz
from scripts.data_storage import FileStorageParquet
from pandas import DataFrame
from bs4 import BeautifulSoup

import pandas as pd
import os
import logging
import sys

default_args = {
    'owner': 'ahsnl7',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
PAGE_LIMIT:int = 20
load_date = datetime.now()
PATH:str = os.getcwd() + f"/include/data/{load_date.strftime('%d-%m-%Y')}/"
CACHE_PATH:str = os.getcwd() + "/include/cache/jobhubcache.db"

client = DataService()
client.set_service(DataProviderJobCz())
client.set_storage(FileStorageParquet(PATH, "jobcz"))

@dag(
    dag_id="extract_jobpost_jobcz",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 10)
)
def run():

    @task()
    def extract():
        return client.request_data(load_date.strftime('%Y-%m-%d'), PAGE_LIMIT, CACHE_PATH, (24*3600))

    @task(multiple_outputs=True)
    def transform(contents):
        load_t = []
        for htmlc in contents:
            soup = BeautifulSoup(htmlc, 'html.parser')
            load_t.extend(client.transform_data(soup))
        return {'trans_job_posts': pd.DataFrame(load_t)}

    @task()
    def load(df:DataFrame):
        client.save_data(df)
    
    try:
        job_posts = extract()
        job_posts_t = transform(job_posts)
        load(job_posts_t['trans_job_posts'])
    except:
        logging.error(f"unexpected error: {sys.exc_info()[0]}")

run()