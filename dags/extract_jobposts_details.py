from datetime import datetime, timedelta
from airflow.decorators import dag, task
from scripts.data_service import DataService, DataExtractionJobCz
from scripts.data_storage import FileStorageParquet
from pandas import DataFrame
from bs4 import BeautifulSoup

import pandas as pd
import os
import logging
import sys

load_date = datetime.now()
default_args = {
    'owner': 'ahsnl7',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
PATH:str = os.getcwd()  + f"/include/data/{load_date.strftime('%d-%m-%Y')}/"
CACHE_PATH:str = os.getcwd() + "/include/cache/jobhubcache.db"

client = DataService()
client.set_service(DataExtractionJobCz())
client.set_storage(FileStorageParquet(PATH, "job_detail"))

@dag(
    dag_id="extract_jobpost_details",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 10)
)
def run():

    @task(multiple_outputs=True)
    def extract():
        return { 'request_data': client.request_data(CACHE_PATH, PATH + "jobcz.parquet", (5*3600)) }

    @task(multiple_outputs=True)
    def transform(contents):
        load_t = []
        for id, htmlc in contents.items():
            soup = BeautifulSoup(htmlc, 'html.parser')
            load_t.extend(client.transform_data(soup, id, load_date.strftime("%Y-%m-%d")))
        pdt = pd.DataFrame(load_t)
        pdt["job_id"] = pdt["job_id"].apply(lambda x: int(x))
        return { 'trans_job_content': pdt}

    @task()
    def load(df:DataFrame):
        client.save_data(df)

    job_posts = extract()
    job_posts_t = transform(job_posts['request_data'])
    load(job_posts_t['trans_job_content'])
  

run()