import pandas as pd
import logging
import requests
import sqlite3
import time
import re
import os
import bs4

from pathlib import Path
from typing import Dict, List
from datetime import datetime
from abc import abstractmethod
from pandas import DataFrame
from bs4 import BeautifulSoup
from scripts.data_storage import IDataStorage

class IDataTransformationService:
    @abstractmethod
    def extract(self, *args):
        pass

    @abstractmethod
    def transform(self, *args) -> List[Dict[str,object]]:
        pass

class CacheService:
    def __init__(self, db_name):
        dirpath = str(Path(db_name).parent)
        if not os.path.exists(dirpath): os.mkdir(dirpath)
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_cache_schema()

    def setcache(self, key, value, ttl=None):
        current_time = int(time.time())
        if ttl:
            expire_time = current_time + ttl
        else:
            expire_time = None

        self.cursor.execute('SELECT * FROM cache WHERE key=?', (key,))
        existing_data = self.cursor.fetchone()

        if existing_data:
            if existing_data[1] != value:
                self.cursor.execute('''
                    UPDATE cache SET value=?, timestamp=? WHERE key=?
                ''', (value, current_time, key))
                self.conn.commit()
        else:
            self.cursor.execute('''
                INSERT INTO cache (key, value, timestamp) VALUES (?, ?, ?)
            ''', (key, value, current_time))
            self.conn.commit()

        if expire_time:
            if current_time > expire_time:
                self.delete(key)

    def getcache(self, key):
        self.cursor.execute('SELECT value, timestamp FROM cache WHERE key=?', (key,))
        data = self.cursor.fetchone()

        if data:
            return data[0]  # Return value if key exists
        else:
            return None  # Return None if key doesn't exist or TTL expired

    def delete(self, key):
        self.cursor.execute('DELETE FROM cache WHERE key=?', (key,))
        self.conn.commit()

    def close(self):
        self.conn.close()

    def create_cache_schema(self):
        self.cursor.execute(f'''
            SELECT name FROM sqlite_master WHERE type='table' AND name='cache'
        ''')
        table_exists = self.cursor.fetchone()

        if not table_exists:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    timestamp INTEGER
                )
            ''')
            self.conn.commit()

class RequestService(CacheService):
    
    def __init__(self, db_name, ttl=None):
        self.ttl = ttl
        super().__init__(db_name)

    def get(self, url:str, cachekey:str):
        try:
            content = ""
            if self.getcache(cachekey) is None:
                time.sleep(2)
                response = requests.get(url)
                content = response.text
                self.setcache(cachekey, content, self.ttl)
            else:
                content = self.getcache(cachekey)
            return content
        except requests.HTTPError as e:
            print(f"Issue found while requesting to server: {e}")
        except ValueError as e:
            print(f"Issue found: {e}")

class DataExtractionJobCz(IDataTransformationService):
    
    def extract(self, *args):
        logging.info(f'Input arguments: {"; ".join(str(a) for a in args)}')
        cachefile = args[0] 
        datastagefile = args[1]
        ttl = args[2]
        request = RequestService(cachefile, ttl)
        job_data = pd.read_parquet(datastagefile)
        keywords = ['sql', 'ssis developer', 'software developer', 'data engineer', 'big data engineer', 'etl developer', 'data integration specialist', 'data warehouse engineer', 'cloud data engineer', 'streaming data engineer', 'database administrator', 'data architect', 'business intelligence developer']
        job_data["title_lower"] = job_data[["title"]].apply(lambda x: x.str.lower())
        job_data = job_data[job_data["title_lower"].str.contains('|'.join(keywords))]
        job_data.reset_index(drop=True)
        contents = dict()
        for i in range(0, len(job_data)):
            link = job_data.iloc[i]['link']
            job_id = job_data.iloc[i]['job_id']
            contents[int(job_id)] = request.get(url=link, cachekey=str(job_id))
            
        return contents

    def transform(self, *args) -> List[Dict[str, object]]:
        soup:BeautifulSoup = args[0]
        date:datetime = args[2]
        container = soup.find('div', class_='Container Container--cassiopeia mb-1200')
        jobdesc, jobreq, jobabout = [], [], []
        content = container.contents if container else []
        for i in content:
            if type(i) == bs4.element.Tag:     
                if i.has_attr('class') and 'RichContent' in i['class'][0]:
                    ultags = [ ultag for ultag in soup.find_all('ul', {'class': 'typography-body-large-text-regular'})]
                    if len(ultags) > 0: jobdesc = [ litag.text for litag in ultags[0].find_all('li') ]
                    if len(ultags) >= 1: jobreq = [ litag.text for litag in ultags[1].find_all('li') ]
                elif i.has_attr('class') and 'mb-1000' in i['class'][0]:
                    jobabout = [ c.text for c in i.contents ]

        return [{
            "date": date,
            "job_id": args[1],
            "job_about": "\n".join(jobabout),
            "job_desc": "\n".join(jobdesc),
            "job_req": "\n".join(jobreq)
        }]

class DataProviderJobCz(IDataTransformationService):

    def extract(self, *args):
        date, page_limit, cachefile, ttl = args[0], args[1], args[2], args[3]
        request = RequestService(cachefile, ttl)
        contents = list()
        for i in range(1, page_limit+1):
            key = f'jobcz_posts_{date}_{i}'
            contents.append(request.get(f"https://beta.www.jobs.cz/en/?page={i}", key))
        return contents

    def transform(self, *args) -> List[Dict[str,object]]:
        soup:BeautifulSoup = args[0]
        job_listings = soup.find_all('article', class_='SearchResultCard')
        rows = list()
        for job in job_listings:
            salary, wfh, two_week_res, other_details = 'N/A', 'N/A', 'N/A', 'N/A'
            for jb in job.find('div', class_='SearchResultCard__body').find_all('span'):
                jb = jb.text.strip()
                if self.contains_currency(jb): salary = jb
                elif "home" in jb: wfh = jb
                elif "2 weeks" in jb: two_week_res = jb
                else: other_details = jb

            job_rating = 'N/A'
            job_info = list()
            for jf in job.find('footer').find_all('li', class_='SearchResultCard__footerItem'):
                jf = jf.text.strip()
                if 'rating' in jf: job_rating = jf
                else: job_info.append(jf)

            collection_attr = {
                "load_date": datetime.now().strftime("%Y-%m-%d"),
                "job_id": int(job.find('a', class_='link-primary SearchResultCard__titleLink').get('data-jobad-id')),
                "title": job.find('h2', class_='SearchResultCard__title').text.strip(),
                "link": job.find('a', class_='link-primary SearchResultCard__titleLink').get('href'),
                "date_added": self.get_job_added(job.find_all(class_=re.compile("SearchResultCard__status--[a-z]+"))),
                "salary": str(salary),
                "company": job_info[0],
                "city": job_info[1].split('–')[0].strip(),
                "district": job_info[1].split('–')[1].strip() if len(job_info[1].split('–')) >= 2 else "N/A",
                "work_from_home": wfh,
                "response_period": two_week_res,
                "rating": job_rating,
                "other_details": other_details
            }
            rows.append(collection_attr)

        return rows

    def contains_currency(self, text):
        czk_pattern = r'\b(\d+\.?\d*)\s?(Kč|Kc|CZK|EUR)\b' 
        match = re.search(czk_pattern, text, re.IGNORECASE)
        return bool(match)

    def get_job_added(self, job) -> str:
        if len(job) > 0:
            return job[0].get_text().strip()
        return "N/A"


class DataService:

    def __init__(self):
        self.service:IDataTransformationService
        self.storage:IDataStorage

    def set_service(self, tservice:IDataTransformationService):
        self.service = tservice

    def set_storage(self, storage:IDataStorage):
        self.storage = storage

    def request_data(self, *args):
        return self.service.extract(*args)

    def transform_data(self, *args):
        return self.service.transform(*args)

    def save_data(self, df:DataFrame):
        if not self.storage.is_exists():
            self.storage.create(df)
        else:
            self.storage.update(df)

    def file_exists(self, file_path):
        dir_path = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        for file in os.listdir(dir_path):
            if os.path.splitext(file)[0] == filename:
                return True
        return False




