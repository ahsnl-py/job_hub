import pyarrow.parquet as pq
import pyarrow as pa
import os
import logging

import pandas as pd
from pandas import DataFrame
from scripts.data_schema import SchemaRepository
from abc import ABC, abstractmethod


class IDataStorage:
    @abstractmethod
    def create(self, data:DataFrame):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def update(self, data:DataFrame):
        pass

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def is_exists(self) -> bool:
        pass

    
class FileStorageCSV(IDataStorage):
    def __init__(self, file_path):
        self.file_path = file_path
        logging.info("Saving to " + self.file_path)
    
    def create(self, data: DataFrame):
        try:
            data.to_csv(self.file_path, mode='a', header=True, index=False)
            print("Data created successfully.")
            return True
        except Exception as e:
            print(f"Error creating data: {e}")
            return False

    def read(self):
        try:
            df = pd.read_csv(self.file_path)
            print("Data read successfully.")
            return df
        except Exception as e:
            print(f"Error reading data: {e}")
            return None

    def update(self, data:DataFrame):
        try:
            data.to_csv(self.file_path, mode='a', header=False, index=False)
            print("Data updated successfully.")
            return True
        except Exception as e:
            print(f"Error updating data: {e}")
            return False

    def delete(self):
        try:
            df = pd.DataFrame()
            df.to_csv(self.file_path, index=False)
            print("Data truncated successfully.")
            return True
        except Exception as e:
            print(f"Error truncating data: {e}")
            return False

    def is_exists(self) -> bool:
        if os.path.isfile(self.file_path): return True
        return False

class FileStorageParquet(IDataStorage):

    def __init__(self, file_path, file_schema:str):
        if not os.path.exists(file_path): os.makedirs(file_path)
        self.file_path = file_path + f'{file_schema}.parquet'
        self.schema_pq = SchemaRepository("parquet").get_schema(file_schema)
        logging.info("Saving to " + self.file_path)

    def create(self, data:DataFrame):
        try:
            table = pa.Table.from_pandas(data, schema=self.schema_pq)
            pq.write_table(table, self.file_path)
            return True
        except Exception as e:
            print(f"Error creating data: {e}")
            return False
    
    def read(self):
        try:
            table = pq.read_table(self.file_path)
            return table.to_pandas()
        except Exception as e:
            print(f"Error reading data: {e}")
            return None
    
    def update(self, data):
        try:
            existing_table = pq.read_table(self.file_path)
            t = self.check_duplicates(existing_table.to_pandas(), data)
            existing_table, new_table = t[0], t[1]

            if new_table.num_rows == 0: 
                return True

            combined_table = pa.concat_tables([existing_table, new_table])
            pq.write_table(combined_table, self.file_path)
            return True
        except Exception as e:
            print(f"Error updating data: {e}")
            return False

    
    def delete(self):
        try:
            # Delete the Parquet file
            import os
            os.remove(self.file_path)
            return True
        except FileNotFoundError:
            print("File not found")
            return False
        except Exception as e:
            print(f"Error deleting file: {e}")
            return False

    def check_duplicates(self, old:DataFrame, new:DataFrame):
        filter_dupl = new[~new["job_id"].isin(old["job_id"])].reset_index(drop=True)
        return [
            pa.Table.from_pandas(old, schema=self.schema_pq), 
            pa.Table.from_pandas(filter_dupl, schema=self.schema_pq)
        ]

    def get_file_extension(self):
        return os.path.splitext(self.file_path)[1]

    def is_exists(self) -> bool:
        if os.path.isfile(self.file_path): return True
        return False