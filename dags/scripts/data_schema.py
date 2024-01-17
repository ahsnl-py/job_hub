from typing import Dict, List
import pandas as pd
import pyarrow as pa

class SchemaFactory:

    def __init__(self, schema:Dict[str, str], output:str) -> None:
        self.schema = schema
        self.output = output

    def create_schema(self):
        if self.output == "pandas":
            return self.get_pandas_df()
        if self.output == "parquet":
            return self.get_parquet_df()

    def get_pandas_df(self):
        df_schema = pd.DataFrame(columns=self.schema.keys())
        # Convert data types based on schema
        for col, dtype in self.schema.items():
            df_schema[col] = df_schema[col].astype(dtype)
        return df_schema

    def get_parquet_df(self):
        return pa.schema([(col, self.parquet_dtypes_converter(dtype)) for col, dtype in self.schema.items()])

    def parquet_dtypes_converter(self, dtype:str):
        types = {
            "string": pa.string(),
            "object": pa.string(),  
            "int64": pa.int64(),
            "int32": pa.int32(),
            "float64": pa.float64(),
            "float32": pa.float32(),
            "bool": pa.bool_(),
            "datetime64[ns]": pa.timestamp('ns')
        }
        return types[dtype]

class SchemaRepository:

    def __init__(self, output:str):
        self.output = output

    def get_schema(self, schema:str):
        # Check if the function exists as an attribute of the class
        if hasattr(self, schema) and callable(getattr(self, schema)):
            # Get the function by its name and execute it
            get_request_schema = getattr(self, schema)
            return SchemaFactory(
                schema=get_request_schema(), 
                output=self.output).create_schema()
        else:
            print(f"Schema '{schema}' does not exist")


    def jobcz(self) -> Dict[str, str]:
        return {
            'load_date': 'string',
            'job_id': 'int64',
            'title': 'string',
            'link': 'string',
            'date_added': 'string',
            'salary': 'string',
            'company': 'string',
            'city': 'string',
            'district': 'string',
            'work_from_home': 'string',
            'response_period': 'string',
            'rating': 'string',
            'other_details': 'string'
        }


    def job_detail(self) -> Dict[str, str]:
        return {
            'date': 'string',
            'job_id': 'int64',
            # 'link': 'string',
            'job_about': 'string',
            'job_req': 'string',
            'job_desc': 'string'
        }