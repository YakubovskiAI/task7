import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from pymongo import MongoClient
import os

file_path = "/opt/airflow/data/tiktok_google_play_reviews.csv"
dataset = Dataset(file_path)

def read_data() -> pd.DataFrame:
    return pd.read_csv(file_path)

def save_data(df: pd.DataFrame):
    df.to_csv(file_path, index=False)


@dag(start_date=datetime(2024, 7, 15), catchup=False)
def data_processing_dag():

    file_sensor = FileSensor(
        task_id="file_sensor", filepath=file_path, poke_interval=10, timeout=600
    )

    @task.branch
    def is_empty_file():
        if os.path.getsize(file_path) > 0:
            return "process_data_task_group.replace_null_values"
        else:
            return "logging_empty_file_task"

    @task.bash
    def logging_empty_file_task():
        return f"echo {file_path} is empty"

    @task_group
    def process_data_task_group():

        @task
        def replace_null_values():
            df = read_data()
            df.fillna("-", inplace=True)
            save_data(df)

        @task
        def sort_by_date():
            df = read_data()
            df.sort_values(by="at", inplace=True)
            save_data(df)

        @task(outlets=dataset)
        def clean_content():
            df = read_data()
            df["content"] = df["content"].str.replace(
                r"[^a-zA-Z0-9\s.,!?]", "", regex=True
            )
            save_data(df)

        replace_null_values() >> sort_by_date() >> clean_content()

    _is_empty_file = is_empty_file()

    (
        file_sensor
        >> _is_empty_file
        >> [logging_empty_file_task(), process_data_task_group()]
    )


data_processing_dag = data_processing_dag()


@dag(schedule=[dataset], start_date=datetime(2024, 7, 15), catchup=False)
def data_loading_mongo_dag():
    @task(execution_timeout = timedelta(minutes=60))
    def data_load_mongo_task():
        client = MongoClient(f'mongodb://{'root'}:{'example'}@127.0.0.1')
        db = client["db1"]
        collection = db["collection1"]
        
        df = read_data()
        data_dict = df.to_dict("records")

        batch_size = 1000
        for i in range(0, len(data_dict), batch_size):
            batch = data_dict[i:i+batch_size]
            collection.insert_many(batch)

    data_load_mongo_task()


data_loading_mongo_dag = data_loading_mongo_dag()
