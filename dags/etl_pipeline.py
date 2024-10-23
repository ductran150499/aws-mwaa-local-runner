from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
from tasks.extract import extract as extract_data
from tasks.transform import transform as transform_data
from tasks.load import load as load_data

@dag(
    schedule_interval="@monthly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['D2TH']
)
def etl_google_maps_pipeline():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    """

    @task()
    def extract():
        """
        #### Extract task
        This task extracts data by calling the extract_data function.
        """
        return extract_data()

    @task()
    def transform(data):
        """
        #### Transform task
        This task transforms the extracted data by calling the transform_data function.
        """
        return transform_data(data)

    @task()
    def load(data):
        """
        #### Load task
        This task loads the transformed data by calling the load_data function.
        """
        load_data(data)

    # Định nghĩa luồng giữa các task
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

etl_google_maps_pipeline = etl_google_maps_pipeline()
