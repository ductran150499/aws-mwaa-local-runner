from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tasks.extract import extract_data
from tasks.transform import transform_data
from tasks.load import load_data

from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run every 30 mins
    schedule_interval="@monthly",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    tags=['D2TH']) # If set, this tag is shown in the DAG view of the Airflow UI
def etl_google_maps_pipeline():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
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

    
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

etl_google_maps_pipeline = etl_google_maps_pipeline()