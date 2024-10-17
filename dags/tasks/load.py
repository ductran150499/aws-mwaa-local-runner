import pandas as pd

def load(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    df = pd.DataFrame(transformed_data)
    print("Load complete! Data loaded: \n", df)
