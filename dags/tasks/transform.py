import pandas as pd

def transform(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    df = pd.DataFrame(extracted_data)
    df['age'] = df['age'] + 1  
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict())  
    print("Transform complete!")
