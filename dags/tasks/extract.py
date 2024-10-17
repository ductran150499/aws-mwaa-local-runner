import pandas as pd

def extract(**kwargs):
    data = {'name': ['John', 'Jane', 'Jim'], 'age': [28, 34, 29]}
    df = pd.DataFrame(data)
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_dict())  
    print("Extract complete!")

