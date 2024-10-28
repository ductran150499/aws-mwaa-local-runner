from pymongo import MongoClient
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def clear_collection() -> None:
    """
    Clears the 'locations' collection in MongoDB before loading new data.
    
    Returns:
        None
    """
    client = MongoClient('mongodb://admin:admin@mongo:27017/')
    db = client['admin']
    collection = db['locations']
    
    delete_result = collection.delete_many({})
    print(f"Deleted {delete_result.deleted_count} records from MongoDB.")

def load_to_mongodb(ti) -> None:
    """
    Loads processed data into MongoDB.

    Args:
        ti: Task Instance to access XCom.

    Returns:
        None
    """
    # Establish a connection to MongoDB
    client = MongoClient('mongodb://admin:admin@mongo:27017/')  # Update connection information if necessary
    db = client['admin']  # Specify the database name
    collection = db['locations']  # Specify the collection name

    # Retrieve processed data from XCom
    unique_entries = ti.xcom_pull(key='transformed_data', task_ids='transform.remove_duplicates')  # Update key and task_ids

    # Check if unique_entries is None
    if unique_entries is None:
        print("No unique entries found in XCom.")
        return
    
    # Prepare data for MongoDB insertion
    documents = []
    for key, values in unique_entries.items():
        document = {
            'type': key,
            'details': values
        }
        documents.append(document)

    # Insert data into MongoDB
    if documents:
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} records into MongoDB.")
    else:
        print("No data to insert into MongoDB.")

def load_tasks():
    # Create a task group for loading tasks
    with TaskGroup(
            group_id="load",
            tooltip="Load processed data to MongoDB"
    ) as group:
        
        # Define the task for clearing the MongoDB collection
        clear_collection_task = PythonOperator(
            task_id='clear_collection',
            python_callable=clear_collection
        )

        # Define the task for loading data into MongoDB
        load_to_mongodb_task = PythonOperator(
            task_id='load_to_mongodb',
            python_callable=load_to_mongodb,
            provide_context=True  # Ensure that Airflow provides the context for the task
        )

        clear_collection_task >> load_to_mongodb_task

        return group
