import logging
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def remove_duplicates(ti) -> dict:
    """
    Removes duplicate entries from crawled data based on a composite unique key
    (title and address) and pushes the unique entries to XCom as a dictionary.

    Args:
        ti: Task Instance to access XCom.

    Returns:
        dict: Dictionary of unique entries from crawled data.
    """
    # Pull data from XCom
    data = ti.xcom_pull(key='crawl_reviews', task_ids='crawling_reviews.get_url')

    if not data:
        logging.warning("No data found in XCom.")
        return {}

    unique_data = {}
    seen = set()

    # Iterate over each item in the data
    for item in data:
        # Create a composite unique key from title and address
        unique_key = f"{item.get('title')}_{item.get('address')}"

        if unique_key not in seen:
            seen.add(unique_key)
            unique_data[unique_key] = item
        else:
            logging.info(f"Duplicate entry found for: {unique_key}")

    logging.info(f"Original entries: {len(data)}, Unique entries: {len(unique_data)}")
    logging.info("Data after transformation: %s", unique_data)

    # Push unique data to XCom for further use
    ti.xcom_push(key='transformed_data', value=unique_data)

    return unique_data

def transform_tasks():
    with TaskGroup(
            group_id="transform",
            tooltip="Transform data"
    ) as group:

        remove_duplicates_task = PythonOperator(
            task_id='remove_duplicates',
            python_callable=remove_duplicates,
            provide_context=True  # Ensure context is provided to access `ti`
        )

        return group