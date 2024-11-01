from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context

def remove_duplicates(ti) -> dict:
    """
    Removes duplicate entries from crawled data based on a unique key
    and pushes the unique entries to XCom as a dictionary.

    Args:
        ti: Task Instance to access XCom.

    Returns:
        dict: Dictionary of unique entries from crawled data.
    """
    # Retrieve data from XCom
    data = ti.xcom_pull(key='crawl_reviews', task_ids='crawling_reviews.get_url')

    if data is None or not data:
        print("No data found in XCom.")
        return {}

    unique_data = {}
    seen = set()

    # Giả sử mỗi item trong data là một danh sách và phần tử đầu tiên là id
    for item in data:
        # Thay 'item[0]' bằng chỉ số thực tế cho khóa duy nhất
        unique_key = item[0]  # Giả sử phần tử đầu tiên là khóa duy nhất

        if unique_key not in seen:  # Kiểm tra xem khóa đã được thấy chưa
            seen.add(unique_key)
            unique_data[unique_key] = item  # Lưu item vào dictionary với khóa duy nhất

    print(f"Original entries: {len(data)}, Unique entries: {len(unique_data)}")
    print("Data after transformation:")
    print(f"Unique entries: {unique_data}")  # In ra các entry duy nhất

    # Push unique data to XCom for further use
    ti.xcom_push(key='transformed_data', value=unique_data)

    return unique_data  # Trả về dữ liệu duy nhất để xử lý thêm nếu cần

def transform_tasks():
    with TaskGroup(
            group_id="transform",
            tooltip="Transform data"
    ) as group:

        remove_duplicates_task = PythonOperator(
            task_id='remove_duplicates',
            python_callable=remove_duplicates,
            provide_context=True  # Đảm bảo ngữ cảnh được cung cấp để truy cập `ti`
        )

        return group
