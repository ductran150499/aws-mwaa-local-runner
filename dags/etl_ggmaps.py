from airflow import DAG
from ggmaps.crawl_locations import crawl_locations_tasks
from ggmaps.crawl_reviews import crawl_reviews_tasks
from ggmaps.transform import transform_tasks
from dags.ggmaps.load import load_tasks
from datetime import datetime

locations = [
    "Quận 1, TP. Hồ Chí Minh",
    "Quận 2, TP. Hồ Chí Minh",
    "Quận 3, TP. Hồ Chí Minh",
    "Quận 4, TP. Hồ Chí Minh",
    "Quận 5, TP. Hồ Chí Minh",
    "Quận 6, TP. Hồ Chí Minh",
    "Quận 7, TP. Hồ Chí Minh",
    "Quận 8, TP. Hồ Chí Minh",
    "Quận 9, TP. Hồ Chí Minh",
    "Quận 10, TP. Hồ Chí Minh",
    "Quận 11, TP. Hồ Chí Minh",
    "Quận 12, TP. Hồ Chí Minh",
    
    "Thủ Đức, TP. Hồ Chí Minh",
    
    "Bình Thạnh, TP. Hồ Chí Minh",
    "Phú Nhuận, TP. Hồ Chí Minh",
    "Tân Bình, TP. Hồ Chí Minh",
    "Tân Phú, TP. Hồ Chí Minh",
    "Gò Vấp, TP. Hồ Chí Minh",
    "Bình Tân, TP. Hồ Chí Minh",
]
categories = [
    "món ăn Việt Nam",
    "món ăn Hàn Quốc",
    "món ăn Nhật Bản",
    "món ăn Trung Quốc",
    "món ăn Thái Lan",
    "món ăn Ấn Độ",
    "món chay",
    "quán cà phê",
    "phở",
    "bún bò",
    "bánh mì",
    "bánh xèo",
    "bún chả Hà Nội",
    "bún thịt nướng",
    "bún riêu",
    "hủ tiếu",
    "mì Quảng",
    "cơm tấm",
    "gỏi cuốn",
    "nem nướng",
    "xôi",
    "bánh cuốn",
    "bánh canh",
    "gà nướng muối ớt",
    "lẩu bò",
    "lẩu gà",
    "lẩu cá",
    "lẩu mắm",
    "bò kho",
    "món gà",
    "chè",
    "bún đậu mắm tôm",
    "quán bia",
    "buffet",
    "quán nướng",
    "ốc",
]

with DAG(
        dag_id='ggmaps_pipeline',
        start_date=datetime(year=2024, 
                            month=9, 
                            day=28),
        schedule_interval='30 9 * * *',
        catchup=False) as dag:

    crawl_locations = crawl_locations_tasks(locations, categories)
    
    crawl_reviews = crawl_reviews_tasks()

    processing = transform_tasks()

    load = load_tasks()

    crawl_locations >> crawl_reviews >> processing >> load