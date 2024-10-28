from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time as time

locations = [
    "Quận 1, TP. Hồ Chí Minh",
    "Quận 3, TP. Hồ Chí Minh",
    "Quận 4, TP. Hồ Chí Minh",
    "Quận 5, TP. Hồ Chí Minh",
    "Quận 6, TP. Hồ Chí Minh",
    "Quận 7, TP. Hồ Chí Minh",
    "Quận 8, TP. Hồ Chí Minh",
    "Quận 10, TP. Hồ Chí Minh",
    "Quận 11, TP. Hồ Chí Minh",
    "Quận 12, TP. Hồ Chí Minh",
    
    "TP. Thủ Đức, TP. Hồ Chí Minh",
    
    "Quận Bình Thạnh, TP. Hồ Chí Minh",
    "Quận Phú Nhuận, TP. Hồ Chí Minh",
    "Quận Tân Bình, TP. Hồ Chí Minh",
    "Quận Tân Phú, TP. Hồ Chí Minh",
    "Quận Gò Vấp, TP. Hồ Chí Minh",
    "Quận Bình Tân, TP. Hồ Chí Minh",
    
    "Huyện Bình Chánh, TP. Hồ Chí Minh",
    "Huyện Củ Chi, TP. Hồ Chí Minh",
    "Huyện Hóc Môn, TP. Hồ Chí Minh",
    "Huyện Nhà Bè, TP. Hồ Chí Minh",
    "Huyện Cần Giờ, TP. Hồ Chí Minh",
]
categories = [
    "nhà hàng",
    "món Việt",
    "món Hàn",
    "món Nhật",
    "món Trung",
    "món Thái",
    "món Âu",
    "món Ấn",
    "món chay",
    "cà phê",
    "phở",
    "bánh mì",
    "bún riêu",
    "bún bò",
    "bún chả",
    "bún đậu mắm tôm",
    "bún mắm",
    "bún thịt nướng",
    "quán bia",
    "buffet",
    "quán ăn",
    "quán nhậu",
    "quán nướng",
    "quán lẩu",
    "quán cơm",
    "quán chay",
    "quán ăn vặt",
    "quán trà sữa",
    "quán kem",
    "quán ốc",
    "quán chè",
] 

def scrape_google_maps(location, category, results_limit=10):
    places_data = []
    try:
        firefox_options = Options()
        firefox_options.add_argument("--headless")
        driver = webdriver.Remote(
            command_executor='http://remote_firefoxdriver:4444/wd/hub',
            options=firefox_options
        )
        time.sleep(5)
        keyword = f"{category} ở {location}"
        driver.get(f"https://www.google.com/maps/search/{keyword}")
        wait = WebDriverWait(driver, 20)
        
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")))
        
        place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
        
        for place in place_cards:
            try:
                place_url = place.get_attribute("href")
                name = place.get_attribute("aria-label")
                places_data.append([category, location, keyword, name, place_url])       
            except Exception as e:
                print(f"Error scraping place: {e}")
    except Exception as e:
        print(f"Error scraping Google Maps: {e}")
    finally:
        driver.quit()
    return places_data

def get_url(ti) -> None:    
    all_data = []
    for i in range(len(locations)):
        location = locations[i]
        for j in range(len(categories)):
            category = categories[j]
            progress = (i * len(categories) + j + 1) / (len(locations) * len(categories))
            print(f"({progress:.2%}): Scraping {category} ở {location}")
            places_data = scrape_google_maps(location, category)
            print(f"Found {len(places_data)} places")
            all_data.extend(places_data)
    # Push data to XCom
    ti.xcom_push(key='crawl', value=all_data)

def crawl_tasks():
    with TaskGroup(
            group_id="crawling",
            tooltip="Crawling Google Maps"
    ) as group:

        get_url_task = PythonOperator(
            task_id='get_url',
            python_callable=get_url,
            provide_context=True  # Ensure context is provided to access `ti`
        )

        return group
