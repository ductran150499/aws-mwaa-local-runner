from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ggmaps.driver_utils import create_driver

import time as time

def scrape_google_maps(location, category, results_limit=5):
    places_data = []
    try:
        driver = create_driver()
        keyword = f"{category} ở {location}"
        driver.get(f"https://www.google.com/maps/search/{keyword}")
        time.sleep(2)
        
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

def get_url(locations, categories, ti) -> None:    
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
    ti.xcom_push(key='crawl_locations', value=all_data)

def crawl_locations_tasks(locations, categories):
    with TaskGroup(
            group_id="crawling_locations",
            tooltip="Crawling Google Maps Locations"
    ) as group:

        get_url_task = PythonOperator(
            task_id='get_url',
            python_callable=get_url,
            provide_context=True,
            op_args=[locations, categories]
        )

        return group
