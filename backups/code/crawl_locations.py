from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from driver_utils import create_driver

import time as time
import csv

def scrape_google_maps(driver, location, category, results_limit=20):
    places_data = []
    try:
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
    return places_data

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

def run():
    all_data = []
    driver = create_driver()
    with open("backups/code/data/locations.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["category", "location", "keyword", "name", "url"]) 
        
        for i in range(len(locations)):
            location = locations[i]
            for j in range(len(categories)):
                category = categories[j]
                progress = (i * len(categories) + j + 1) / (len(locations) * len(categories))
                print(f"({progress:.2%}): Scraping {category} ở {location}")
                places_data = scrape_google_maps(driver, location, category)
                all_data.extend(places_data)
                print(f"Found {len(places_data)} places")
                
                for data in places_data:
                    writer.writerow(data)
    driver.quit()

if __name__ == "__main__":
    run()