from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv

# Setup WebDriver (make sure to specify the path to your ChromeDriver)
# setUpWebDriver
options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1200x10000')

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)


def scrape_google_maps(keyword, location, category, results_limit=50):
    keyword = f"{category} ở {location}"
    driver.get(f"https://www.google.com/maps/search/{keyword}")
    
    places_data = []
    
    wait = WebDriverWait(driver, 20)    
    wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")))
        
    place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
    
    for place in place_cards:
        try:
            # Extract place name and URL            
            place_url = place.get_attribute("href")
            
            # Name is aria-label of place element
            name = place.get_attribute("aria-label")
            
            # Append data (keyword, place URL, and address)
            places_data.append([category, location, keyword, name, place_url])
            
        except Exception as e:
            print(f"Error scraping place: {e}")
    
    return places_data

# Function to write data to CSV
def write_to_csv(data, filename="google_places_data_by_category_phase_1.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Category", "Location", "Keyword", "Name", "URL"])
        writer.writerows(data)

# Keywords to search for

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

# Loop through keywords and scrape data
all_data = []

for i in range(len(locations)):
        location = locations[i]
        for j in range(len(categories)):
            category = categories[j]
            progress = (i * len(categories) + j + 1) / (len(locations) * len(categories))
            print(f"Phase 1 - [{progress:.2%}]: Scraping {category} ở {location}")
            time.sleep(2)
            keyword = f"{category} ở {location}"
            places_data = scrape_google_maps(keyword, location, category)
            print(f"Found {len(places_data)} places")
            all_data.extend(places_data)
        ## backup
        write_to_csv(all_data)
write_to_csv(all_data)

driver.quit()

