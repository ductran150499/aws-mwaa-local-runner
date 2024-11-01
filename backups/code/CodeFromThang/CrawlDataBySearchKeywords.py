from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import csv

options = webdriver.ChromeOptions()
options.add_argument('headless')  # Make browser open in background
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

def scrape_google_maps(keyword, location, category, results_limit):
    base_url = "https://www.google.com/maps/"
    driver.get(base_url)
    time.sleep(5)
    
    search_bar = driver.find_element(By.ID, "searchboxinput")
    search_bar.clear()
    search_bar.send_keys(keyword)
    time.sleep(1)
    search_bar.send_keys(Keys.RETURN)
    time.sleep(5)  

    places_data = []
    max_scroll_attempts = 30 

    for scroll_attempt in range(max_scroll_attempts):
        # Lấy danh sách thẻ địa điểm
        place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")

        print(f"Số lượng thẻ địa điểm: {len(place_cards)}")

        for place in place_cards:
            try:
                place_url = place.get_attribute("href")
                name = place.get_attribute("aria-label")
                
                if name and place_url and [category, location, keyword, name, place_url] not in places_data:
                    places_data.append([category, location, keyword, name, place_url])

            except Exception as e:
                print(f"Error scraping place: {e}")

        if place_cards:
            driver.execute_script("arguments[0].scrollIntoView();", place_cards[-1])
            time.sleep(2)  
        
        if len(places_data) >= results_limit:
            break

    return places_data

# Function to write data to CSV
def write_to_csv(data, filename="google_places_data_by_category.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Category", "Location", "Keyword", "Name", "URL"])
        writer.writerows(data)

locations = ["Quận 1, TP. Hồ Chí Minh"]
categories = ["phở"] 
results_limit = 50
all_data = []

# loop through all locations and categories to construct the keywords
for location in locations:
    for category in categories:
        keyword = f"{category} ở {location}"
        print(f"Scraping data for: {keyword}")
        places_data = scrape_google_maps(keyword, location, category, results_limit)
        print(f"Found {len(places_data)} places")
        all_data.extend(places_data)

# Save the data to CSV
write_to_csv(all_data)

# Close the browser
driver.quit()

print("Data scraping completed successfully!")