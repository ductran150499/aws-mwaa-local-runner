from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import csv
import json
import re
from unidecode import unidecode

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1200x10000')

service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service)
wait = WebDriverWait(driver, 15)

def get_location_info():
    location_info = {}
    try:
        location_info['title'] = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf'))).text.strip()
        location_info['address'] = driver.find_element(By.CSS_SELECTOR, '.Io6YTe').text.strip()

    except Exception as e:
        time.sleep(2)
        print("Error fetching location info")
    
    return location_info

def extract_lat_long(url):
    match = re.search(r'@([\d\.-]+),([\d\.-]+)', url)
    if match:
        latitude = match.group(1)
        longitude = match.group(2)
        return {"latitude": latitude, "longitude": longitude}
    else:
        return None

# Hàm lấy thông tin giờ mở cửa
def get_opening_hours():
    try:
        expand_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'span[aria-label="Show open hours for the week"]')))
        expand_button.click()
        time.sleep(2)

        open_hours_element = driver.find_element(By.CSS_SELECTOR, 'div.t39EBf.GUrTXd')
        
        aria_label_text = open_hours_element.get_attribute('aria-label')

        days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        hour_info_list = aria_label_text.split(';')

        hour_info_list[-1] = hour_info_list[-1].split(". Hide open hours for the week")[0]

        opening_hours = {}

        for i, day in enumerate(days_of_week):
            if i < len(hour_info_list):
                day_info = hour_info_list[i].strip()
                day_name, hours = day_info.split(", ")
                opening_hours[day_name] = hours

        return opening_hours

    except Exception as e:
        print("Error fetching opening hours")
        return None

# Hàm lấy thông tin giá tiền
def get_price_info():
    try:
        expand_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.IgTTAf')))
        expand_button.click()
        time.sleep(2)

        price_rows = driver.find_elements(By.CSS_SELECTOR, 'table.rqRH4d tr')

        price_info_list = []

        for row in price_rows:
            price_range = row.find_element(By.CSS_SELECTOR, 'td.fsAi0e').text.strip()

            percentage = row.find_element(By.CSS_SELECTOR, 'span.xYsBQe').get_attribute('style')
            percentage_value = re.search(r'width: (\d+)%', percentage).group(1) + '%'

            price_info = {
                "currency": "đ",
                "price_range": price_range,
                "percentage": percentage_value
            }
            price_info_list.append(price_info)

        return price_info_list

    except Exception as e:
        print("Error fetching price info")
        return None

# Hàm lấy số liệu thống kê đánh giá
def get_review_stats():
    try:
        rate = driver.find_element(By.CSS_SELECTOR, 'div.jANrlb div.fontDisplayLarge').text.strip()

        reviews = driver.find_element(By.CSS_SELECTOR, 'button.HHrUdb.fontTitleSmall.rqjGif span').text.strip()
        reviews = reviews.split()[0]  

        star_rows = driver.find_elements(By.CSS_SELECTOR, 'tr.BHOKXe')
        detail = {}

        for row in star_rows:
            stars = row.find_element(By.CSS_SELECTOR, 'td.yxmtmf.fontBodyMedium').text.strip() + " stars"

            aria_label = row.get_attribute('aria-label')
            num_reviews = aria_label.split(', ')[1]  
            detail[stars] = num_reviews

        review_stats = {
            "rate": rate,
            "reviews": reviews,
            "detail": detail
        }

        return review_stats

    except Exception as e:
        print("Error fetching review stats")
        return None

# Hàm để chuyển qua tab "About" và thu thập thông tin bổ sung
def get_additional_info():
    try:
        about_tab = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@data-tab-index="2"]')))
        click_with_js(driver, about_tab)
        
        time.sleep(2)

        sections = driver.find_elements(By.CSS_SELECTOR, 'div.iP2t7d.fontBodyMedium')
        additional_info = {}

        for section in sections:
            title = section.find_element(By.CSS_SELECTOR, 'h2.iL3Qke.fontTitleSmall').text.strip()

            items = section.find_elements(By.CSS_SELECTOR, 'ul.ZQ6we li.hpLkke span[aria-label]')
            item_list = [item.get_attribute('aria-label') for item in items]

            additional_info[title] = item_list

        return additional_info

    except Exception as e:
        print("Error fetching additional info")
        return None

def click_with_js(driver, element):
    driver.execute_script("arguments[0].click();", element)

def get_ratings(review):
    food_rating = 'N/A'
    service_rating = 'N/A'
    atmosphere_rating = 'N/A'

    try:
        rating_container = review.find_element(By.CSS_SELECTOR, 'div[jslog="127691"]')
        
        try:
            food_rating = rating_container.find_element(By.XPATH, './/span[b[text()="Food:"]]').text.split(':')[-1].strip()
        except Exception as e:
            print("Food rating not found")

        try:
            service_rating = rating_container.find_element(By.XPATH, './/span[b[text()="Service:"]]').text.split(':')[-1].strip()
        except Exception as e:
            print("Service rating not found")

        try:
            atmosphere_rating = rating_container.find_element(By.XPATH, './/span[b[text()="Atmosphere:"]]').text.split(':')[-1].strip()
        except Exception as e:
            print("Atmosphere rating not found")

    except Exception as e:
        print(f"Error fetching ratings")

    return food_rating, service_rating, atmosphere_rating

def get_user_reviews(sort_type):
    user_reviews = []

    try:
        reviews_tab = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@data-tab-index="1"]')))
        click_with_js(driver, reviews_tab)        
        time.sleep(2)

        sort_button = driver.find_element(By.CSS_SELECTOR, 'button.g88MCb.S9kvJb[aria-label="Sort reviews"]')
        sort_button.click()
        time.sleep(3) 

        sort_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'div.fxNQSd[data-index="{sort_type}"]'))
        )
        sort_option.click()

        time.sleep(3)

        actions = ActionChains(driver)
        scroll_attempts = 0
        max_scroll_attempts = 3  
        total_review_container = []

        while scroll_attempts < max_scroll_attempts:
            actions.send_keys(Keys.PAGE_DOWN).perform()  
            time.sleep(2)  

            new_review_container = driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf.fontBodyMedium')
            print("Len new_review_container:", len(new_review_container))

            if len(new_review_container) > 0:
                total_review_container.extend(new_review_container)  
            scroll_attempts += 1  
        
        print("Len  total_review_container:", len(total_review_container))

        for review in total_review_container:
            try:
                try:
                    see_more_button = review.find_element(By.CSS_SELECTOR, 'button.w8nwRe.kyuRq[aria-label="See more"]')
                    if see_more_button.is_displayed() and see_more_button.is_enabled():
                        see_more_button.click()
                        time.sleep(1)
                except Exception as e:
                    print("See more button not found or not clickable")

                food_rating, service_rating, atmosphere_rating = get_ratings(review)

                user_reviews.append({
                    'name': review.find_element(By.CSS_SELECTOR, '.d4r55').text.strip(),
                    'time': review.find_element(By.CSS_SELECTOR, '.rsqaWe').text.strip(),
                    'rating': review.find_element(By.CSS_SELECTOR, '.kvMYJc').get_attribute('aria-label'),
                    'food': food_rating,
                    'service': service_rating,
                    'atmosphere': atmosphere_rating,
                    'review': review.find_element(By.CSS_SELECTOR, 'span.wiI7pd').text.strip(), 
                })
            except Exception as e:
                print(f"Error fetching individual review")
    
    except Exception as e:
        print("Error while fetching reviews:")
    
    return user_reviews

def get_reviews_data(url):
    location_info = {}
    try:
        driver.get(url)

        location_info = get_location_info()

        # Tìm vĩ độ và kinh độ trong URL (thường nằm sau "@")
        time.sleep(3)
        current_url = driver.current_url
        lat_long = extract_lat_long(current_url)
        if lat_long:
            location_info['lat_long'] = lat_long

        opening_hours = get_opening_hours()
        if opening_hours:
            location_info['opening_hours'] = opening_hours
        
        # Lấy thông tin giá tiền
        price_info = get_price_info()
        if price_info:
            location_info['price_info'] = price_info

        # Lấy thông tin thống kê reviews
        review_stats = get_review_stats()
        if review_stats:
            location_info['review_stats'] = review_stats
        # ========

        # Lấy thông tin bổ sung từ tab "About"
        additional_info = get_additional_info()
        if additional_info:
            location_info['additional_info'] = additional_info
        # ========
        
        # Lấy thông tin danh sách review
        most_relevant_reviews = get_user_reviews(sort_type=0)
        if most_relevant_reviews:
            location_info['most_relevant_reviews'] = most_relevant_reviews

        newest_reviews = get_user_reviews(sort_type=1)
        if newest_reviews:
            location_info['newest_reviews'] = newest_reviews

        highest_rating_reviews = get_user_reviews(sort_type=2)
        if highest_rating_reviews:
            location_info['highest_rating_reviews'] = highest_rating_reviews

        lowest_rating_reviews = get_user_reviews(sort_type=3)
        if lowest_rating_reviews:
            location_info['lowest_rating_reviews'] = lowest_rating_reviews
    except Exception as e:
        print("Error fetching reviews data")
        
    return location_info

def scrape_google_maps(location, category, results_limit=20):
    keyword = f"{category} ở {location}"
    places_data = []
     
    driver.get(f"https://www.google.com/maps/search/{keyword}")
    
    wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")))
        
    place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
    
    print(f"Found {len(place_cards)} places")
    
    for i in range(len(place_cards)):
        place = place_cards[i]
        place_url = place.get_attribute("href")
        name = place.get_attribute("aria-label")
        
        # Print get detail data, with progress
        places_data.append({
            "category": category,
            "location": location,
            "keyword": keyword,
            "name": name,
            "url": place_url,
        })
    
    results = []
    
    for i in range(len(places_data)):
        place = places_data[i]
        place_url = place["url"]
        print(f" - [{i+1}/{len(places_data)}] Scraping detail of {place['name']}")
        location_info = get_reviews_data(place_url)
        results.append({
            **place,
            "location_info": location_info
        })
    return results

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

# Loop through keywords and scrape data
all_data = []

def scrapped_location(location):
    # check if file exists
    name = unidecode(f"data/ggmaps_data_{location}.json")
    
    try:
        with open(name, 'r', encoding='utf8') as f:
            return True
    except:
        return False
    
def scrapped_category(location, category):
    name = unidecode(f"data/ggmaps_data_{location}_{category}.json")
    
    try:
        with open(name, 'r', encoding='utf8') as f:
            return True
    except:
        return False
    

for i in range(len(locations)):
        category_data = []
        location = locations[i]
        if (scrapped_location(location)):
            print(f"Location {location} already scrapped")
            scrapped_data = json.load(open(unidecode(f"data/ggmaps_data_{location}.json"), 'r', encoding='utf8'))
            all_data.extend(scrapped_data)
            continue
        
        for j in range(len(categories)):
            category = categories[j]
            
            if (scrapped_category(location, category)):
                print(f"Category {category} at {location} already scrapped")
                scrapped_data = json.load(open(unidecode(f"data/ggmaps_data_{location}_{category}.json"), 'r', encoding='utf8'))
                category_data.extend(scrapped_data)
                continue
            
            current = i * len(categories) + j + 1
            total = len(locations) * len(categories)
            print(f"[{current}/{total}]:{(current/total):.2%} Scraping {category} ở {location}")
            places_data = scrape_google_maps(location, category)
            category_data.extend(places_data)
        
            with open(unidecode(f"data/ggmaps_data_{location}_{category}.json"), "w", encoding='utf8') as f:
                json.dump(places_data, f, indent=4, ensure_ascii=False)
        
        all_data.extend(category_data)
        
        with open(unidecode(f"data/ggmaps_data_{location}.json"), "w", encoding='utf8') as f:
            json.dump(category_data, f, indent=4, ensure_ascii=False)

with open(f"data/ggmaps_data_final.json", "w", encoding='utf8') as f:
    json.dump(all_data, f, indent=4, ensure_ascii=False)
        

driver.quit()
