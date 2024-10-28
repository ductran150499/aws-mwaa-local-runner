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

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1200x10000')

service = Service(ChromeDriverManager().install())

# Create a class to initiate the webdriver and set up the wait
class Scraper:
    def __init__(self):
        self.driver = webdriver.Chrome(service=service)
        self.wait = WebDriverWait(self.driver, 15)
    
    def close_driver(self):
        self.driver.quit()

# Hàm lấy thông tin về địa điểm (location_info)
def get_location_info(driver, wait):
    location_info = {}
    try:
        location_info['title'] = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf'))).text.strip()
        location_info['address'] = driver.find_element(By.CSS_SELECTOR, '.Io6YTe').text.strip()

    except Exception as e:
        print("Error fetching location info:", e)
    
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
def get_opening_hours(driver, wait):
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
        print("Error fetching opening hours:", e)
        return None

# Hàm lấy thông tin giá tiền
def get_price_info(driver, wait):
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
        print("Error fetching price info:", e)
        return None

# Hàm lấy số liệu thống kê đánh giá
def get_review_stats(driver, wait):
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
        print("Error fetching review stats:", e)
        return None

# Hàm để chuyển qua tab "About" và thu thập thông tin bổ sung
def get_additional_info(driver, wait):
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
        print("Error fetching additional info:", e)
        return None

def click_with_js(driver, element):
    driver.execute_script("arguments[0].click();", element)

def get_ratings(driver, wait, review):
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
        print(f"Error fetching ratings: {e}")

    return food_rating, service_rating, atmosphere_rating

def get_user_reviews(driver, wait, sort_type):
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

                food_rating, service_rating, atmosphere_rating = get_ratings(driver, wait, review)

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
                print(f"Error fetching individual review: {e}")
    
    except Exception as e:
        print("Error while fetching reviews:", e)
    
    return user_reviews

def get_reviews_data(url):
    scraper = Scraper()
    location_info = {}
    try:
        scraper.driver.get(url)

        location_info = get_location_info(scraper.driver, scraper.wait)

        # Tìm vĩ độ và kinh độ trong URL (thường nằm sau "@")
        time.sleep(3)
        current_url = scraper.driver.current_url
        lat_long = extract_lat_long(current_url)
        if lat_long:
            location_info['lat_long'] = lat_long

        opening_hours = get_opening_hours(scraper.driver, scraper.wait)
        if opening_hours:
            location_info['opening_hours'] = opening_hours
        
        # Lấy thông tin giá tiền
        price_info = get_price_info(scraper.driver, scraper.wait)
        if price_info:
            location_info['price_info'] = price_info

        # Lấy thông tin thống kê reviews
        review_stats = get_review_stats(scraper.driver, scraper.wait)
        if review_stats:
            location_info['review_stats'] = review_stats
        # ========

        # Lấy thông tin bổ sung từ tab "About"
        additional_info = get_additional_info(scraper.driver, scraper.wait)
        if additional_info:
            location_info['additional_info'] = additional_info
        # ========
        
        # Lấy thông tin danh sách review
        most_relevant_reviews = get_user_reviews(scraper.driver, scraper.wait, sort_type=0)
        if most_relevant_reviews:
            location_info['most_relevant_reviews'] = most_relevant_reviews

        newest_reviews = get_user_reviews(scraper.driver, scraper.wait, sort_type=1)
        if newest_reviews:
            location_info['newest_reviews'] = newest_reviews

        highest_rating_reviews = get_user_reviews(scraper.driver, scraper.wait, sort_type=2)
        if highest_rating_reviews:
            location_info['highest_rating_reviews'] = highest_rating_reviews

        lowest_rating_reviews = get_user_reviews(scraper.driver, scraper.wait, sort_type=3)
        if lowest_rating_reviews:
            location_info['lowest_rating_reviews'] = lowest_rating_reviews
    finally:
        scraper.close_driver()
        
    return location_info

def scrape_google_maps(keyword, location, category, results_limit=50):
    scraper = Scraper()
    keyword = f"{category} ở {location}"
    driver = scraper.driver
    wait = scraper.wait
    places_data = []
     
    try:
        driver.get(f"https://www.google.com/maps/search/{keyword}")
        
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")))
            
        place_cards = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'https://www.google.com/maps/place/')]")[:results_limit]
        
        for i in range(len(place_cards)):
            place = place_cards[i]
            try:
                place_url = place.get_attribute("href")
                name = place.get_attribute("aria-label")
                
                scraper.close_driver()
                # Print get detail data, with progress
                print(f" - [{i+1}/{len(place_cards)}] Scraping detail of {name}")
                location_info = get_reviews_data(place_url)
                places_data.append({
                    "category": category,
                    "location": location,
                    "keyword": keyword,
                    "name": name,
                    "url": place_url,
                    "location_info": location_info
                })            
            except Exception as e:
                print(f"Error scraping place: {e}")
    finally:
        scraper.close_driver()
    return places_data

locations = [
    "Quận 1, TP. Hồ Chí Minh",
    # "Quận 3, TP. Hồ Chí Minh",
    # "Quận 4, TP. Hồ Chí Minh",
    # "Quận 5, TP. Hồ Chí Minh",
    # "Quận 6, TP. Hồ Chí Minh",
    # "Quận 7, TP. Hồ Chí Minh",
    # "Quận 8, TP. Hồ Chí Minh",
    # "Quận 10, TP. Hồ Chí Minh",
    # "Quận 11, TP. Hồ Chí Minh",
    # "Quận 12, TP. Hồ Chí Minh",
    
    # "TP. Thủ Đức, TP. Hồ Chí Minh",
    
    # "Quận Bình Thạnh, TP. Hồ Chí Minh",
    # "Quận Phú Nhuận, TP. Hồ Chí Minh",
    # "Quận Tân Bình, TP. Hồ Chí Minh",
    # "Quận Tân Phú, TP. Hồ Chí Minh",
    # "Quận Gò Vấp, TP. Hồ Chí Minh",
    # "Quận Bình Tân, TP. Hồ Chí Minh",
    
    # "Huyện Bình Chánh, TP. Hồ Chí Minh",
    # "Huyện Củ Chi, TP. Hồ Chí Minh",
    # "Huyện Hóc Môn, TP. Hồ Chí Minh",
    # "Huyện Nhà Bè, TP. Hồ Chí Minh",
    # "Huyện Cần Giờ, TP. Hồ Chí Minh",
]
categories = [
    "nhà hàng",
    # "món Việt",
    # "món Hàn",
    # "món Nhật",
    # "món Trung",
    # "món Thái",
    # "món Âu",
    # "món Ấn",
    # "món chay",
    # "cà phê",
    # "phở",
    # "bánh mì",
    # "bún riêu",
    # "bún bò",
    # "bún chả",
    # "bún đậu mắm tôm",
    # "bún mắm",
    # "bún thịt nướng",
    # "quán bia",
    # "buffet",
    # "quán ăn",
    # "quán nhậu",
    # "quán nướng",
    # "quán lẩu",
    # "quán cơm",
    # "quán chay",
    # "quán ăn vặt",
    # "quán trà sữa",
    # "quán kem",
    # "quán ốc",
    # "quán chè",
] 

# Loop through keywords and scrape data
all_data = []

for i in range(len(locations)):
        category_data = []
        location = locations[i]
        for j in range(len(categories)):
            category = categories[j]
            current = i * len(categories) + j + 1
            total = len(locations) * len(categories)
            print(f"[{current}/{total}]:{(current/total):.2%} Scraping {category} ở {location}")
            keyword = f"{category} ở {location}"
            places_data = scrape_google_maps(keyword, location, category)
            category_data.extend(places_data)
        
            with open(f"ggmaps_data_{location}_{category}.json", "w", encoding='utf8') as f:
                json.dump(places_data, f, indent=4, ensure_ascii=False)
        
        all_data.extend(category_data)
        
        with open(f"ggmaps_data_{location}.json", "w", encoding='utf8') as f:
            json.dump(category_data, f, indent=4, ensure_ascii=False)

with open(f"ggmaps_data_final.json", "w", encoding='utf8') as f:
    json.dump(all_data, f, indent=4, ensure_ascii=False)
        

