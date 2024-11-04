from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

from driver_utils import create_driver

import time as time
import re
import csv
import json
import os

import argparse

parser = argparse.ArgumentParser("Run crawlings by chunks")
parser.add_argument("chunk", help="An integer: [0, 1, 2, 3]", type=int)
args = parser.parse_args()

def get_location_info(driver, wait):
    location_info = {}
    try:
        location_info['title'] = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf'))).text.strip()
        location_info['address'] = driver.find_element(By.CSS_SELECTOR, '.Io6YTe').text.strip()

    except Exception as e:
        pass
    
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
            pass

        try:
            service_rating = rating_container.find_element(By.XPATH, './/span[b[text()="Service:"]]').text.split(':')[-1].strip()
        except Exception as e:
            pass
        try:
            atmosphere_rating = rating_container.find_element(By.XPATH, './/span[b[text()="Atmosphere:"]]').text.split(':')[-1].strip()
        except Exception as e:
            pass
    except Exception as e:
        pass

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

            if len(new_review_container) > 0:
                total_review_container.extend(new_review_container)  
            scroll_attempts += 1  

        for review in total_review_container:
            try:
                try:
                    see_more_button = review.find_element(By.CSS_SELECTOR, 'button.w8nwRe.kyuRq[aria-label="See more"]')
                    if see_more_button.is_displayed() and see_more_button.is_enabled():
                        see_more_button.click()
                        time.sleep(1)
                except Exception as e:
                    pass

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
                pass
    
    except Exception as e:
        pass
    
    return user_reviews

def get_reviews_data(url, driver, wait):  
    driver.get(url)

    location_info = get_location_info(driver, wait)

    # Tìm vĩ độ và kinh độ trong URL (thường nằm sau "@")
    time.sleep(3)
    current_url = driver.current_url
    lat_long = extract_lat_long(current_url)
    if lat_long:
        location_info['lat_long'] = lat_long

    opening_hours = get_opening_hours(driver, wait)
    if opening_hours:
        location_info['opening_hours'] = opening_hours
    
    # Lấy thông tin giá tiền
    price_info = get_price_info(driver, wait)
    if price_info:
        location_info['price_info'] = price_info

    # Lấy thông tin thống kê reviews
    review_stats = get_review_stats(driver, wait)
    if review_stats:
        location_info['review_stats'] = review_stats
    # ========

    # Lấy thông tin bổ sung từ tab "About"
    additional_info = get_additional_info(driver, wait)
    if additional_info:
        location_info['additional_info'] = additional_info
    # ========
    
    # Lấy thông tin danh sách review
    most_relevant_reviews = get_user_reviews(driver, wait, sort_type=0)
    if most_relevant_reviews:
        location_info['most_relevant_reviews'] = most_relevant_reviews

    newest_reviews = get_user_reviews(driver, wait, sort_type=1)
    if newest_reviews:
        location_info['newest_reviews'] = newest_reviews

    highest_rating_reviews = get_user_reviews(driver, wait, sort_type=2)
    if highest_rating_reviews:
        location_info['highest_rating_reviews'] = highest_rating_reviews

    lowest_rating_reviews = get_user_reviews(driver, wait, sort_type=3)
    if lowest_rating_reviews:
        location_info['lowest_rating_reviews'] = lowest_rating_reviews
    # ========

    return location_info
    
def scrape_reviews_from_ggmaps(places_data):
    reviews_data = []
    try:
        driver = create_driver()
        wait = WebDriverWait(driver, 15)
        
        for i, place in enumerate(places_data):
            progress = (i + 1) / len(places_data)
            print(f"({i + 1}/{len(places_data)}) [{progress:.2%}]: Scraping reviews for {place[3]}")
            url = place[4]
            review_data = get_reviews_data(url, driver, wait)
            reviews_data.append(review_data)
    except Exception as e:
        pass
    finally:
        driver.quit()
    return reviews_data

def run(chunk):
    locations = []
    
    with(open('backups/code/data/locations.csv', 'r', encoding='utf-8')) as f:
        reader = csv.reader(f)
        for row in reader:
            locations.append(row)
            
    # Remove the header
    locations = locations[1:]
    count = len(locations)
    chunk_size = count // 4
    
    start = 0
    end = count
    
    if chunk == None:
        print("No chunk provided, run all locations")
    elif chunk == 0:
        start = 0
        end = chunk_size
    elif chunk == 1:
        start = chunk_size
        end = 2 * chunk_size
    elif chunk == 2:
        start = 2 * chunk_size
        end = 3 * chunk_size
    else:
        start = 3 * chunk_size
        end = count

    reviews_data = []
    
    save_part = 'all' if chunk == None else chunk
    save_file_name = f'backups/code/data/reviews_data_{save_part}.json'
    
    try:
        with open(save_file_name, 'r') as file:
            data = json.load(file)
            last_index = data[-1]['index']
            review_data = data
    except Exception as e:
        pass
        last_index = -1
        
    print(f"Start={start}, End={end}, Last Index={last_index}, save to {save_file_name}")

    for i in range(len(locations)):
        if (i < start) or (i >= end) or (i <= last_index):
            continue
        location = locations[i]
        progress = (i + 1 - start) / (end - start)
        print(f"Index={i}, Total={end - start}, progress={progress:.2%}: Scraping reviews for {location[3]}")
        url = location[4]
        driver = create_driver()
        wait = WebDriverWait(driver, 15)
        review_data = get_reviews_data(url, driver, wait)
        review_data['category'] = location[0]
        review_data['location'] = location[1]
        review_data['keyword'] = location[2]
        review_data['url'] = url
        review_data['index'] = i
        driver.quit()
        reviews_data.append(review_data)

        print(f"Backup to json file at index={i}")
        with open(save_file_name, 'w', encoding='utf-8') as f:
            json.dump(reviews_data, f, ensure_ascii=False, indent=4)
        
    print(f"Scraped {len(reviews_data)} locations, expected {len(locations)}, saved to {save_file_name}")

if __name__ == "__main__":
    chunk = args.chunk
    run(chunk)