from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from driver_utils import create_driver
 
import time as time
import re
import csv
import json
import os
from datetime import datetime, timezone

import argparse

parser = argparse.ArgumentParser("Run crawlings by chunks")
parser.add_argument("chunk", help="An integer: [0, 1, 2, 3,....]", type=int)
parser.add_argument("num_chunk", help="An integer: [4, 8, 12]", type=int)
args = parser.parse_args()

def get_location_info(driver, wait):
    location_info = {}
    try:
        location_info['title'] = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf'))).text.strip()
        location_info['address'] = driver.find_element(By.CSS_SELECTOR, '.Io6YTe.kR99db.fdkmkc').text.strip()

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
        actions = ActionChains(driver)
        scroll_attempts = 0
        max_scroll_attempts = 1 

        while scroll_attempts < max_scroll_attempts:
            actions.send_keys(Keys.PAGE_DOWN).perform() 
            time.sleep(1) 
            scroll_attempts += 1  

        expand_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.puWIL.hKrmvd'))) 
        actions = ActionChains(driver)
        actions.move_to_element(expand_button).click().perform()

        time.sleep(2)

        open_hours_element = driver.find_element(By.CSS_SELECTOR, 'div.t39EBf.GUrTXd')
        
        aria_label_text = open_hours_element.get_attribute('aria-label')

        hour_info_list = aria_label_text.split(';')

        opening_hours = {}

        if "Hide open hours for the week" in hour_info_list[-1]:
            hour_info_list[-1] = hour_info_list[-1].split(". Hide open hours for the week")[0]

        for day_info in hour_info_list:
            day_info = day_info.strip()
            day_name, hours = day_info.split(", ", 1)
            
            if ", " in hours:
                hours_list = [h.strip() for h in hours.split(", ")]
            else:
                hours_list = [hours.strip()]
        
            opening_hours[day_name] = hours_list

        return opening_hours

    except Exception as e:
        return None

# Hàm lấy thông tin giá tiền
def get_price_info(driver, wait):
    try:
        expand_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.IgTTAf.LkEmWe')))
        actions = ActionChains(driver)
        actions.move_to_element(expand_button).click().perform()
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

    actions = ActionChains(driver)
    title_element  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf')))
    actions.move_to_element(title_element).perform()  
    for _ in range(1): 
        actions.send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(2) 

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
    
def run(chunk, num_chunks=8):
    locations = []
    
    with(open('backups/code/data/locations.csv', 'r', encoding='utf-8')) as f:
        reader = csv.reader(f)
        for row in reader:
            locations.append(row)
            
    # Remove the header
    locations = locations[1:]
    count = len(locations)
    chunk_size = count // num_chunks
    
    start = 0
    end = count
    
    if chunk == None:
        print("No chunk provided, run all locations")
    else:
        start = chunk * chunk_size
        end = (chunk + 1) * chunk_size
        if chunk == num_chunks - 1:
            end = count
    
    save_part = 'all' if chunk == None else chunk
    save_file_name = f'backups/code/data/reviews_data_{save_part}.json'
    last_index_filePath = f'backups/code/data/last_index_{save_part}.json'

    last_index = get_last_index(last_index_filePath)
        
    print(f"Start={start}, End={end}, Last Index={last_index}, save to {save_file_name}")

    for i in range(len(locations)):
        if (i < start) or (i >= end) or (i <= last_index):
            continue
        location = locations[i]
        progress = (i + 1 - start) / (end - start)
        utc_dt = datetime.now(timezone.utc)
        dt = utc_dt.astimezone()
        print(f"Chunk={chunk}, Index={i}, Total={end - start}, progress={progress:.2%}, time={dt}: Scraping reviews for {location[3]}")
        url = location[4]

        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('headless')  # Chế độ headless (chạy không cần giao diện)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        wait = WebDriverWait(driver, 15)

        review_data = get_reviews_data(url, driver, wait)
        review_data['category'] = location[0]
        review_data['location'] = location[1]
        review_data['keyword'] = location[2]
        review_data['url'] = url
        review_data['index'] = i
        driver.quit()

        print(f"Backup to json file at index={i}")
        append_to_json_line_by_line(review_data, save_file_name)

        save_last_index(i, last_index_filePath)

def append_to_json_line_by_line(new_data, file_path):
    with open(file_path, 'a', encoding='utf-8') as file: 
        file.write(json.dumps(new_data, ensure_ascii=False) + '\n')

def get_last_index(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data.get('last_index', -1)
    except FileNotFoundError:
        return -1  

def save_last_index(index, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump({"last_index": index}, file)

if __name__ == "__main__":
    chunk = args.chunk
    num_chunk = args.num_chunk
    print(f"Run chunk={chunk}, num_chunk={num_chunk}")
    run(chunk, num_chunk)