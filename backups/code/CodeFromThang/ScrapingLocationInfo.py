import json
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time

# Hàm lấy thông tin về địa điểm (location_info)
def get_location_info(driver, wait):
    location_info = {}
    try:
        location_info['title'] = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf'))).text.strip()       
        location_info['address'] = driver.find_element(By.CSS_SELECTOR, '.Io6YTe.kR99db.fdkmkc').text.strip()

    except Exception as e:
        print("Error fetching location info:", e)
    
    return location_info

# Hàm lấy thông tin về kinh độ và vĩ độ
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
        print("Error fetching opening hours:", e)
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
                print(f"Error fetching individual review: {e}")
    
    except Exception as e:
        print("Error while fetching reviews:", e)
    
    return user_reviews

def get_reviews_data(url, output):
    options = webdriver.ChromeOptions()
    # Thiết lập các tùy chọn cho Chrome để bỏ qua lỗi SSL
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    # options.add_argument('headless')  # Chế độ headless (chạy không cần giao diện)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
      
    driver.get(url)

    wait = WebDriverWait(driver, 15)

    actions = ActionChains(driver)
    title_element  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.DUwDvf')))
    actions.move_to_element(title_element).perform()  
    for _ in range(1): 
        actions.send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(2) 

    # Tìm title và địa chỉ
    location_info = get_location_info(driver, wait)
    # ========

    # Tìm vĩ độ và kinh độ trong URL (thường nằm sau "@")
    time.sleep(3)
    current_url = driver.current_url
    lat_long = extract_lat_long(current_url)
    if lat_long:
        location_info['lat_long'] = lat_long
    # ========

    # Lấy thông tin giờ mở cửa
    opening_hours = get_opening_hours(driver, wait)
    if opening_hours:
        location_info['opening_hours'] = opening_hours
    # ========
    
    # Lấy thông tin giá tiền
    price_info = get_price_info(driver, wait)
    if price_info:
        location_info['price_info'] = price_info
    # ========

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

    driver.quit()

    with open(output, 'w', encoding='utf-8') as f:
        json.dump(location_info, f, ensure_ascii=False, indent=4)

    print(f'Data saved to {output}')


url_pho="https://www.google.com/maps/place/Ph%E1%BB%9F+gia+truy%E1%BB%81n+Nam+%C4%90%E1%BB%8Bnh/data=!4m7!3m6!1s0x31752f817b0d26c3:0xf5181c6288159f69!8m2!3d10.7636698!4d106.6890646!16s%2Fg%2F11j2xn07s8!19sChIJwyYNe4EvdTERaZ8ViGIcGPU?authuser=0&hl=en&rclk=1"
get_reviews_data(url_pho, 'result_pho.json')

url_pho_test1="https://www.google.com/maps/place/PH%E1%BB%9E+TH%C3%8CN+QU%E1%BA%ACN+1+%28Ph%E1%BB%9F+Th%C3%ACn+13+L%C3%B2+%C4%90%C3%BAc%29%7C+Ph%E1%BB%9F+ngon+S%C3%A0i+G%C3%B2n/data=!4m7!3m6!1s0x31752f77874f6e07:0xc330879c8a05a03f!8m2!3d10.7681788!4d106.6970277!16s%2Fg%2F11tx_tx1z7!19sChIJB25Ph3cvdTERP6AFipyHMMM?authuser=0&hl=en&rclk=1"
# get_reviews_data(url_pho_test1, 'result_pho_test1.json')

url_doanPhap = "https://www.google.com/maps/place/L'+Etoile+Restaurant/@10.7957271,106.6586511,6103m/data=!3m1!1e3!4m10!1m2!2m1!1zUVXDoW4gUGjDoXA!3m6!1s0x31752f347a27ed2b:0x81769ad594aacae0!8m2!3d10.786391!4d106.694481!15sCgtRVcOhbiBQaMOhcFoNIgtxdcOhbiBwaMOhcJIBEWZyZW5jaF9yZXN0YXVyYW504AEA!16s%2Fg%2F1tjgm2h3?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI0MTAyOS4wIKXMDSoASAFQAw%3D%3D"
# get_reviews_data(url_doanPhap, 'result_doanPhap.json')