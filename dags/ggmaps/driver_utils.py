from selenium import webdriver

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    options.add_argument("--window-size=2560,5000")

    driver = webdriver.Remote(
        command_executor='http://docker.for.mac.localhost:4444',
        options=options
    )
    
    return driver
        