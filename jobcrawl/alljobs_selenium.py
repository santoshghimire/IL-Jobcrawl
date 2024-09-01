import time
import os
import random
import logging
from selenium import webdriver
import urllib.parse as urlparse
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.chrome.service import Service


LOGGER.setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

MAX_ALLOWED_LOAD_MORE_JOBS_TIMEOUTEXCEPTION_TRIES = 5
MAX_RELOAD_FROM_BEGINNING_ALLOWED = 10


class AlljobsScraper(object):
    WAIT_TIME = 10

    def __init__(self, log, url=None):
        self.log = log
        self.url = url
        display = Display(visible=0, size=(800, 800))
        display.start()
        self.screenshot_dir = 'alljobs_screenshots'
        self.html_dir = 'alljobs_htmls_test'
        if not os.path.exists(self.screenshot_dir):
            os.mkdir(self.screenshot_dir)
        if not os.path.exists(self.html_dir):
            os.mkdir(self.html_dir)            
        self.init_driver()

    def init_driver(self):
        self.close_driver()
        chrome_driver = '/usr/local/bin/chromedriver'
        service = Service(executable_path=chrome_driver)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument("enable-automation")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-infobars")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-browser-side-navigation")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--enable-javascript")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.log.info("WebDriver initialized")

        # self.driver.get(self.url)
        # time.sleep(60)
        # print("Done sleeping")
        # self.close_dialogue_box()
        # time.sleep(10)
        # # self.save('last')
        # try:
        #     job_container_div_list_open = self.driver.find_elements(By.CSS_SELECTOR, 'div.open-board')
        #     print(bool(job_container_div_list_open), len(job_container_div_list_open))
        # except:
        #     print("Failed to find job boards open")
        # try:
        #     job_container_div_list_organic = self.driver.find_elements(BY.CSS_SELECTOR, 'div.organic-board')
        #     print(bool(job_container_div_list_organic), len(job_container_div_list_organic))
        # except:
        #     print("Failed to find job boards organic")

        # print("Hello")
        # body = self.driver.page_source
        # time.sleep(1000)
        # self.driver.close()


    def parse(self, url):
        try:
            self.driver.get(url)
        except:
            self.init_driver(url)
            self.driver.get(url)

        time.sleep(random.randint(3, 6))
        parsed = urlparse.urlparse(url)
        page = int(urlparse.parse_qs(parsed.query)['page'][0])
        initial = True if str(page) == '1' else False
        if initial or not self.found_job_boards():
            self.close_dialogue_box(initial=initial)
        body = self.save(page)
        return body

    def found_job_boards(self):
        try:
            job_container_div_list_open = self.driver.find_elements(By.CSS_SELECTOR, 'div.open-board')
            job_container_div_list_open = job_container_div_list_open or []
        except:
            print("Failed to find job boards open")
        try:
            job_container_div_list_organic = self.driver.find_elements(BY.CSS_SELECTOR, 'div.organic-board')
            job_container_div_list_organic = job_container_div_list_organic or []
        except:
            print("Failed to find job boards organic")
        return bool(job_container_div_list_open + job_container_div_list_organic)

    def close_dialogue_box(self, initial=False):
        try:
            self.driver.find_element(By.ID, 'cboxClose').click()
            print("Closed dialog box div")
        except:
            print("Failed to click div close btn")

        try:
            self.driver.find_element(By.CLASS_NAME, 'close-button').click()
            print("Closed dialog box img")
        except:
            print("Failed to click img close btn")
        time.sleep(1)

    def save(self, i):
        if i < 5:
            self.take_screenshot(i)
        html_file = os.path.join(self.html_dir, 'alljobs_{}.html'.format(i))
        try:
            html_body = self.driver.page_source
            with open(html_file, 'w') as fp:
                fp.write(html_body)
            return html_body
        except Exception:
            self.log.exception("Failed to save html page %s", i)

    def take_screenshot(self, i):
        screenshot_file = os.path.join(self.screenshot_dir, 'alljobs_{}.png'.format(i))
        try:
            self.driver.save_screenshot(screenshot_file)
        except Exception:
            self.log.exception("Failed to save screenshot %s", screenshot_file)

    def close_driver(self):
        try:
            self.driver.close()
        except:
            pass


if __name__ == '__main__':
    import logging
    log = logging.getLogger()
    print("Hello")
    url = 'https://www.alljobs.co.il/SearchResultsGuest.aspx?page=1&position=&type=&freetxt=&city=&region='
    self = AlljobsScraper(log)
    body = self.scrape(url)
    print(body)
