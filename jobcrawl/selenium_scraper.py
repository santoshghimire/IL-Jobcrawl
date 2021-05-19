import time
import logging
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.remote_connection import LOGGER

LOGGER.setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class DrushimScraper(object):
    WAIT_TIME = 10

    def __init__(self, url, log):
        self.log = log
        self.url = url
        chrome_options = Options()
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument("enable-automation")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-browser-side-navigation")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--enable-javascript")
        chrome_driver = '/usr/local/bin/chromedriver'
        display = Display(visible=0, size=(800, 800))
        display.start()
        self.driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)

    def scrape(self):
        self.log.info("Scraping %s", self.url)
        # self.driver.implicitly_wait(self.WAIT_TIME)
        self.driver.get(self.url)
        yield self.driver.page_source
        page_count = 1
        while True:
            if not self.click_load_jobs_button(page_count):
                break
            time.sleep(10)
            yield self.driver.page_source
            page_count += 1

    def click_load_jobs_button(self, page_count):
        self.log.info("Clicking load jobs button: Pages scraped = %s", page_count)
        try:
            load_more_jobs = WebDriverWait(self.driver, self.WAIT_TIME).until(
                expected_conditions.visibility_of_element_located((
                    By.XPATH,
                    "//button[@class='v-btn v-btn--contained theme--light v-size--default load_jobs_btn ']")))
            load_more_jobs.click()
        except:
            self.log.exception("CLicking load jobs button failed")
            return
        self.log.info("Load more jobs button clicked successfully")
        return True

    def close_driver(self):
        try:
            self.driver.close()
        except:
            pass


if __name__ == '__main__':
    import logging
    self = DrushimScraper('https://www.drushim.co.il/jobs/search/%22%22/?ssaen=1', logging.getLogger())
    count = 0
    for page_source in self.scrape():
        count += 1
        if count == 5:
            break
    print("All Done")
    self.close_driver()
