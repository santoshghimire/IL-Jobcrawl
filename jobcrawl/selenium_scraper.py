import time
import logging
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.chrome.service import Service


LOGGER.setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

MAX_ALLOWED_LOAD_MORE_JOBS_TIMEOUTEXCEPTION_TRIES = 5
MAX_RELOAD_FROM_BEGINNING_ALLOWED = 10


class DrushimScraper(object):
    WAIT_TIME = 10

    def __init__(self, url, log):
        self.log = log
        self.url = url
        self.total_crash_count = 0
        self.crash_count = 0
        self.load_more_jobs_timeout_exception_count = 0
        self.reload_scrape_from_beginning_count = 0
        display = Display(visible=0, size=(800, 800))
        display.start()
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

    def scrape(self, offset=None):
        self.log.info("Scraping %s", self.url)
        # self.driver.implicitly_wait(self.WAIT_TIME)
        self.driver.get(self.url)
        if offset is None:
            yield self.driver.page_source
        page_count = 1
        while True:
            try:
                if not self.click_load_jobs_button(page_count):
                    break
            except (WebDriverException, TimeoutException):
                self.reload_scrape_from_beginning_count += 1
                if self.reload_scrape_from_beginning_count >= MAX_RELOAD_FROM_BEGINNING_ALLOWED:
                    self.log.error("Giving up after reloading from beginning for %s times. page_count=%s", MAX_RELOAD_FROM_BEGINNING_ALLOWED, page_count)
                    break
                self.log.info("Encountered webdriver crash, resuming scraping from page_count=%s", page_count)
                self.init_driver()
                time.sleep(1)
                for pg in self.scrape(offset=page_count):
                    yield pg
                break
            if offset is not None and page_count < offset:
                page_count += 1
                time.sleep(1)
                continue
            time.sleep(10)
            try:
                yield self.driver.page_source
            except (WebDriverException, TimeoutException):
                self.reload_scrape_from_beginning_count += 1
                if self.reload_scrape_from_beginning_count >= MAX_RELOAD_FROM_BEGINNING_ALLOWED:
                    self.log.error("Giving up after reloading from beginning for %s times. page_count=%s", MAX_RELOAD_FROM_BEGINNING_ALLOWED, page_count)
                    break
                self.log.info("Encountered webdriver crash, resuming scraping from page_count=%s", page_count)
                self.init_driver()
                time.sleep(1)
                for pg in self.scrape(offset=page_count):
                    yield pg
                break
            page_count += 1

    def click_load_jobs_button(self, page_count):
        self.log.info("Clicking load jobs button: Pages scraped = %s", page_count)
        if page_count == 1:
            try:
                close_btn_cls = 'v-icon notranslate font-weight-bold mdi mdi-close theme--dark'
                close_btn = WebDriverWait(self.driver, self.WAIT_TIME).until(expected_conditions.visibility_of_element_located((
                    By.XPATH, "//i[@class='{}']".format(close_btn_cls))))
                close_btn.click()
            except:
                self.log.exception("Failed to click close btn")
        try:
            load_more_jobs = WebDriverWait(self.driver, self.WAIT_TIME).until(
                expected_conditions.visibility_of_element_located((
                    By.XPATH,
                    "//button[@class='v-btn v-btn--contained theme--light v-size--default load_jobs_btn ']")))
            load_more_jobs.click()
            self.load_more_jobs_timeout_exception_count = 0
        except TimeoutException as e:
            self.crash_count += 1
            self.total_crash_count += 1
            self.load_more_jobs_timeout_exception_count += 1
            self.log.error("Attempt={}: Got TimeoutException while loading more jobs. Remaining attempts={}. crash_count={}, total_crash_count={}, page={}:"
                           "".format(self.load_more_jobs_timeout_exception_count,
                            MAX_ALLOWED_LOAD_MORE_JOBS_TIMEOUTEXCEPTION_TRIES - self.load_more_jobs_timeout_exception_count,
                            self.crash_count, self.total_crash_count, page_count))
            if self.load_more_jobs_timeout_exception_count >= MAX_ALLOWED_LOAD_MORE_JOBS_TIMEOUTEXCEPTION_TRIES:
                # Max attempts reached. Reload from beginning
                raise e
            # Re-try till allowed (5 times) to click load jobs button
            return self.click_load_jobs_button(page_count)
        except WebDriverException as e:
            self.crash_count += 1
            self.total_crash_count += 1
            if "session deleted because of page crash" in str(e):
                self.log.exception("CLicking load jobs button failed coz of page crash: crash_count={}, total_crash_count={},"
                                   " page={}".format(self.crash_count, self.total_crash_count, page_count))
                raise e
            self.log.exception("Giving up: clicking load jobs button failed due to unknown error: crash_count={}, total_crash_count={}, page={}"
                               "".format(self.crash_count, self.total_crash_count, page_count))
            return
        except:
            self.crash_count += 1
            self.total_crash_count += 1
            self.log.exception("Giving up: clicking load jobs button failed because of unknown error: crash_count={}, total_crash_count={}, page={}"
                               "".format(self.crash_count, self.total_crash_count, page_count))
            return

        self.crash_count = 0
        self.log.info("Load more jobs button clicked successfully (page={})".format(page_count))
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
