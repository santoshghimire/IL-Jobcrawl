# -*- coding: utf-8 -*-
import requests
import scrapy
import time
import random
# from scrapy.shell import inspect_response
from dateutil.parser import parse
from scrapy import signals
from jobcrawl.items import JobItem
from scrapy.http import HtmlResponse
# from jobcrawl.selenium_scraper import DrushimScraper
# from pydispatch import dispatcher
# from scrapy.xlib.pydispatch import dispatcher
from jobcrawl.endtime_check import reached_endtime

# import sys
# import locale
# import codecs
import re
import datetime


class DrushimSpider(scrapy.Spider):
    name = "drushim"
    allowed_domains = ["drushim.co.il"]
    base_url = "https://www.drushim.co.il"
    scrape_url = 'https://www.drushim.co.il/jobs/search/%22%22/?ssaen=1'
    start_urls = (scrape_url, )
    seen_job_ids = set()
    api_url = 'https://www.drushim.co.il/api/jobs/search?searchTerm=%22%22&ssaen=1&isAA=true&page={}&isAA=true'
    max_page = 5000

    def __init__(self):
        # sys.stdout = codecs.getwriter(
        #     locale.getpreferredencoding())(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        # self.selenium_scraper = DrushimScraper(self.scrape_url, self.logger)
        # dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.total_jobs = 0

    # Start New API way of scraping
    def parse(self, response):
        page = 0
        # for page_source in self.selenium_scraper.scrape():
        #     if reached_endtime():
        #         self.logger.info("Drushim: End run because endtime is reached")
        #         break
        #     response = HtmlResponse(url=self.scrape_url, body=page_source, encoding='utf-8')
        #     page_job_count = 0
        #     for item in self.parse_html(response):
        #         self.total_jobs += 1
        #         page_job_count += 1
        #         yield item
        #     self.logger.info("Drushim: Page %s job count = %s, total_jobs=%s", page, page_job_count, self.total_jobs)
        #     break  # IMPORTANT - We only need first page - 25 jobs.

        # Start calling api
        end_confirmation_count = 1
        end_confirmation_num = 2
        no_results_confirmation_count = 1
        no_results_confirmation_num = 2
        while True:
            if reached_endtime():
                self.logger.info("Drushim: End run because endtime is reached")
                break
            api_res = self.get_api_results(page=page)
            page_job_count = 0
            for item in self.parse_api_results(api_res):
                self.total_jobs += 1
                page_job_count += 1
                yield item

            self.logger.info("Drushim: API RES Page %s job count = %s, total_jobs=%s", page, page_job_count, self.total_jobs)
            next_page = api_res.get('NextPageNumber', page + 1)  # Next Page
            api_total_pages = api_res.get('TotalPagesNumber', 0)
            if next_page == -1:
                if end_confirmation_count > end_confirmation_num:
                    if page < api_total_pages:
                        # Regardless of confirmation, proceed
                        page += 1
                        end_confirmation_count = 1  # Reset
                    else:
                        # Reached endpage
                        self.logger.info("Drushim: Reached END page %s after confirmation %s times. Exit..."
                            "API Total pages = %s, API RES Page %s job count = %s, total_jobs=%s",
                            page, end_confirmation_num + 1, api_total_pages, page, page_job_count, self.total_jobs)
                        break
                end_confirmation_count += 1  # Retry same page for 3 times
            elif page_job_count == 0:
                end_confirmation_count = 1
                if no_results_confirmation_count > no_results_confirmation_num:
                    max_page = min(self.max_page, api_total_pages)
                    if page >= max_page:
                        self.logger.info("Drushim: Reached max page %s with 0 page job count. Exit..."
                            "API RES Page %s job count = %s, total_jobs=%s",
                            max_page, page, page_job_count, self.total_jobs)
                        break
                    # Actually no results on this page, proceed ahead
                    self.logger.info("Drushim: Found page %s with 0 page job count after confirmation."
                            "API RES Page %s job count = %s, total_jobs=%s",
                            page, page, page_job_count, self.total_jobs)
                    page = next_page
                else:
                    no_results_confirmation_count += 1  # Retry same page for 3 times
            else:
                end_confirmation_count = 1
                no_results_confirmation_count = 1
                page = next_page
            time.sleep(random.randint(3, 6))

        self.logger.info("Drushim: Scraping finished. Total Pages = %s, Total_jobs=%s", page, self.total_jobs)

    def get_api_results(self, page=1):
        url = self.api_url.format(page)
        error_sleep = 10
        res_json = {}
        for i in range(5):
            i += 1
            try:
                response = requests.get(url)
            except Exception as exp:
                self.logger.exception("Drushim: [Attempt {}]: Failed to fetch api results for page {}".format(i, page))
                continue
            if response.status_code != 200:
                delay = error_sleep * i
                self.logger.error("Drushim: [Attempt {}]: Got not ok status code {} for api results for page {}"
                    ". Sleeping for {} s".format(i, response.status_code, page, delay))
                time.sleep(delay)
            try:
                res_json = response.json()
                time.sleep(0.5)
            except Exception as exp:
                self.logger.error("Drushim: [Attempt {}]: Failed to parse json result. status code={}, page={}"
                    "".format(i, response.status_code, page))
                continue
            if 'ResultList' not in res_json:
                self.logger.error("Drushim: [Attempt {}]: Invalid json result obtained. status code={}, page={}, json result={}"
                    "".format(i, response.status_code, page, res_json))
                continue
            return res_json
        return res_json

    def parse_api_results(self, api_res):
        for job in api_res.get('ResultList') or []:
            job_link = job.get('JobInfo', {}).get('Link')
            job_url = "{}{}".format(self.base_url, job_link)
            job_id = '-'.join(job_link.strip('/').split('/')[-2:])

            if not job_id or job_id in self.seen_job_ids:
                continue

            self.seen_job_ids.add(job_id)
            job_description = job.get('JobContent', {}).get('Description', '')
            job_requirement = job.get('JobContent', {}).get('Requirement', '')

            try:
                job_post_date = parse(job.get('JobInfo', {}).get('Date', ''))
                job_post_date = job_post_date.strftime("%d/%m/%Y")
            except:
                self.logger.error("Failed to parse job post date")
                job_post_date = datetime.date.today().strftime("%d/%m/%Y")

            categories = []
            for category in job.get('JobContent', {}).get('Categories', []):
                if category.get('NameInHebrew', ''):
                    categories.append(category['NameInHebrew'])

            country_areas = [a.get('City', '') for a in job.get('JobContent', {}).get('Addresses', [])]

            item = JobItem()
            item['Job'] = {
                'Site': 'Drushim',
                'Company': job.get('Company', {}).get('CompanyDisplayName'),
                'Company_jobs': "{}/{}".format(self.base_url, job.get('Company', {}).get('ToUrl')),
                'Job_id': job_id,
                'Job_title': job.get('JobContent', {}).get('Name'),
                'Job_Description': "\n".join([job_description, job_requirement]),
                'Job_Post_Date': job_post_date,
                'Job_URL': job_url,
                'Country_Areas': " ".join(country_areas),
                'Job_categories': " ".join(categories),
                'AllJobs_Job_class': '',
                'unique_id': 'drushim_{}'.format(job_id)
            }
            yield item

    # End New API way of scraping

    def parse_old(self, response):
        page = 1
        for page_source in self.selenium_scraper.scrape():
            if reached_endtime():
                self.logger.info("Drushim: End run because endtime is reached")
                break
            response = HtmlResponse(url=self.scrape_url, body=page_source, encoding='utf-8')
            page_job_count = 0
            for item in self.parse_html(response):
                self.total_jobs += 1
                page_job_count += 1
                yield item
            self.logger.info("Drushim: Page %s job count = %s, total_jobs=%s", page, page_job_count, self.total_jobs)
            page += 1

    def parse_html(self, response):
        job_container_list = response.xpath(
            "//div[@class='job-item-main pb-3 job-hdr']")
        for job_container in job_container_list:
            job_link = job_container.xpath(
                        './/div[contains(@class, "nowrap align-self-center pc-view open-job text-center")]'
                        '/a/@href').extract_first()
            if job_link:
                job_link = "{}{}".format(self.base_url, job_link)
                if job_link.endswith('/'):
                    job_link = job_link[:-1]

            try:
                job_id = "-".join(job_link.split("/")[-2:])
            except:
                job_id = ""

            if not job_id or job_id in self.seen_job_ids:
                continue

            self.seen_job_ids.add(job_id)

            try:
                job_title = job_container.xpath(
                    './/span[contains(@class, "job-url primary--text")]').xpath(
                        "normalize-space(string())").extract_first()
            except:
                job_title = ""

            try:
                company = job_container.xpath(
                    ".//div[@class='layout job-details-top mt-md-2']"
                    "/div[@class='flex grow-none ml-3']/p").xpath("normalize-space(string())").extract_first()
            except:
                company = ""

            try:
                company_jobs = job_container.xpath(
                    ".//div[@class='layout job-details-top mt-md-2']"
                    "/div[@class='flex grow-none ml-3']/p/a/@href").extract_first()
            except:
                company_jobs = job_link

            try:
                job_description = job_container.xpath(
                        './/div[contains(@class, "layout job-intro vacancyMain")]').xpath("normalize-space(string())").extract_first()
                        
            except:
                job_description = ""

            job_sub_details = job_container.xpath(".//div[@class='layout job-details-sub']").xpath(
                "normalize-space(string())").extract_first()
            jsd_val = []
            if job_sub_details:
                jsd_val = job_sub_details.split("|")

            country_areas = jsd_val[0].strip() if jsd_val else ""
            category = jsd_val[2].strip() if jsd_val and len(jsd_val) == 4 else ""
            job_post_date = jsd_val[-1].strip() if jsd_val and len(jsd_val) == 4 else ""
            job_post_date = self.find_date(job_post_date)

            item = JobItem()
            item['Job'] = {
                'Site': 'Drushim',
                'Company': company,
                'Company_jobs': company_jobs,
                'Job_id': job_id,
                'Job_title': job_title,
                'Job_Description': job_description,
                'Job_Post_Date': job_post_date,
                'Job_URL': job_link,
                'Country_Areas': country_areas,
                'Job_categories': category,
                'AllJobs_Job_class': '',
                'unique_id': 'drushim_{}'.format(job_id)
            }

            yield item

    def spider_closed(self, spider):
        self.selenium_scraper.close_driver()

    @staticmethod
    def find_date(job_post_date):
        today_date = datetime.date.today()
        today_date_str = today_date.strftime("%d/%m/%Y")
        try:
            job_post_date_num = re.findall(r'[\d]+', job_post_date)[0]
            job_post_date_num = int(job_post_date_num)
            if job_post_date_num:
                second = 'שְׁנִיָה'
                seconds = 'שניות'
                minute = 'דַקָה'
                minutes = 'דקות'
                hour = 'שָׁעָה'
                hours = 'שעות'
                day = 'יְוֹם'
                days = 'ימים'
                # month = 'חוֹדֶשׁ'
                # months = 'חודשים'
                hms = [second, seconds, minute, minutes, hour, hours]
                if day in job_post_date:
                    job_post_date = datetime.date.today() - datetime.timedelta(days=job_post_date_num)
                    job_post_date = job_post_date.strftime("%d/%m/%Y")
                elif days in job_post_date:
                    job_post_date = datetime.date.today() - datetime.timedelta(days=job_post_date_num)
                    job_post_date = job_post_date.strftime("%d/%m/%Y")
                elif [x for x in hms if x in job_post_date]:
                    job_post_date = today_date_str
                elif job_post_date_num == 0:
                    job_post_date = today_date_str
                else:
                    job_post_date = job_post_date
        except:
            if job_post_date == 'לפני דקה':
                job_post_date = today_date_str
            else:
                job_post_date = ""
        return job_post_date

