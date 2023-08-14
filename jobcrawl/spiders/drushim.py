# -*- coding: utf-8 -*-
import scrapy
# from scrapy.shell import inspect_response
from scrapy import signals
from jobcrawl.items import JobItem
from scrapy.http import HtmlResponse
from jobcrawl.selenium_scraper import DrushimScraper
from pydispatch import dispatcher
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

    def __init__(self):
        # sys.stdout = codecs.getwriter(
        #     locale.getpreferredencoding())(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        self.selenium_scraper = DrushimScraper(self.scrape_url, self.logger)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.total_jobs = 0

    def parse(self, response):
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
                    ".//span[@class='job-url primary--text font-weight-bold primary--text']").xpath(
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
                job_description = job_container.xpath(".//div[@class='layout job-intro vacancyMain mt-2 px-md-4 px-2 py-1 wrap pointer']").xpath(
                    "normalize-space(string())").extract_first()
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

