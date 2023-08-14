# -*- coding: utf-8 -*-
import os
import scrapy
# from scrapy.shell import inspect_response
from jobcrawl.items import JobItem
import re
import datetime
# import sys
# import locale
# import codecs
import urllib.parse as urlparse
from scrapy.http import HtmlResponse
from jobcrawl.js_scraper import JSScraperRunner
from jobcrawl.endtime_check import reached_endtime


class JobmasterSpider(scrapy.Spider):
    name = "jobmaster"
    allowed_domains = ["jobmaster.co.il"]
    start_urls = (
        'https://www.jobmaster.co.il/',
    )

    def __init__(self):

        # sys.stdout = codecs.getwriter(locale.getpreferredencoding()
        #                               )(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        self.total_locations = 0
        self.total_jobs = 0
        self.location_total_jobs = {}
        self.html_dir_name = 'jobmaster_htmls'
        if not os.path.exists(self.html_dir_name):
            os.makedirs(self.html_dir_name)
        self.runner = JSScraperRunner(self.logger)

    def parse(self, response):
        """
        Get all the links for Location
        """
        job_location_links_list = response.xpath("//a[contains(@href,'/jobs/searchfilterHome.asp?type=ezor&l=')]/@href").extract()

        for c, location_li in enumerate(job_location_links_list):
            if reached_endtime():
                self.logger.info("JobMaster: End run because endtime is reached")
                break
            yield scrapy.Request(
                response.urljoin(location_li),
                callback=self.parse_each_sub_location,
                dont_filter=True,
                meta={"location_id": c}
            )


    def parse_each_sub_location(self, response):
        if response.status != 200:
            self.logger.error("{}\n ERROR Code {}: {} \n {}".format(
                "*" * 30, response.status, response.url, "*" * 30))

        job_location_links_list = response.xpath("//a[contains(@href, '/jobs/?l=')]/@href").extract()
        for c, location_li in enumerate(job_location_links_list):
            if reached_endtime():
                self.logger.info("JobMaster: End run because endtime is reached")
                break
            self.total_locations += 1
            location_id = "{}_{}".format(response.meta['location_id'], c)
            self.location_total_jobs[location_id] = 0
            yield scrapy.Request(
                response.urljoin(location_li),
                callback=self.parse_each_location,
                dont_filter=True,
                meta={"location_id": location_id}
            )

    def parse_each_location(self, response):
        if response.status != 200:
            self.logger.error("{}\n ERROR Code {}: {} \n {}".format(
                "*" * 30, response.status, response.url, "*" * 30))

        url = response.url
        parsed = urlparse.urlparse(url)
        try:
            page = urlparse.parse_qs(parsed.query)['currPage'][0]
        except:
            page = 1
        location_id = response.meta['location_id']
        fname = "location_{}_page_{}.html".format(location_id, page)
        output_file = os.path.join(self.html_dir_name, fname)

        for attempt in range(5):
            # Run JS Crawler
            if reached_endtime():
                self.logger.info("JobMaster: End run because endtime is reached")
                break
            self.runner.run(url, output_file)

            if not os.path.isfile(output_file):
                self.logger.error("Jobmaster: Output file not present. url=%s, attempt=%s, remaining=%s", url, attempt, 4 - attempt)
                continue

            fobj = open(output_file)
            body = fobj.read()
            response = HtmlResponse(url=url, body=body, encoding='utf-8')
            fobj.close()
            try:
                os.remove(output_file)
            except OSError:
                pass


            job_article_div_list = response.xpath(
                "//article[@class='CardStyle JobItem font14 ']")
            # job_article_div_list = response.xpath("//article[contains(@class,'CardStyle JobItem font14')]")
            page_job_count = 0
            for job_article in job_article_div_list:
                job_article_id = job_article.xpath(".//@id").extract_first()
                job_id_group = re.findall(r'[\d]+', job_article_id)
                if job_id_group:
                    job_id = job_id_group[0]
                    job_link = "https://www.jobmaster.co.il/jobs/checknum.asp?key={}".format(job_id)
                else:
                    job_id = ""
                    job_link = ""

                job_item_sel = job_article.xpath(".//div[@class='JobItemRight Transition']")
                try:
                    job_title = job_item_sel.xpath(
                        ".//a[@class='CardHeader']/text()").extract_first()
                except:
                    job_title = ''

                try:
                    company = job_item_sel.xpath(".//a[@class='font14 CompanyNameLink']").xpath("normalize-space(string())").extract_first()
                    if not company:
                        company = job_item_sel.xpath(".//span[@class='font14 ByTitle']/text()").extract_first()
                    if company:
                        company = company.strip()
                except:
                    company = ""

                try:
                    company_jobs = job_item_sel.xpath(".//a[@class='font14 CompanyNameLink']/@href").extract_first()
                    if company_jobs:
                        company_jobs = response.urljoin(company_jobs)
                except:
                    company_jobs = ""

                try:
                    country_areas = job_item_sel.xpath(".//li[@class='jobLocation']/text()").extract_first()
                except:
                    country_areas = ""

                try:
                    category = job_item_sel.xpath(".//li[@class='jobType']").xpath("normalize-space(string())").extract_first()
                    if category:
                        category = category.strip()
                except:
                    category = ""

                # try:
                #     category = job_item_sel.xpath(".//span[@class='Gray']").xpath(
                #         "normalize-space(string())").extract_first().strip()
                #     category = category.replace("|", ",")
                # except:
                #     category = ''

                try:
                    all_child_elem_job_item = job_item_sel.xpath("./*")
                    child_index = 3
                    job_description = ""
                    while child_index < len(all_child_elem_job_item):
                        job_description += all_child_elem_job_item[
                            child_index].xpath("normalize-space(string())"
                                               ).extract_first()
                        job_description += "\n"
                        child_index += 1

                except:

                    job_description = ""

                try:
                    job_post_date = job_item_sel.xpath("./div[@class='paddingTop10px']/span[@class='Gray']/text()").extract_first()
                    job_post_date = self.find_date(job_post_date)

                except:
                    today_date = datetime.date.today()
                    today_date_str = today_date.strftime("%d/%m/%Y")

                item = JobItem()

                item['Job'] = {
                    'Site': 'JobMaster',
                    'Company': company,
                    'Company_jobs': company_jobs,
                    'Job_id': job_id,
                    'Job_title': job_title,
                    'Job_Description': job_description,
                    'Job_Post_Date': job_post_date,
                    'Job_URL': job_link,
                    'Country_Areas': country_areas,
                    'Job_categories': category,
                    'AllJobs_Job_class': "",
                    'unique_id': 'jobmaster_{}'.format(job_id)
                }
                self.total_jobs += 1
                page_job_count += 1
                self.location_total_jobs[location_id] += 1
                yield item

            self.logger.info("Jobmaster: Location %s, Page %s job count = %s, location_total_jobs=%s, total_jobs=%s",
                location_id, page, page_job_count, self.location_total_jobs[location_id], self.total_jobs)

            pagi_link_sel_list = response.xpath("//a[@class='paging']")
            for pagi_link_sel in pagi_link_sel_list:
                nextpagi_text = pagi_link_sel.xpath(
                    "text()").extract_first()
                if nextpagi_text == u'\u05d4\u05d1\u05d0\xbb' or nextpagi_text == u'\u05d4\u05d1\u05d0 \xbb':
                    next_url = response.urljoin(pagi_link_sel.xpath("@href").extract_first())
                    yield scrapy.Request(next_url, self.parse_each_location, dont_filter=True,
                                         meta={'location_id': location_id})
            break

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
                job_post_date = today_date_str
        return job_post_date

