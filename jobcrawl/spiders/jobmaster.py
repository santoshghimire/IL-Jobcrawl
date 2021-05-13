# -*- coding: utf-8 -*-
import os
import scrapy
# from scrapy.shell import inspect_response
from jobcrawl.items import JobItem
import re
import datetime
import sys
import locale
import codecs
import urlparse
from scrapy.http import HtmlResponse
from jobcrawl.js_scraper import JSScraperRunner


class JobmasterSpider(scrapy.Spider):
    name = "jobmaster"
    allowed_domains = ["jobmaster.co.il"]
    start_urls = (
        'https://www.jobmaster.co.il/',
    )

    def __init__(self):

        sys.stdout = codecs.getwriter(locale.getpreferredencoding()
                                      )(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.total_locations = 0
        self.total_locations_job = 0
        self.all_jobs_count = 0
        self.each_location_total_jobs = 0
        self.html_dir_name = 'jobmaster_htmls'
        if not os.path.exists(self.html_dir_name):
            os.makedirs(self.html_dir_name)
        self.runner = JSScraperRunner(self.logger)

    def parse(self, response):
        """
        Get all the links for Location
        """
        job_location_links_list = response.xpath("//a[contains(@href,'/jobs/searchfilter.asp?type=')]/@href").extract()

        for c, location_li in enumerate(job_location_links_list):
            self.total_locations += 1
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
            self.total_locations += 1
            yield scrapy.Request(
                response.urljoin(location_li),
                callback=self.parse_each_location,
                dont_filter=True,
                meta={"location_id": "{}_{}".format(response.meta['location_id'], c)}
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

        # Run JS Crawler
        self.runner.run(url, output_file)

        if os.path.isfile(output_file):
            body = open(output_file).read()
            response = HtmlResponse(url=url, body=body, encoding='utf-8')

        job_article_div_list = response.xpath(
            "//article[@class='CardStyle JobItem font14']")
        for job_article in job_article_div_list:
            job_article_id = job_article.xpath(".//@id").extract_first()
            job_id_group = re.findall(r'[\d]+', job_article_id)
            if job_id_group:
                job_id = job_id_group[0]
                job_link = "https://www.jobmaster.co.il/code/check/" \
                           "checknum.asp?flagShare={}".format(job_id)
            else:
                job_id = ""
                job_link = ""

            job_item_sel = job_article.xpath(".//div[@class='JobItemRight Transition']")
            try:
                job_title = job_item_sel.xpath(
                    ".//div[@class='CardHeader']/text()").extract_first()
            except:
                job_title = ''

            try:
                company = job_item_sel.xpath(".//div")[1].xpath(
                    ".//a[@class='font14 CompanyNameLink']/text()").extract_first()
                if not company:
                    company = job_item_sel.xpath(
                        ".//div")[1].xpath(".//span[@class='font14 ByTitle']/text()").extract_first()
                if company:
                    company = company.strip()
            except:
                company = ""

            try:
                company_jobs = job_item_sel.xpath(".//div")[1].xpath(
                    ".//a[@class='font14 CompanyNameLink']/@href").extract_first()
                if company_jobs:
                    company_jobs = response.urljoin(company_jobs)
            except:
                company_jobs = ""

            try:
                country_areas = job_item_sel.xpath(
                    ".//li[@class='jobLocation']/text()").extract_first()
            except:
                country_areas = ""

            try:
                category = job_item_sel.xpath(".//li[@class='jobType']").xpath(
                    "normalize-space(string())").extract_first()
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
                job_post_date = all_child_elem_job_item[1].xpath(
                    "text()").extract_first()
                try:
                    job_post_date_num = re.findall(r'[\d]+', job_post_date)[0]
                    job_post_date_num = int(job_post_date_num)

                    if job_post_date_num:

                        second = 'שְׁנִיָה'.decode('utf-8')
                        seconds = 'שניות'.decode('utf-8')
                        minute = 'דַקָה'.decode('utf-8')
                        minutes = 'דקות'.decode('utf-8')
                        hour = 'שָׁעָה'.decode('utf-8')
                        hours = 'שעות'.decode('utf-8')
                        day = 'יְוֹם'.decode('utf-8')
                        days = 'ימים'.decode('utf-8')
                        # month = 'חוֹדֶשׁ'.decode('utf-8')
                        # months = 'חודשים'.decode('utf-8')
                        hms = [second, seconds, minute, minutes, hour, hours]

                        if day in job_post_date:
                            job_post_date = datetime.date.today() - \
                                datetime.timedelta(days=job_post_date_num)
                            job_post_date = job_post_date.strftime("%d/%m/%Y")
                        elif days in job_post_date:
                            job_post_date = datetime.date.today() - \
                                datetime.timedelta(days=job_post_date_num)
                            job_post_date = job_post_date.strftime("%d/%m/%Y")

                        elif [x for x in hms if x in job_post_date]:
                            job_post_date = datetime.date.today()
                            job_post_date = job_post_date.strftime("%d/%m/%Y")

                        elif job_post_date_num == 0:
                            job_post_date = datetime.date.today()
                            job_post_date = job_post_date.strftime("%d/%m/%Y")

                        else:
                            job_post_date = job_post_date
                except:
                    job_post_date = all_child_elem_job_item[1].xpath(
                        "text()").extract_first()

            except:
                job_post_date = ""

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
            yield item

        pagi_link_sel_list = response.xpath("//a[@class='paging']")

        for pagi_link_sel in pagi_link_sel_list:
            nextpagi_text = pagi_link_sel.xpath(
                "text()").extract_first()
            if nextpagi_text == u'\u05d4\u05d1\u05d0\xbb' or nextpagi_text == u'\u05d4\u05d1\u05d0 \xbb':
                next_url = response.urljoin(pagi_link_sel.xpath("@href").extract_first())
                yield scrapy.Request(next_url, self.parse_each_location, dont_filter=True,
                                     meta={'location_id': location_id})
