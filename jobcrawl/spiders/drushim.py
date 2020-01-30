# -*- coding: utf-8 -*-
import scrapy
# from scrapy.shell import inspect_response
from jobcrawl.items import JobItem

import sys
import locale
import codecs
import re
import datetime


class DrushimSpider(scrapy.Spider):
    name = "drushim"
    allowed_domains = ["drushim.co.il"]
    start_urls = (
        'https://www.drushim.co.il/jobs/search',
    )

    def __init__(self):

        sys.stdout = codecs.getwriter(
            locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

    def parse(self, response):

        main_content_job_list = response.xpath(
            "//div[@id='MainContent_JobList_jobList']")
        job_container_list = main_content_job_list.xpath(
            ".//div[@class='jobContainer']")

        for job_container in job_container_list:

            job_link = job_container.xpath(
                ".//a[@class='fullPage']/@href").extract_first()

            try:
                job_id = job_link.split("/")[-3]
            except:
                job_id = ""

            try:
                job_title = job_container.xpath(
                    ".//h2[@class='jobName']/text()").extract_first()
            except:
                job_title = ""

            try:
                company = job_container.xpath(
                    ".//div[@class='fieldContainer vertical first']/"
                    "span[@class='fieldTitle']/text()").extract_first()

            except:
                company = ""

            try:
                company_jobs = job_container.xpath(
                    ".//a[@class='companyLink noToggle']/@href"
                ).extract_first()
            except:
                company_jobs = job_link

            job_fields_sel_list = job_container.xpath(
                ".//div[@class='jobFields']/*")
            job_fields_sel_list = job_fields_sel_list[1:]

            try:
                job_description = "\n".join(
                    job_fields_sel_list.xpath("string()").extract()
                )
            except:
                job_description = ""

            try:
                country_areas = job_fields_sel_list[3].xpath(
                    ".//span/text()")[-1].extract()
            except:
                country_areas = ""

            try:
                category = job_fields_sel_list[4].xpath(
                    ".//span/text()")[-1].extract()
            except:
                category = ""

            try:
                job_post_date = job_container.xpath(
                    ".//span[@class='jobDate rtl']/text()"
                ).extract_first()

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
                    job_post_date = job_container.xpath(
                        ".//span[@class='jobDate rtl']/text()"
                    ).extract_first()

            except:
                job_post_date = ""

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

        next_pagi = main_content_job_list.xpath(
            ".//a[@class='pager lightBg stdButton']/@href"
        ).extract_first()
        # next_pagi = 'https://www.drushim.co.il/jobs/?page=2'

        if next_pagi:
            yield scrapy.Request(next_pagi, callback=self.parse)
