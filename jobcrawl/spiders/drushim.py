# -*- coding: utf-8 -*-
import scrapy
from jobcrawl.items import JobItem

import sys
import locale
import codecs
import re


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
        # inspect_response(response,self)
        main_content_job_list = response.xpath(
            "//div[@id='MainContent_JobList_jobList']")
        job_container = main_content_job_list.xpath(
            ".//div[@class='jobContainer']")

        job_link_list = job_container.xpath(
            ".//a[@class='fullPage']/@href").extract()
        # job_link_list = job_link_list[1:2]

        for job_link in job_link_list:
            yield scrapy.Request(
                job_link, callback=self.parse_each_job,
                dont_filter=True)

        next_pagi = main_content_job_list.xpath(
            ".//a[@class='pager lightBg stdButton']/@href").extract_first()
        # next_pagi = 'https://www.drushim.co.il/jobs/?page=2'

        if next_pagi:
            yield scrapy.Request(next_pagi, callback=self.parse)

    def parse_each_job(self, response):
        # inspect_response(response,self)
        job_container = response.xpath("//div[@class='jobContainer']")
        job_fields_sel_list = response.xpath("//div[@class='jobFields']/*")

        try:
            job_id = response.url.split("/")[-3]
        except:
            job_id = ""

        try:
            job_title = job_container.xpath(
                ".//h1[@class='jobName']/text()").extract_first()
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
                ".//a[@class='companyLink noToggle']/@href").extract_first()
        except:
            company_jobs = ""

        field_container_sel_list = job_container.xpath(
            ".//div[@class='fieldContainer horizontal']")
        try:
            job_description = "\n".join(
                job_fields_sel_list.xpath("string()").extract())
            job_post_date = re.findall(r'\d\d/\d\d/\d\d\d\d', job_description)

            if job_post_date:
                job_post_date = job_post_date[0]
            else:

                field_container_sel_list = job_container.xpath(
                    ".//div[@class='fieldContainer horizontal']")
                job_post_date = field_container_sel_list[-1].xpath(
                    ".//span[@class='fieldText rtl']/text()").extract_first()

        except:

            job_post_date = ""

        try:
            country_areas = job_fields_sel_list[4].xpath(
                ".//span/text()")[-1].extract()
        except:
            country_areas = ""

        try:
            category = field_container_sel_list[2].xpath(
                ".//a/text()").extract_first()
        except:
            category = ""

        try:
            job_description = "\n".join(
                job_fields_sel_list[1:].xpath("string()").extract())
            job_description = job_description.replace(
                re.findall(
                    r'\d\d/\d\d/\d\d\d\d\n?(.*)',
                    job_description
                )[0], ""
            )

        except:
            job_description = ""

        item = JobItem()

        item['Job'] = {
            'Site': 'Drushim',
            'Company': company,
            'Company_jobs': company_jobs,
            'Job_id': job_id,
            'Job_title': job_title,
            'Job_Description': job_description,
            'Job_Post_Date': job_post_date,
            'Job_URL': response.url,
            'Country_Areas': country_areas,
            'Job_categories': category,
            'AllJobs_Job_class': '',
            'unique_id': 'drushim_{}'.format(job_id)
        }
        yield item
