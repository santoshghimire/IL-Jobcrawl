# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import  inspect_response

from jobcrawl.items import JobItem
import re
import datetime
import sys
import locale
import codecs


class JobmasterSpider(scrapy.Spider):
    name = "jobmaster"
    allowed_domains = ["http://www.jobmaster.co.il/"]
    start_urls = (
        # 'https://www.jobmaster.co.il/code/home/home.asp?sType=mikumMisra',
        # 'http://www.jobmaster.co.il/code/check/checkhevra.asp?cs=PLPOTOGSMJTPGJNOMJEBSKRMO',
        'http://www.jobmaster.co.il/code/check/search.asp?ezor=42',
    )

    def __init__(self):

        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.total_locations = 0
        self.total_locations_job = 0
        self.all_jobs_count = 0
        self.each_location_total_jobs = 0

    # def parse(self, response):
    #     """
    #     Get all the links for Location
    #     """
    #     job_location_links_list = response.xpath("//a[contains(@href,'/check/search.asp?ezor=')]/@href").extract()

    #     # yield scrapy.Request(response.urljoin(job_location_links_list[-1]), callback=self.parse_each_location,dont_filter=True)

    #     for location_li in job_location_links_list:
    #         self.total_locations += 1
    #         yield scrapy.Request(response.urljoin(location_li), callback=self.parse_each_location, dont_filter=True)

    # def parse_each_location(self, response):
    def parse(self, response):

        """ Parse Each location Link and Extract Each job in this location"""
        job_article_id_list = response.xpath("//article[@class='CardStyle JobItem font14 noWrap']/@id").extract()

        job_id_list = [re.findall(r'[\d]+', x.strip()) for x in job_article_id_list]

        job_id_list = [x[0] for x in job_id_list if x]

        if job_id_list:
            for job_id in job_id_list:
                self.each_location_total_jobs += 1
                job_link = "http://www.jobmaster.co.il/code/check/checknum.asp?flagShare={}".format(job_id)
                yield scrapy.Request(job_link, self.parse_each_job,dont_filter=True, meta={'job_id': job_id})

        pagi_link_sel_list = response.xpath("//a[@class='paging']")

        for pagi_link_sel in pagi_link_sel_list:

            nextpagi_text = pagi_link_sel.xpath("text()").extract_first()
            if nextpagi_text == u'\u05d4\u05d1\u05d0 \xbb':
                yield scrapy.Request(response.urljoin(pagi_link_sel.xpath("@href").extract_first()),
                                     self.parse_each_location, dont_filter=True)

    def parse_each_job(self,response):
        # inspect_response(response, self)

        """ Parse Each job and extract the data points"""
        job_item_sel = response.xpath("//div[@class='JobItemRight']")
        all_child_elem_job_item = job_item_sel.xpath("./*")

        try:

            job_title = job_item_sel.xpath(".//div[@class='CardHeader']/text()").extract_first()
        except:
            job_title = ""

        try:
            company = job_item_sel.xpath(".//a[@class='font14 ByTitle']/text()").extract_first()
            if not company:
                company = job_item_sel.xpath(".//span[@class='font14 ByTitle']/text()").extract_first()
        except:
            company = ""

        try:
            company_jobs = job_item_sel.xpath(".//a[@class='font14 ByTitle']/@href").extract_first()
            if company_jobs:
                company_jobs = response.urljoin(company_jobs)
        except:
            company_jobs = ""

        try:
            country_areas = job_item_sel.xpath(".//li[@class='jobLocation']/text()").extract_first()
        except:
            country_areas = ""

        try:
            category = job_item_sel.xpath(".//span[@class='Gray']").xpath(
                "normalize-space(string())").extract_first().strip()
            category = category.replace("|", ",")
        except:
            category = ''

        try:
            all_child_elem_job_item = job_item_sel.xpath("./*")
            child_index = 3
            job_description = ""
            while child_index < len(all_child_elem_job_item):
                job_description += all_child_elem_job_item[child_index].xpath("normalize-space(string())").extract_first()
                job_description += "\n"
                child_index += 1

        except:

            job_description = ""

        try:
            job_post_date = all_child_elem_job_item[1].xpath("text()").extract_first()
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
                    month = 'חוֹדֶשׁ'.decode('utf-8')
                    months = 'חודשים'.decode('utf-8')
                    hms = [second, seconds, minute, minutes, hour, hours]

                    if day in job_post_date:
                        job_post_date = datetime.date.today() - datetime.timedelta(days=job_post_date_num)
                        job_post_date = job_post_date.strftime("%d/%m/%Y")
                    elif days in job_post_date:
                        job_post_date = datetime.date.today() - datetime.timedelta(days=job_post_date_num)
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
                job_post_date = all_child_elem_job_item[1].xpath("text()").extract_first()

        except:
            job_post_date = ""

        item = JobItem()

        item['Job'] = {
            'Site': 'JobMaster',
            'Company': company,
            'Company_jobs': company_jobs,
            'Job_id': response.meta['job_id'],
            'Job_title': job_title,
            'Job_Description': job_description,
            'Job_Post_Date': job_post_date,
            'Job_URL': response.url,
            'Country_Areas': country_areas,
            'Job_categories': category,
            'AllJobs_Job_class': "",
            'unique_id': 'jobmaster_{}'.format(response.meta['job_id'])
            }

        yield item
