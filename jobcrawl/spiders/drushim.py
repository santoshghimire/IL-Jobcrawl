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
    base_url = "https://www.drushim.co.il"
    start_urls = (
        'https://www.drushim.co.il/jobs/search/%22%22/?ssaen=1',
    )

    def __init__(self):

        sys.stdout = codecs.getwriter(
            locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

    def parse(self, response):

        job_container_list = response.xpath(
            "//div[@class='job-item-main pb-3 job-hdr']")
        for job_container in job_container_list:

            job_link = job_container.xpath(
                ".//div[@class='flex nowrap align-self-center pc-view open-job text-center']/a/@href").extract_first()
            if job_link:
                job_link = "{}{}".format(self.base_url, job_link)

            try:
                job_id = "-".join(job_link.split("/")[-2:])
            except:
                job_id = ""

            try:
                job_title = job_container.xpath(
                    ".//span[@class='job-url primary--text font-weight-bold primary--text']").xpath(
                        "normalize-space(string())").extract_first()
            except:
                job_title = ""

            try:
                company = job_container.xpath(
                    ".//div[@class='layout job-details-top mt-md-2 align-baseline']"
                    "/div[@class='flex grow-none ml-3']/p").xpath("normalize-space(string())").extract_first()
            except:
                company = ""

            try:
                company_jobs = job_container.xpath(
                    ".//div[@class='layout job-details-top mt-md-2 align-baseline']"
                    "/div[@class='flex grow-none ml-3']/p/a/@href").extract_first()
            except:
                company_jobs = job_link

            job_description = ""

            job_sub_details = job_container.xpath(".//div[@class='layout job-details-sub']").xpath(
                "normalize-space(string())").extract_first()
            jsd_val = []
            if job_sub_details:
                jsd_val = job_sub_details.split("|")

            country_areas = jsd_val[0].strip() if jsd_val else ""
            category = jsd_val[2].strip() if jsd_val and len(jsd_val) == 4 else ""
            job_post_date = jsd_val[-1].strip() if jsd_val and len(jsd_val) == 4 else ""

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
