# -*- coding: utf-8 -*-
# import sys
# import codecs
# import locale
import scrapy
import urllib.parse as urlparse

from jobcrawl.items import JobItem


class JobNetDebugSpider(scrapy.Spider):
    name = "jobnet_debug"
    allowed_domains = ["jobnet.co.il"]
    start_urls = (
        'https://www.jobnet.co.il/jobs?p=147',
        'https://www.jobnet.co.il/jobs?p=210',
    )

    def __init__(self):
        pass
        # sys.stdout = codecs.getwriter(
        #     locale.getpreferredencoding())(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')

    def parse(self, response):
        print("URL = {}".format(response.url))
        job_table = response.xpath("//table[@id='ContentPlaceHolder1_ucSearhRes_rptResults']")
        job_rows = job_table.xpath(".//tr")
        good = 0
        err = 0
        c = 0
        for job_row in job_rows:
            job_title = job_row.xpath(".//h2[@itemprop='title']").xpath(
                "normalize-space(string())").extract_first()

            job_link = job_row.xpath(".//a[contains(@href, '/jobs?')]/@href").extract_first()
            try:
                job_id = urlparse.parse_qs(job_link).get('positionid')[0]
            except:
                job_id = ""

            if job_link:
                job_link = "http://www.jobnet.co.il{}".format(job_link)
            else:
                job_link = ""

            job_post_date = job_row.xpath(
                ".//p[@itemprop='datePosted']/text()"
            ).extract_first()

            company_elem = job_row.xpath(".//p[@itemprop='hiringOrganization']")
            company_name = company_elem.xpath(
                "normalize-space(string())").extract_first()

            company_jobs = company_elem.xpath(".//a/@href").extract_first()
            if company_jobs:
                company_jobs = "http://www.jobnet.co.il{}".format(company_jobs)
            else:
                company_jobs = ''

            job_description = []
            try:
                job_desc = job_row.xpath(".//div[@itemprop='description']").xpath(
                    "normalize-space(string())").extract_first()
                if job_desc:
                    job_description.append(job_desc.strip())
            except:
                pass

            try:
                job_skills = job_row.xpath(".//div[@itemprop='skills']").xpath(
                    "normalize-space(string())").extract_first()
                if job_skills:
                    job_description.append(job_skills.strip())
            except:
                pass

            job_description = "\n".join(job_description)

            try:
                country_areas = job_row.xpath(
                    ".//span[@itemprop='jobLocation']"
                ).xpath("normalize-space(string())").extract_first()
            except:
                country_areas = ""

            try:
                category = job_row.xpath(
                    ".//span[@itemprop='employmentType']"
                ).xpath("normalize-space(string())").extract_first()
            except:
                category = ""

            item = JobItem()
            item['Job'] = {
                'Site': 'JobNet',
                'Company': company_name,
                'Company_jobs': company_jobs,
                'Job_id': job_id,
                'Job_title': job_title,
                'Job_Description': job_description,
                'Job_Post_Date': job_post_date,
                'Job_URL': job_link,
                'Country_Areas': country_areas,
                'Job_categories': category,
                'AllJobs_Job_class': '',
                'unique_id': 'jobnet_{}'.format(job_id)
            }
            if not job_id:
                err += 1
                print("jobnet_debug: defective job row index = {}".format(c))
            else:
                good += 1
            c += 1
        print("jobnet_debug: error rows = {}, good rows = {}".format(err, good))
