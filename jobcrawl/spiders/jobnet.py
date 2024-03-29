# -*- coding: utf-8 -*-
# import sys
# import codecs
# import locale
import scrapy
from urllib.parse import urlparse, parse_qs

from jobcrawl.items import JobItem
from jobcrawl.endtime_check import reached_endtime


class JobNetSpider(scrapy.Spider):
    name = "jobnet"
    allowed_domains = ["jobnet.co.il"]
    start_urls = (
        'https://www.jobnet.co.il/jobs?p=0',
    )

    def __init__(self):
        # sys.stdout = codecs.getwriter(
        #     locale.getpreferredencoding())(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        self.total_jobs = 0

    def parse(self, response):

        job_table = response.xpath("//table[@id='ContentPlaceHolder1_ucSearhRes_rptResults']")
        job_rows = job_table.xpath(".//tr")
        page_job_count = 0
        for job_row in job_rows:
            job_title = job_row.xpath(".//h2[@itemprop='title']").xpath(
                "normalize-space(string())").extract_first()

            job_link = job_row.xpath(".//a[contains(@href, '/jobs?')]/@href").extract_first()
            if job_link:
                job_link = "http://www.jobnet.co.il{}".format(job_link)
            else:
                job_link = ""

            try:
                parsed = urlparse(job_link)
                qs = parse_qs(parsed.query)
                job_id = qs.get('positionid')[0]
            except:
                job_id = ""

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
            page_job_count += 1
            self.total_jobs += 1
            yield item

        # handling pagination
        current_pg_from_query = int(response.url.split('?p=')[-1])
        selected_page = response.xpath("//a[@class='btnPaging Selected']")
        selected_pg_no = selected_page.xpath("normalize-space(string())").extract_first()
        try:
            selected_page_no = int(selected_pg_no)
        except TypeError:
            print("Jobnet: Failed to get selected page no as int: {} (current_pg_from_query={})"
                  "".format(selected_pg_no, current_pg_from_query))
            selected_page_no = None
        self.logger.info("Jobnet: Page %s job count = %s, total_jobs=%s", current_pg_from_query, page_job_count, self.total_jobs)
        if reached_endtime():
            self.logger.info("Jobnet: End run because endtime is reached")
        elif selected_page_no is not None:
            if current_pg_from_query != selected_page_no:
                next_url = "http://www.jobnet.co.il/jobs?p=" + str(selected_page_no)
                yield scrapy.Request(next_url, callback=self.parse)
            else:
                available_pages = response.xpath("//a[@class='btnPaging ']")
                available_page_no = [i.xpath("normalize-space(string())").extract_first() for i in available_pages]
                all_available_page_nos = []
                for av_page_no in available_page_no:
                    try:
                        all_available_page_nos.append(int(av_page_no))
                    except:
                        pass
                next_page = selected_page_no + 1
                if next_page in all_available_page_nos:
                    next_url = "http://www.jobnet.co.il/jobs?p=" + str(next_page)
                    yield scrapy.Request(next_url, callback=self.parse)
