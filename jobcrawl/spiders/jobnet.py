# -*- coding: utf-8 -*-
import sys
import codecs
import locale
import scrapy
import urlparse

from jobcrawl.items import JobItem


class JobNetSpider(scrapy.Spider):
    name = "jobnet"
    allowed_domains = ["jobnet.co.il"]
    start_urls = (
        'http://www.jobnet.co.il/positionresults.aspx?p=0',
    )

    def __init__(self):

        sys.stdout = codecs.getwriter(
            locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

    def parse(self, response):

        job_container_list = response.xpath("//div[@typeof='JobPosting']")

        for job_container in job_container_list:
            job_title = job_container.xpath(
                ".//div[@class='divTitle']/h2/a/text()"
            ).extract_first()

            job_link = job_container.xpath(
                ".//div[@class='divTitle']/h2/a/@href"
            ).extract_first()

            try:
                job_id = urlparse.parse_qs(job_link).get('positionid')[0]
            except:
                job_id = ""
            job_link = "http://www.jobnet.co.il/" + job_link

            job_post_date = job_container.xpath(
                ".//p[@property='datePosted']/text()"
            ).extract_first()

            company_name = job_container.xpath(
                ".//span[@class='PositionCompanyName']/text()"
            ).extract_first()

            try:
                company_id_str = job_container.xpath(
                    ".//span[@class='PositionCompanyName']/@onclick"
                ).extract_first()
                company_query_params = company_id_str.replace(
                    'javascript:window.open(', ''
                ).split(",")[0].replace("'", '')
                company_id = urlparse.parse_qs(company_query_params).get(
                    'companyid')[0]
            except:
                company_id = ""

            company_jobs = (
                "http://www.jobnet.co.il/positionresults"
                ".aspx?companyid=" + company_id
            )

            job_heads = job_container.xpath(
                ".//div[contains(@class, 'jobContainerInfo')]"
                "/p[@class='headLines']/text()"
            ).extract()
            job_description = []
            try:
                job_headline_1 = job_heads[1]
                if job_headline_1:
                    job_description.append(job_headline_1.strip())
            except:
                pass
            job_desc = job_container.xpath(
                ".//div[contains(@class, 'jobContainerInfo')]"
                "/i[@property='description']"
            ).xpath("normalize-space(string())").extract_first()
            if job_desc:
                job_description.append(job_desc.strip())
            try:
                job_headline_2 = job_heads[2]
                if job_headline_2:
                    job_description.append(job_headline_2.strip())
            except:
                pass
            job_skills = job_container.xpath(
                ".//div[contains(@class, 'jobContainerInfo')]"
                "/i[@property='skills']"
            ).xpath("normalize-space(string())").extract_first()
            if job_skills:
                job_description.append(job_skills.strip())
            job_description = "\n".join(job_description)
            country_areas = job_container.xpath(
                ".//div[@class='jobContainerLocation']/i"
            ).xpath("normalize-space(string())").extract_first()

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
            yield item

        # handling pagination
        next_pagi = response.xpath(
            "//div[@class='searchInfo']/p/text()").extract_first()
        total = int(next_pagi.split(' ')[-1])
        per_page = float(next_pagi.split(' ')[0])
        if total / per_page > total / int(per_page):
            pages = total / int(per_page) + 1
        else:
            pages = total / int(per_page)
        for i in range(1, pages):
            next_url = "http://www.jobnet.co.il/positionresults.aspx?p=" +\
                str(i)
            yield scrapy.Request(next_url, callback=self.parse)
