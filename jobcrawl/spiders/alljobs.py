import re
import os
# import sys
import time
# import codecs
import scrapy
# import locale
import urllib.parse as urlparse
from jobcrawl.items import JobItem
from scrapy.http import HtmlResponse
from jobcrawl.js_scraper import JSScraperRunner


class AllJobsSpider(scrapy.Spider):
    """ Spider to scrape job information from site http://www.alljobs.co.il """

    name = "alljobs"
    allowed_domains = ["http://www.alljobs.co.il"]
    start_urls = ['https://www.alljobs.co.il/SearchResultsGuest.aspx?page=1&position=&type=&freetxt=&city=&region=']

    def __init__(self):
        # sys.stdout = codecs.getwriter(
        #     locale.getpreferredencoding())(sys.stdout)
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        self.html_dir_name = 'alljobs_htmls'
        if not os.path.exists(self.html_dir_name):
            os.makedirs(self.html_dir_name)
        self.runner = JSScraperRunner(self.logger)

    def parse(self, response):
        url = response.url
        parsed = urlparse.urlparse(url)
        page = urlparse.parse_qs(parsed.query)['page'][0]
        fname = "page_{}.html".format(page)
        output_file = os.path.join(self.html_dir_name, fname)

        # Run JS Crawler
        self.runner.run(url, output_file)

        if os.path.isfile(output_file):
            body = open(output_file).read()
            response = HtmlResponse(url=url, body=body, encoding='utf-8')

            # Parse the HTML response
            job_container_div_list = response.xpath("//div[@class='open-board']") or []

            for job_item_sel in job_container_div_list:
                job_id_container = job_item_sel.xpath(".//@id").extract_first()
                job_id_group = re.findall(r'[\d]+', job_id_container)
                if job_id_group:
                    job_id = job_id_group[0]
                    job_link = "http://www.alljobs.co.il/Search/" \
                               "UploadSingle.aspx?JobID={}".format(job_id)
                else:
                    job_id = ''
                    job_link = ''

                try:
                    job_date = job_item_sel.xpath(
                        './/div[@class="job-content-top-date"]/text()'
                    ).extract_first()
                    date = job_date.split(' ')[-1]
                except:
                    date = ""

                try:
                    job_class = job_item_sel.xpath(
                        './/div[@class="job-content-top-status-text"]/text()'
                    ).extract_first()
                except:
                    job_class = ""

                try:

                    job_title = job_item_sel.xpath(
                        './/div[contains(@class, "job-content-top-title")]'
                        '//div/a/h2/text()').extract_first()
                except:
                    job_title = ""

                try:
                    company = job_item_sel.xpath(
                        './/div[@class="T14"]/a/text()').extract_first()
                    if company:
                        company = company.strip()
                except:
                    company = ""

                try:
                    company_jobs = job_item_sel.xpath(
                        './/div[@class="job-company-details"]'
                        '//a[@class="L_Blue gad"]/@href').extract_first()
                    company_jobs = response.urljoin(company_jobs)
                except:
                    company_jobs = ""

                if 'SearchResultsGuest.aspx' in company_jobs:
                    # if no company jobs link, then set job link
                    company_jobs = job_link
                try:
                    location_list_sel = response.xpath(
                        "//div[@class='job-regions-box']")
                    if location_list_sel:
                        location_list = location_list_sel.xpath(
                            ".//a/text()").extract()
                        country_areas = ", ".join(location_list)
                    else:
                        country_areas = job_item_sel.xpath(
                            './/div[@class="job-content-top-location"]/a/text()'
                        ).extract_first()

                except:
                    country_areas = ""

                job_description = ""
                try:
                    description_div_id = "job-body-content" + \
                                         str(job_id)
                    description_div = job_item_sel.xpath(
                        './/div[@id="' + description_div_id + '"]/*')
                    for dv in description_div:
                        description_text = dv.xpath(
                            "normalize-space(string())").extract_first()
                        if description_text:
                            job_description += description_text
                            job_description += "\n"

                except:
                    job_description = ""

                item = JobItem()

                item['Job'] = {
                    'Site': 'AllJobs',
                    'Company': company,
                    'Company_jobs': company_jobs,
                    'Job_id': job_id,
                    'Job_title': job_title,
                    'Job_Description': job_description,
                    'Job_Post_Date': date,
                    'Job_URL': job_link,
                    'Country_Areas': country_areas,
                    'Job_categories': '',
                    'AllJobs_Job_class': job_class,
                    'unique_id': 'alljobs_{}'.format(job_id)
                }

                yield item

            next_page = response.xpath('//div[@class="jobs-paging-next"]/a/@href').extract_first()
            if next_page:
                time.sleep(1)
                yield scrapy.Request(
                    response.urljoin(next_page), self.parse, dont_filter=True)
        else:
            self.logger.error("Output file not present. url=%s", url)
