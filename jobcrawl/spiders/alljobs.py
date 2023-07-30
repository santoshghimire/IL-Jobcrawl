import re
import os
# import sys
import time
# import codecs
import scrapy
# import locale
from datetime import datetime
import urllib.parse as urlparse
from jobcrawl.items import JobItem
from scrapy.http import HtmlResponse
from jobcrawl.js_scraper import JSScraperRunner


class AllJobsSpider(scrapy.Spider):
    """ Spider to scrape job information from site http://www.alljobs.co.il """

    name = "alljobs"
    allowed_domains = ["alljobs.co.il"]
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
        self.total_jobs = 0
        self.max_page = 1500

    def should_end_run(self, page, endpage_scraped=False):
        text = 'scraped' if endpage_scraped else 'not scraped'
        if self.reached_endtime():
            self.logger.error("Alljobs: Reached Endtime. Ending run. End page (%s) = %s, total_jobs=%s", text, page, self.total_jobs)
            return True
        elif self.reached_maxpage(page):
            self.logger.error("Alljobs: Reached max page. Ending run. End page (%s) = %s, total_jobs=%s", text, page, self.total_jobs)
            return True
        return False

    def reached_endtime(self):
        now = datetime.utcnow()
        endtime = now.replace(hour=12, minute=30, second=0, microsecond=0)
        return now > endtime

    def reached_maxpage(self, page):
        if page.isdigit():
            return int(page) >= self.max_page
        return False

    def get_sequential_nextpage(self, current_url):
        parsed = urlparse.urlparse(current_url)
        page = urlparse.parse_qs(parsed.query)['page'][0]
        if not page.isdigit():
            return False
        next_page_no = str(int(page) + 1)
        return url.replace('page={}'.format(page), 'page={}'.format(next_page_no))

    def parse(self, response):
        url = response.url
        parsed = urlparse.urlparse(url)
        page = urlparse.parse_qs(parsed.query)['page'][0]
        fname = "page_{}.html".format(page)
        output_file = os.path.join(self.html_dir_name, fname)

        proper_nextpage_found = False
        for attempt in range(5):
            # Run JS Crawler
            self.runner.run(url, output_file)

            if not os.path.isfile(output_file):
                self.logger.error("Output file not present. url=%s, attempt=%s, remaining=%s", url, attempt, 4 - attempt)
                if self.should_end_run(page, endpage_scraped=False):
                    break
                continue

            fobj = open(output_file)
            body = fobj.read()
            response = HtmlResponse(url=url, body=body, encoding='utf-8')
            fobj.close()
            try:
                os.remove(output_file)
            except OSError:
                pass

            # Parse the HTML response
            job_container_div_list_open = response.xpath("//div[@class='open-board']") or []
            job_container_div_list_organic = response.xpath("//div[@class='organic-board']") or []
            job_container_div_list = job_container_div_list_open + job_container_div_list_organic

            page_job_count = 0
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
                    # TODO: Change minutes ago to date format
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
                    if not job_title:
                        job_title = job_item_sel.xpath(
                            './/div[contains(@class, "job-content-top-title")]'
                            '//div/a/h3/text()').extract_first()
                    if job_title:
                        job_title = job_title.strip()
                except:
                    job_title = ""

                try:
                    company = job_item_sel.xpath(
                        './/div[@class="T14"]/a/text()').extract_first()
                    if company:
                        company = company.strip()
                    else:
                        # Confidential company
                        company = job_item_sel.xpath(
                            './/div[@class="T14"]').xpath('/normalize-space(string())').extract_first()
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
                page_job_count += 1
                self.total_jobs += 1
                yield item

            self.logger.info("Alljobs: Page %s job count = %s, total_jobs=%s", page, page_job_count, self.total_jobs)
            next_page = response.xpath('//div[@class="jobs-paging-next"]/a/@href').extract_first()
            if next_page:
                if self.should_end_run(page, endpage_scraped=True):
                    break
                time.sleep(1)
                proper_nextpage_found = True
                yield scrapy.Request(
                    response.urljoin(next_page), self.parse, dont_filter=True)
            else:
                proper_nextpage_found = True  # Needed to prevent sequential nextpage run from triggering.
                self.logger.info("Alljobs: Next page not found. Ending run. End page = %s. page job count = %s, total_jobs=%s", page, page_job_count, self.total_jobs)
            break


        if not proper_nextpage_found and not self.should_end_run(page):
            next_page_url = self.get_sequential_nextpage(current_url)
            if next_page_url:
                self.log.info("Alljobs: Parsing sequential nextpage: %s", next_page_url)
                yield scrapy.Request(next_page_url, self.parse, dont_filter=True)
