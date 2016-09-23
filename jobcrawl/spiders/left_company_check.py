import sys
import codecs
import scrapy
import locale
from xlrd import open_workbook
from openpyxl import load_workbook

from scrapy import signals
import pandas as pd
import datetime
from scrapy.xlib.pydispatch import dispatcher

from jobcrawl.mailer import send_email
import os

today = datetime.date.today()
today_str = today.strftime("%Y_%m_%d")


class LeftCompany(scrapy.Spider):
    """ Spider to verify removed companies """

    name = "left"
    allowed_domains = ["jobmaster.co.il", "drushim.co.il", "alljobs.co.il"]

    start_urls = []
    excel_dir = 'daily_competitor_client_changes'
    excel_path = '{}/{}_Daily-Competitor-Client-Change.xlsx'.format(
        excel_dir, today_str)

    def __init__(self):

        dispatcher.connect(self.spider_closed, signals.spider_closed)
        sys.stdout = codecs.getwriter(
            locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

    def spider_closed(self, spider):

        new_companies_df = pd.read_excel(
            self.excel_path, sheetname='New_Company')
        left_companies_df = pd.read_excel(
            self.excel_path, sheetname='Companies_That_left')
        left_companies_df = left_companies_df.drop_duplicates(keep=False)

        writer = pd.ExcelWriter(self.excel_path, engine='openpyxl')
        new_companies_df.to_excel(writer, 'New_Company', index=False)
        left_companies_df.to_excel(writer, 'Companies_That_left', index=False)
        writer.save()

        directory = "IL-jobcrawl-data"

        try:
            # send email for competitior changes
            drushim_file = "{}/{}_Drushim_crawled_complete.xls".format(
                directory, today_str)
            jobmaster_file = "{}/{}_Jobmaster_crawled_complete.xls".format(
                directory, today_str)
            alljobs_file = "{}/{}_Alljobs_crawled_complete.xls".format(
                directory, today_str)
            directory = 'daily_competitor_client_changes'
            file_name = '{}_Daily-Competitor-Client-Change.xlsx'.format(
                today_str)
            body = "Please find the attachment for {}".format(file_name)

            send_email(directory=directory, file_name=file_name, body=body)

            """After sending email remove the crawled_complete.xls file"""
            try:
                os.remove(drushim_file)
                os.remove(jobmaster_file)
                os.remove(alljobs_file)
            except:
                pass
        except:
            pass

    def start_requests(self):

        # excel_path = 'daily_competitor_client_changes/main.xlsx'
        wb = open_workbook(self.excel_path)
        sheet = wb.sheet_by_name('Companies_That_left')
        for i in range(1, sheet.nrows):
            row = sheet.row_values(i)
            company = row[1]
            site = row[0]
            company_url = row[2]

            company_jobs = row[3]
            if company_url:
                company_detail = [site, company, company_url, company_jobs]
                yield scrapy.Request(
                    company_url, self.parse,
                    meta={'company_detail': company_detail},
                    dont_filter=True
                )

    def parse(self, response):

        company_detail = response.meta['company_detail']

        drushimob_div = response.xpath("//div[@class='jobCount']")  # drushim
        alljobs_jobs_div = response.xpath("//div[@class='job-paging']")
        jobmaster_jobs = response.xpath(
            "//div[@class='CenterContent']/article")

        if (
            drushimob_div or alljobs_jobs_div or jobmaster_jobs or
            '/Search/' in response.url
        ):
            # print ('Job', response.url)
            wb = load_workbook(self.excel_path)
            sheet = wb.get_sheet_by_name('Companies_That_left')

            sheet.append(company_detail)
            wb.save(self.excel_path)
