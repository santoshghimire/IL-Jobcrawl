# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import os
import sys
import locale
import xlwt
import codecs
import pymysql
import pandas as pd
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem
import datetime
import clientchanges
from openpyxl import load_workbook
from mailer import send_email

from openpyxl import Workbook

today = datetime.date.today()
today_str = today.strftime("%Y_%m_%d")

pymysql.install_as_MySQLdb()


directory = "./IL-jobcrawl-data"
# main_excel_file_path = "{}/{}_site_data.xlsx".format(directory, today_str)
main_excel_file_path = "{}/{}_Daily-List-Of-Competitor-Jobs.xlsx".format(
    directory, today_str)


class JobscrawlerPipeline(object):

    def open_spider(self, spider):

        if spider.name != 'left':

            if not os.path.exists(directory):
                os.mkdir(directory)

            self.ids_seen = set()
            # name of the sheet for current website
            self.sheet_name = spider.name.title()
            self.temp_each_site_excel_file_path = '{}/{}_{}.xls'.format(
                directory, today_str, self.sheet_name
            )  # temporary xls file which contain scraped item
            sys.stdout = codecs.getwriter(
                locale.getpreferredencoding())(sys.stdout)
            reload(sys)
            sys.setdefaultencoding('utf-8')

            """ Create main excel file with all sheets"""
            if not os.path.isfile(main_excel_file_path):
                wb = Workbook(encoding='utf-8')
                # wb = Workbook()
                wb.active.title = 'Drushim'
                wb.create_sheet('Jobmaster')
                wb.create_sheet('Alljobs')

                wb.save(main_excel_file_path)

            """ To create each site's excel file"""
            self.file_exists = False
            self.book = xlwt.Workbook(encoding='utf-8')
            self.sheet = self.book.add_sheet(self.sheet_name)
            self.sheet.write(0, 0, 'Site')
            self.sheet.write(0, 1, 'Company')
            self.sheet.write(0, 2, 'Company_jobs')
            self.sheet.write(0, 3, 'Job_id')
            self.sheet.write(0, 4, 'Job_title')
            self.sheet.write(0, 5, 'Job_Description')
            self.sheet.write(0, 6, 'Job_Post_Date')
            self.sheet.write(0, 7, 'Job_URL')
            self.sheet.write(0, 8, 'Country_Areas')
            self.sheet.write(0, 9, 'Job_categories')
            self.sheet.write(0, 10, 'AllJobs_Job_class')
            self.sheet.write(0, 11, 'Crawl_Date')
            self.sheet.write(0, 12, 'unique_id')
            self.next_row = self.sheet.last_used_row

    def close_spider(self, spider):
        if spider.name != 'left':
            # save each spider excel file
            self.book.save(self.temp_each_site_excel_file_path)
            try:
                main_book = load_workbook(main_excel_file_path)
                main_writer = pd.ExcelWriter(
                    main_excel_file_path, engine='openpyxl')
                main_writer.book = main_book

                main_writer.sheets = dict(
                    (ws.title, ws) for ws in main_book.worksheets)
                unsorted_xls_df = pd.read_excel(
                    self.temp_each_site_excel_file_path)
                sorted_xls = unsorted_xls_df.sort_values(by='Company')
                sorted_xls = sorted_xls.drop_duplicates()

                sorted_xls.to_excel(main_writer, self.sheet_name, index=False)
                main_writer.save()
                try:
                    os.remove(self.temp_each_site_excel_file_path)
                except:
                    pass
            except:
                spider.log(
                    "openpyxl BadZipfile ERROR Dosen't effect our automation")
                # Error in attaching file to main sheet so
                # send email for total site data
                directory = 'IL-jobcrawl-data'
                file_name = '{}_{}.xlsx'.format(
                    today_str, self.sheet_name)
                body = "Please find the attachment for {}".format(file_name)

                send_email(directory=directory, file_name=file_name, body=body)

            directory = './IL-jobcrawl-data'
            open('{}/{}_{}_data_transfer_complete.xls'.format(
                directory, today_str, spider.name.title()), 'a')

            drushim_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                directory, today_str, 'Drushim')
            alljobs_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                directory, today_str, 'Alljobs')
            jobmaster_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                directory, today_str, 'Jobmaster')

            """ check if all crawled complete for all sites excel file for 3
            sites exists and proceed creating client changes xls"""
            if (
                os.path.isfile(drushim_file) and
                os.path.isfile(jobmaster_file) and
                os.path.isfile(alljobs_file)
            ):
                os.remove(drushim_file)
                os.remove(alljobs_file)
                os.remove(jobmaster_file)

                # send email for total site data
                directory = 'IL-jobcrawl-data'
                file_name = '{}_Daily-List-Of-Competitor-Jobs.xlsx'.format(
                    today_str)
                body = "Please find the attachment for {}".format(file_name)

                send_email(directory=directory, file_name=file_name, body=body)

    def process_item(self, item, spider):

        if spider.name != 'left':
            crawl_date = datetime.date.today()
            crawl_date_str = crawl_date.strftime("%d/%m/%Y")
            if item['Job']['unique_id'] in self.ids_seen:
                raise DropItem(
                    "*" * 100 + "\n" +
                    "Duplicate item found: %s" % item + "\n" + "*" * 100)
            else:
                self.ids_seen.add(item['Job']['unique_id'])

                self.next_row += 1
                self.sheet.write(self.next_row, 0, item['Job']['Site'])
                self.sheet.write(self.next_row, 1, item['Job']['Company'])
                self.sheet.write(self.next_row, 2, item['Job']['Company_jobs'])
                self.sheet.write(self.next_row, 3, item['Job']['Job_id'])
                self.sheet.write(self.next_row, 4, item['Job']['Job_title'])
                self.sheet.write(
                    self.next_row, 5, item['Job']['Job_Description'])
                self.sheet.write(
                    self.next_row, 6, item['Job']['Job_Post_Date'])
                self.sheet.write(self.next_row, 7, item['Job']['Job_URL'])
                self.sheet.write(
                    self.next_row, 8, item['Job']['Country_Areas'])
                self.sheet.write(
                    self.next_row, 9, item['Job']['Job_categories'])
                self.sheet.write(
                    self.next_row, 10, item['Job']['AllJobs_Job_class'])
                self.sheet.write(self.next_row, 11, crawl_date_str)
                self.sheet.write(self.next_row, 12, item['Job']['unique_id'])
                return item


class MySQLPipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool
        self.ids_seen = set()

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):

        if spider.name != 'left':
            if item['Job']['unique_id'] in self.ids_seen:
                raise DropItem(
                    "+" * 100 + "\n" + "Duplicate item found: %s" % item +
                    "\n" + "+" * 100)
            else:
                self.ids_seen.add(item['Job']['unique_id'])
                dbpool = self.dbpool.runInteraction(self.insert, item, spider)
                dbpool.addErrback(self.handle_error, item, spider)
                dbpool.addBoth(lambda _: item)
                return dbpool

    def insert(self, conn, item, spider):

        if spider.name != 'left':
            crawl_date = datetime.date.today()
            crawl_date_str = crawl_date.strftime("%d/%m/%Y")

            conn.execute("""
                INSERT INTO sites_datas(
                Site,
                Company,
                Company_jobs,
                Job_id,
                Job_title,
                Job_Description,
                Job_Post_Date,
                Job_URL,
                Country_Areas,
                Job_categories,
                AllJobs_Job_class,
                Crawl_Date,
                unique_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s)
            """, (
                item['Job']['Site'],
                item['Job']['Company'],
                item['Job']['Company_jobs'],
                item['Job']['Job_id'],
                item['Job']['Job_title'],
                item['Job']['Job_Description'],
                item['Job']['Job_Post_Date'],
                item['Job']['Job_URL'],
                item['Job']['Country_Areas'],
                item['Job']['Job_categories'],
                item['Job']['AllJobs_Job_class'],
                crawl_date_str,
                item['Job']['unique_id']

            ))
            spider.log("Item stored in dbSchema: %s %r" % (
                item['Job']['Job_id'], item))

    def close_spider(self, spider):
        if spider.name != 'left':
            directory = "./IL-jobcrawl-data"
            # clientchanges.ClientChanges()
            """ create a ...crawled_complete.xls file for each
            spider to notify crawling has finished"""
            open('{}/{}_{}_crawled_complete.xls'.format(
                directory, today_str, spider.name.title()), 'a')

            drushim_file = "{}/{}_Drushim_crawled_complete.xls".format(
                directory, today_str)
            jobmaster_file = "{}/{}_Jobmaster_crawled_complete.xls".format(
                directory, today_str)
            alljobs_file = "{}/{}_Alljobs_crawled_complete.xls".format(
                directory, today_str)
            """ check if all the ...crawled_complete.xls excel file for 3 sites exists and
            proceed creating client changes xls"""
            if (
                os.path.isfile(drushim_file) and
                os.path.isfile(jobmaster_file) and
                os.path.isfile(alljobs_file)
            ):
                clientchanges.ClientChanges()

    def handle_error(self, failure, item, spider):
        """Handle occurred on dbSchema interaction."""
        self.logger.info("DB Schema Handled")
