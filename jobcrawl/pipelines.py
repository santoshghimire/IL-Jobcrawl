# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import sys
import locale
import codecs
import pymysql
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem
import datetime
from openpyxl import Workbook

today = datetime.date.today()
today_str = today.strftime("%Y_%m_%d")

pymysql.install_as_MySQLdb()


directory = "./IL-jobcrawl-data"


class JobscrawlerPipeline(object):

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

    def open_spider(self, spider):
        if spider.name != 'left':
            if not os.path.exists(directory):
                os.mkdir(directory)
            # name of the sheet for current website
            self.sheet_name = spider.name.title()
            self.temp_each_site_excel_file_path = '{}/{}_{}.xlsx'.format(
                directory, today_str, self.sheet_name
            )  # temporary xls file which contain scraped item
            sys.stdout = codecs.getwriter(
                locale.getpreferredencoding())(sys.stdout)
            reload(sys)
            sys.setdefaultencoding('utf-8')

            """ To create each site's excel file"""
            self.workbook = Workbook()
            self.workbook.active.title = self.sheet_name

            # grab the active worksheet
            self.ws = self.workbook.active
            self.ws.append([
                'Site', 'Company', 'Company_jobs', 'Job_id',
                'Job_title', 'Job_Description', 'Job_Post_Date',
                'Job_URL', 'Country_Areas', 'Job_categories',
                'AllJobs_Job_class', 'Crawl_Date', 'unique_id'
            ])

    def process_item(self, item, spider):
        if spider.name != 'left':
            crawl_date = datetime.date.today()
            crawl_date_str = crawl_date.strftime("%d/%m/%Y")
            if item['Job']['unique_id'] in self.ids_seen:
                raise DropItem(
                    "+" * 100 + "\n" + "Duplicate item found: %s" % item +
                    "\n" + "+" * 100)
            else:
                self.ids_seen.add(item['Job']['unique_id'])
                dbpool = self.dbpool.runInteraction(self.insert, item, spider)
                dbpool.addErrback(self.handle_error, item, spider)
                dbpool.addBoth(lambda _: item)

                self.ws.append([
                    item['Job']['Site'], item['Job']['Company'],
                    item['Job']['Company_jobs'], item['Job']['Job_id'],
                    item['Job']['Job_title'], item['Job']['Job_Description'],
                    item['Job']['Job_Post_Date'], item['Job']['Job_URL'],
                    item['Job']['Country_Areas'],
                    item['Job']['Job_categories'],
                    item['Job']['AllJobs_Job_class'], crawl_date_str,
                    item['Job']['unique_id']
                ])
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
            # save each spider excel file
            self.workbook.save(self.temp_each_site_excel_file_path)

    def handle_error(self, failure, item, spider):
        """Handle occurred on dbSchema interaction."""
        self.logger.info("DB Schema Handled")
