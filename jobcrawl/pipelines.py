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
from scrapy.exceptions import DropItem
import datetime
from openpyxl import Workbook

today = datetime.date.today()
today_str = today.strftime("%Y_%m_%d")

directory = "./IL-jobcrawl-data"


class JobscrawlerPipeline(object):

    def __init__(self, db_setting):
        self.conn = pymysql.connect(
            host=db_setting['host'],
            port=3306,
            user=db_setting['user'],
            passwd=db_setting['passwd'],
            db=db_setting['db'],
            charset='utf8'
        )
        self.cur = self.conn.cursor()
        self.dropped_count = {
            'alljobs': 0,
            'drushim': 0,
            'jobmaster': 0,
            'jobnet': 0,
        }
        # self.ids_seen = set()

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_setting = {
            'host': settings.get("MYSQL_HOST"),
            'user': settings.get('MYSQL_USER'),
            'passwd': settings.get('MYSQL_PASSWORD'),
            'db': settings.get('MYSQL_DBNAME')
        }
        return cls(db_setting)

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
            # insert item
            try:
                self.cur.execute("""
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
                self.conn.commit()
            except pymysql.err.IntegrityError:
                if spider.name in self.dropped_count:
                    self.dropped_count[spider.name] += 1
                else:
                    self.dropped_count[spider.name] = 1

                raise DropItem(
                    "\n" + "+" * 50 + "\n" +
                    "Duplicate item found: %s" % item +
                    "\n" + "+" * 50)

            # write to excel
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
            return item

    def close_spider(self, spider):
        if spider.name != 'left':
            # save each spider excel file
            self.workbook.save(self.temp_each_site_excel_file_path)
        try:
            self.conn.close()
        except:
            pass
        spider.logger.info("Total Dropped Item count for %s = %s",
            spider.name, self.dropped_count.get(spider.name))
