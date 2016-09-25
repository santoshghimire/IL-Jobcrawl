# -*- coding: utf-8 -*-
# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import sys
import locale
import csv
import codecs
import pymysql
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem
import datetime
import clientchanges
from mailer import send_email

pymysql.install_as_MySQLdb()


class JobscrawlerPipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool
        self.ids_seen = set()
        self.directory = "./IL-jobcrawl-data"
        self.today = datetime.date.today()
        self.today_str = self.today.strftime("%Y_%m_%d")

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
            if not os.path.exists(self.directory):
                os.mkdir(self.directory)
            self.site_name = spider.name.title()
            self.file_name = '{}_{}.csv'.format(self.today_str, self.site_name)
            self.file_path = '{}/{}'.format(
                self.directory, self.file_name
            )
            sys.stdout = codecs.getwriter(
                locale.getpreferredencoding())(sys.stdout)
            reload(sys)
            sys.setdefaultencoding('utf-8')

            # To create each site's csv file
            csvfile = open(self.file_path, 'w')
            self.csvwriter = csv.writer(csvfile)
            self.csvwriter.writerow([
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

                row = [
                    item['Job']['Site'], item['Job']['Company'],
                    item['Job']['Company_jobs'], item['Job']['Job_id'],
                    item['Job']['Job_title'], item['Job']['Job_Description'],
                    item['Job']['Job_Post_Date'], item['Job']['Job_URL'],
                    item['Job']['Country_Areas'],
                    item['Job']['Job_categories'],
                    item['Job']['AllJobs_Job_class'], crawl_date_str,
                    item['Job']['unique_id']
                ]
                row = [s.encode('utf-8') for s in row]
                self.csvwriter.writerow(row)
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
            body = "Please find the attachment for {}".format(
                self.file_name)
            send_email(
                directory=self.directory, file_name=self.file_name,
                body=body)

            drushim_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                self.directory, self.today_str, 'Drushim')
            alljobs_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                self.directory, self.today_str, 'Alljobs')
            jobmaster_file = '{}/{}_{}_data_transfer_complete.xls'.format(
                self.directory, self.today_str, 'Jobmaster')

            """ check if all crawled complete for all sites excel file for 3
            sites exists and proceed creating client changes xls"""
            if (
                os.path.isfile(drushim_file) and
                os.path.isfile(jobmaster_file) and
                os.path.isfile(alljobs_file)
            ):
                # prepare clientchanges report
                clientchanges.ClientChanges()

                os.remove(drushim_file)
                os.remove(alljobs_file)
                os.remove(jobmaster_file)

    def handle_error(self, failure, item, spider):
        """Handle occurred on dbSchema interaction."""
        self.logger.info("DB Schema Handled")
