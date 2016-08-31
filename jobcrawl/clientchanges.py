import datetime
import pymysql
import os
import sys
import locale
import codecs
import pandas as pd
from jobcrawl import settings


class ClientChanges:

    def __init__(self):
        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.today = datetime.date.today()
        self.today_str = self.today.strftime("%d/%m/%Y")

        self.yesterday = self.today - datetime.timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%d/%m/%Y")

        # """ For testing purpose will """
        # self.today_str = "30/08/2016"
        # self.yesterday_str = "29/08/2016"

        self.excel_file_path = self.create_file()
        self.df_main = self.read_sql()
        self.excel_writer()

    def create_file(self):
        """ Create directory and file for client changes and return excel file path"""
        directory_name = "daily_competitor_client_changes"
        if not os.path.exists(directory_name):
            os.mkdir(directory_name)

        filename = "{}_{}.xls".format(self.today.strftime("%Y_%m_%d"), directory_name)
        excel_file_path = "{}/{}".format(directory_name, filename)
        return excel_file_path

    def read_sql(self):
        """ Read sql query (database table)  and return pandas dataframe"""

        conn = pymysql.connect(host=settings.MYSQL_HOST, port=3306, user=settings.MYSQL_USER,
                               passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_DBNAME,
                               charset='utf8')
        sql = """SELECT Site,Company, Company_jobs,Crawl_Date,count(*) as Num_Company_jobs
                             FROM sites_datas
                             WHERE Crawl_Date in (%(today)s,%(yesterday)s)
                             GROUP BY Company,Site,Company_jobs,Crawl_Date"""
        df_main = pd.read_sql(sql, conn, params={
            'today': self.today_str, 'yesterday': self.yesterday_str})

        return df_main

    def excel_writer(self):
        """"write to excel file using pandas """
        writer = pd.ExcelWriter(self.excel_file_path)
        columns = ['Site', 'Company', 'Company_jobs', 'Num_Company_jobs']
        try:
            df_copy = self.df_main.drop_duplicates(['Company'], keep=False)
            try:
                yesterdays_jobs = df_copy.Crawl_Date == self.yesterday_str
                df_removed_companies = df_copy[yesterdays_jobs]
                df_removed_companies = df_removed_companies.sort_values(by=['Site', 'Company'])
                df_removed_companies.to_excel(writer, index=False, sheet_name='Companies_That_left', columns=columns, encoding='utf-8')
            except:
                pass
            try:
                todays_jobs = df_copy.Crawl_Date == self.today_str
                df_new_companies = df_copy[todays_jobs]
                df_new_companies = df_new_companies.sort_values(by=['Site', 'Company'])
                df_new_companies.to_excel(writer, index=False, sheet_name='New_Company',columns=columns, encoding='utf-8')
            except:
                pass
        except:
            pass

        writer.save()

#
#
# if __name__ =='__main__':
#     ClientChanges()

