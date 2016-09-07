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

        """ For testing purpose will """
        # self.today_str = "06/09/2016"
        # self.yesterday_str = "05/09/2016"

        self.excel_file_path = self.create_file()
        self.df_main = self.read_sql()
        self.excel_writer()

    def create_file(self):
        """ Create directory and file for client changes and return excel file path"""
        directory_name = "daily_competitor_client_changes"
        if not os.path.exists(directory_name):
            os.mkdir(directory_name)

        filename = "{}_Daily-Competitor-Client-Change.xls".format(self.today.strftime("%Y_%m_%d"))
        excel_file_path = "{}/{}".format(directory_name, filename)
        return excel_file_path

    def read_sql(self):
        """ Read sql query (database table)  and return pandas dataframe"""

        conn = pymysql.connect(host=settings.MYSQL_HOST, port=3306, user=settings.MYSQL_USER,
                               passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_DBNAME,
                               charset='utf8')

        sql = """SELECT Site,Company, Company_jobs,Crawl_Date,Job_Post_Date,unique_id
                                     FROM sites_datas
                                     WHERE Crawl_Date in (%(today)s,%(yesterday)s)
            """
        df_main = pd.read_sql(sql, conn, params={
            'today': self.today_str, 'yesterday': self.yesterday_str})

        return df_main

    def excel_writer(self):
        """"write to excel file using pandas """
        writer = pd.ExcelWriter(self.excel_file_path)
        columns = ['Site', 'Company', 'Company_jobs', 'Num_Company_jobs']
        try:
            df_main = self.df_main
            """ Count all the occurence of Company"""
            df_main['Num_Company_jobs'] = df_main.groupby('Company')['Company'].transform('count')

            """ Crawled Yesterday"""
            yesterday_df = df_main[df_main['Crawl_Date'] == self.yesterday_str]
            yesterday_df = yesterday_df.drop_duplicates('Company')  # drop duplicates company from yesterday and keep 1

            """ Crawled Today"""
            today_df = df_main[df_main['Crawl_Date'] == self.today_str]
            today_df = today_df.drop_duplicates('Company')  # drop duplicates company and just keep one

            """ Merge both crawled today and crawled yesterday """
            df_merge = pd.concat([today_df, yesterday_df])
            """ Again drop duplicates companies and keep none of them
            if they are present in crawled today and crawled yesterday"""
            df_merge = df_merge.drop_duplicates(['Company'], keep=False)

            """ After Droping duplicates  remaining companys are either crawled yesterday or crawled today"""

            """ Crawled today which were not in crawled yesterday are new companies"""
            df_new_companies = df_merge[df_merge['Crawl_Date'] == self.today_str]
            df_new_companies = df_new_companies.sort_values(by=['Site', 'Company'])
            df_new_companies.to_excel(writer, index=False, sheet_name='New_Company', columns=columns,
                                      encoding='utf-8')

            """Crawled yesterday but not present in crawled today are removed companies"""
            df_removed_companies = df_merge[df_merge['Crawl_Date'] == self.yesterday_str]
            df_removed_companies = df_removed_companies.sort_values(by=['Site', 'Company'])
            df_removed_companies.to_excel(writer, index=False, sheet_name='Companies_That_left', columns=columns, encoding='utf-8')

        except:
            pass

        writer.save()

#
#
# if __name__ =='__main__':
#     ClientChanges()
