import datetime
import pymysql
import os
import sys
import locale
import codecs
import pandas as pd
from jobcrawl import settings

settings = {
    'MYSQL_HOST':'localhost',
    'MYSQL_DBNAME':'il_sites_datas',
    'MYSQL_USER':'root',
    'MYSQL_PASSWORD':'root'
}

class ClientChanges:


    def __init__(self):
        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.today = datetime.date.today()
        self.today_str = self.today.strftime("%d/%m/%Y")

        self.yesterday = self.today - datetime.timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%d/%m/%Y")

        self.one_week = self.today - datetime.timedelta(days=7)
        self.one_week_str = self.one_week.strftime("%d/%m/%Y")

        """ For testing purpose will """
        self.today_str = "21/09/2016"
        self.yesterday_str = "20/09/2016"
        self.one_week_str = "15/09/2016"

        self.excel_file_path = self.create_file()
        self.df_main = self.read_sql()
        self.excel_writer()

    def create_file(self):
        """ Create directory and file for client changes and return excel file path"""
        directory_name = "daily_competitor_client_changes"
        if not os.path.exists(directory_name):
            os.mkdir(directory_name)

        filename = "{}_Daily-Competitor-Client-Change.xlsx".format(self.today.strftime("%Y_%m_%d"))
        excel_file_path = "./{}/{}".format(directory_name, filename)
        return excel_file_path

    def read_sql(self):
        """ Read sql query (database table)  and return pandas dataframe"""

        conn = pymysql.connect(host=settings.MYSQL_HOST, port=3306, user=settings.MYSQL_USER,
                               passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_DBNAME,
                               charset='utf8')

        sql = """SELECT Site,Company, Company_jobs,Crawl_Date,Job_Post_Date,unique_id
                                     FROM sites_datas
                                     WHERE Crawl_Date BETWEEN %(one_week)s AND %(today)s
            """
        df_main = pd.read_sql(sql, conn, params={
             'one_week': self.one_week_str, 'today': self.today_str})

        return df_main

    def excel_writer(self):
        """"write to excel file using pandas """
        writer = pd.ExcelWriter(self.excel_file_path)
        columns = ['Site', 'Company', 'Company_jobs', 'Num_Company_jobs']
        try:
            df_main = self.df_main
            """Groupby site,company and crawl_date and transform total number of jobs on particualr crawl
            date for the each company of the each sites"""

            df_main['Num_Company_jobs'] = df_main.groupby(['Site', 'Company', 'Crawl_Date'])['unique_id'].transform(
                'count')

            """ Drop duplicates companies for each site on each crawl date"""

            df_main = df_main.drop_duplicates(subset=['Site', 'Company', 'Crawl_Date'])

            """ count the number of Crawl_Date for each Sites's Company and transform value to Site's Company
                Same as droping duplicates"""
            df_main['crawl_date_count'] = df_main.groupby(['Site', 'Company'])['unique_id'].transform('count')

            """If crawl_date_count is 2 that's mean it was crawled yesterday and today so we are not intrested on this

                we are only instrested in crawl date count  equal to 1
            """

            df_main = df_main[df_main.crawl_date_count == 1]

            df_new_companies = df_main[df_main.Crawl_Date == self.today_str]
            df_removed_companies = df_main[df_main.Crawl_Date == self.yesterday_str]

            df_new_companies = df_new_companies.sort_values(by=['Site', 'Company'])
            df_new_companies.to_excel(writer, index=False, sheet_name='New_Company', columns=columns,
                                      encoding='utf-8')

            df_removed_companies = df_removed_companies.sort_values(by=['Site', 'Company'])
            df_removed_companies.to_excel(writer, index=False, sheet_name='Companies_That_left', columns=columns, encoding='utf-8')

        except:
            pass

        writer.save()

#
#
# if __name__ =='__main__':
#     ClientChanges()
