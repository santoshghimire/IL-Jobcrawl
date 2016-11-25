import datetime
import pymysql
import os
import sys
import locale
import codecs
import pandas as pd

settings = {
    'MYSQL_HOST': 'localhost',
    'MYSQL_DBNAME': 'il_sites_datas',
    'MYSQL_USER': 'root',
    'MYSQL_PASSWORD': 'root'
}


class ClientChanges:

    def __init__(self):
        sys.stdout = codecs.getwriter(
            locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.today = datetime.date.today()

        """ For testing purpose will """
        # self.today_str = "05/10/2016"
        # self.today = datetime.datetime.strptime(self.today_str, "%d/%m/%Y")
        """ End Testing """

        self.today_str = self.today.strftime("%d/%m/%Y")
        self.today_file = self.today.strftime("%Y_%m_%d")

        self.yesterday = self.today - datetime.timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%d/%m/%Y")

        # Generate range of dates for a week
        self.date_range = []
        for i in range(8):
            item = (self.today - datetime.timedelta(days=i)).strftime(
                "%d/%m/%Y")
            item = '"' + item + '"'
            self.date_range.append(item)

        self.excel_file_path = self.create_file()

    def start(self):
        self.df_main = self.read_sql()
        self.excel_writer()

    def get_stats(self):
        new_removed_stats = self.get_removed_stats()
        total_stats = self.get_total_stats()
        new_removed_stats.update(total_stats)
        return new_removed_stats

    def create_file(self):
        """ Create directory and file for client changes
        and return excel file path"""
        directory_name = "daily_competitor_client_changes"
        if not os.path.exists(directory_name):
            os.mkdir(directory_name)

        filename = "{}_Daily-Competitor-Client-Change.xlsx".format(
            self.today_file)
        excel_file_path = "./{}/{}".format(directory_name, filename)
        return excel_file_path

    def read_sql(self):
        """ Read sql query (database table)  and return pandas dataframe"""

        conn = pymysql.connect(
            host=settings.get('MYSQL_HOST'), port=3306,
            user=settings.get('MYSQL_USER'),
            passwd=settings.get('MYSQL_PASSWORD'),
            db=settings.get('MYSQL_DBNAME'),
            charset='utf8'
        )
        format_strings = ','.join(['%s'] * len(self.date_range))

        sql = """SELECT Site,Company, Company_jobs,Crawl_Date,Job_Post_Date,unique_id
            FROM sites_datas
            WHERE Crawl_Date IN (%s)""" % format_strings
        sql = sql % tuple(self.date_range)
        df_main = pd.read_sql(
            sql, conn
        )
        return df_main

    def get_total_stats(self):
        """ Read sql query (database table)  and return pandas dataframe"""

        conn = pymysql.connect(
            host=settings.get('MYSQL_HOST'), port=3306,
            user=settings.get('MYSQL_USER'),
            passwd=settings.get('MYSQL_PASSWORD'),
            db=settings.get('MYSQL_DBNAME'),
            charset='utf8'
        )
        data = {'total_jobs': {}, 'total_companies': {}}
        for company in ["Drushim", "AllJobs", "JobMaster"]:
            sql = """select count(*) as count from sites_datas where
                Site= "%s" and
                Crawl_Date= "%s";""" % (company, self.today_str)
            result = pd.read_sql(
                sql, conn
            )
            data['total_jobs'][company] = result['count'][0]

            sql_company = """select count(Distinct(Company)) as count from sites_datas
            where Site="%s" and Crawl_Date="%s";""" % (company, self.today_str)
            result_company = pd.read_sql(
                sql_company, conn
            )
            data['total_companies'][company] = result_company['count'][0]

        return data

    def excel_writer(self):
        """"write to excel file using pandas """
        # Remove duplicates
        writer = pd.ExcelWriter(self.excel_file_path)
        columns = ['Site', 'Company', 'Company_jobs', 'Num_Company_jobs']
        new_columns = [
            'Site', 'Company', 'Company_jobs', 'Num_Company_jobs',
            'Company Site URL', 'Company Phone', 'Company Email']

        # Groupby site,company and crawl_date and transform total number
        # of jobs on particualr crawl
        # date for the each company of the each sites
        self.df_main['Num_Company_jobs'] = self.df_main.groupby(
            ['Site', 'Company', 'Crawl_Date']
        )['unique_id'].transform('count')

        # Drop duplicates companies for each site on each crawl date
        self.df_main = self.df_main.drop_duplicates(
            subset=['Site', 'Company', 'Crawl_Date'])

        # ****** GET NEW COMPANIES **********
        # ***********************************
        df_new = self.df_main.copy()
        df_new['crawl_date_count'] = df_new.groupby(
            ['Site', 'Company']
        )['unique_id'].transform('count')
        df_new = df_new[df_new.crawl_date_count == 1]

        df_new_companies = df_new[
            df_new.Crawl_Date == self.today_str]
        df_new_companies = df_new_companies.sort_values(
            by=['Site', 'Company'])

        df_new_companies['Company Site URL'] = ""
        df_new_companies['Company Phone'] = ""
        df_new_companies['Company Email'] = ""
        df_new_companies.to_excel(
            writer, index=False, sheet_name='New_Companies',
            columns=new_columns, encoding='utf-8')

        # ****** GET REMOVED COMPANIES ******
        # ***********************************
        # Get yesterday and today only data
        df_removed = self.df_main.copy()
        df_removed = df_removed[
            (df_removed['Crawl_Date'] == self.yesterday_str) |
            (df_removed['Crawl_Date'] == self.today_str)
        ]
        # get number of crawl date
        df_removed['crawl_date_count'] = df_removed.groupby(
            ['Site', 'Company']
        )['unique_id'].transform('count')
        df_removed = df_removed[df_removed.crawl_date_count == 1]
        df_removed_companies = df_removed[
            df_removed.Crawl_Date == self.yesterday_str]
        df_removed_companies = df_removed_companies.sort_values(
            by=['Site', 'Company'])

        df_removed_companies.to_excel(
            writer, index=False, sheet_name='Companies_That_left',
            columns=columns, encoding='utf-8')

        # save the excel
        writer.save()

    def get_removed_stats(self):
        stats = {'new': {}, 'removed': {}}
        df_new_companies = pd.read_excel(
            self.excel_file_path, sheetname='New_Companies')
        df_removed_companies = pd.read_excel(
            self.excel_file_path, sheetname='Companies_That_left')
        for company in ["Drushim", "AllJobs", "JobMaster"]:
            stats['new'][company] = len(
                df_new_companies[df_new_companies['Site'] == company])
            stats['removed'][company] = len(
                df_removed_companies[df_removed_companies['Site'] == company])
        return stats

    def clean_residual_database(self, month_range):
        format_strings = ','.join(['"%s"'] * len(month_range))
        conn = pymysql.connect(
            host=settings.get('MYSQL_HOST'), port=3306,
            user=settings.get('MYSQL_USER'),
            passwd=settings.get('MYSQL_PASSWORD'),
            db=settings.get('MYSQL_DBNAME'),
            charset='utf8'
        )
        sql = """DELETE FROM sites_datas WHERE Crawl_Date NOT IN (%s)""" % format_strings
        sql = sql % tuple(month_range)
        try:
            with conn.cursor() as cursor:
                # Create a new record
                cursor.execute(sql)
            # connection is not autocommit by default.
            # So you must commit to save your changes.
            conn.commit()
        finally:
            conn.close()


if __name__ == '__main__':
    c = ClientChanges()
    # c.start()
    # c.get_stats()
    month_range = []
    for i in range(70):
        item = (c.today - datetime.timedelta(days=i)).strftime("%d/%m/%Y")
        month_range.append(item)
    # print(month_range)
    c.clean_residual_database(month_range)
    print('success')
