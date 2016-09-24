# import datetime
import pymysql
import sys
import locale
import codecs
import pandas as pd
from openpyxl import load_workbook
from openpyxl import Workbook

settings = {
    'MYSQL_HOST': 'localhost',
    'MYSQL_DBNAME': 'il_sites_datas',
    'MYSQL_USER': 'root',
    'MYSQL_PASSWORD': 'root'
}


def read_sql(site):
    """ Read sql query (database table)  and return pandas dataframe"""
    sys.stdout = codecs.getwriter(
        locale.getpreferredencoding())(sys.stdout)
    reload(sys)
    sys.setdefaultencoding('utf-8')

    file_name = '{}.xlsx'.format(site)
    main_writer = pd.ExcelWriter(
        file_name, engine='openpyxl')

    # today = datetime.date.today()
    # today_str = today.strftime("%d/%m/%Y")

    today_str = '21/09/2016'

    conn = pymysql.connect(
        host=settings.get('MYSQL_HOST'), port=3306,
        user=settings.get('MYSQL_USER'),
        passwd=settings.get('MYSQL_PASSWORD'),
        db=settings.get('MYSQL_DBNAME'),
        charset='utf8'
    )

    sql = """SELECT Site, Company, Company_jobs, Job_id, Job_title,
        Job_Description, Job_Post_Date, Job_URL, Country_Areas,
        Job_categories, AllJobs_Job_class, Crawl_Date, unique_id
        FROM sites_datas
        WHERE Site=%(site)s AND Crawl_Date =%(crawl_date)s
        Order By Company
    """
    df_main = pd.read_sql(sql, conn, params={
        'site': site, 'crawl_date': today_str})
    df_main.to_excel(main_writer, site, index=False)
    main_writer.save()
    return file_name


def main():
    # file_names = []
    # for site in ['AllJobs', 'Drushim', 'JobMaster']:
    #     file_name = read_sql(site)
    #     file_names.append(file_name)

    file_names = ['AllJobs.xlsx', 'Drushim.xlsx', 'JobMaster.xlsx']
    main_excel_file_path = 'output.xlsx'

    wb = Workbook(encoding='utf-8')
    wb.active.title = 'Drushim'
    wb.save(main_excel_file_path)

    for each_file in file_names:
        main_writer = pd.ExcelWriter(
            main_excel_file_path, engine='openpyxl')
        main_book = load_workbook(main_excel_file_path)
        main_writer.book = main_book

        main_writer.sheets = dict(
            (ws.title, ws) for ws in main_book.worksheets)

        sorted_xls = pd.read_excel(
            each_file)
        sheet_name = each_file.replace('.xlsx', '')
        sorted_xls.to_excel(main_writer, sheet_name, index=False)
        main_writer.save()

if __name__ == '__main__':
    main()
