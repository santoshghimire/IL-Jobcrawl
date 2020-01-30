import warnings
warnings.filterwarnings("ignore")


DATE_FMT = "%d/%m/%Y"


def parse_dates(sd, ed):
    if not ed:
        ed = datetime.today().strftime(DATE_FMT)
    return datetime.strptime(sd, DATE_FMT), datetime.strptime(ed, DATE_FMT)


def main(site, start_date, end_date):
    start_date, end_date = parse_dates(start_date, end_date)
    if start_date > end_date:
        print("Start date is greater than end date")
        return
    print("\nGetting data from {} to {}\n".format(
        start_date.strftime(DATE_FMT), end_date.strftime(DATE_FMT)))
    conn = pymysql.connect(
        host=settings.MYSQL_HOST, port=3306, user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_DBNAME,
        charset='utf8'
    )

    df_all = []
    current_date = None
    while True:
        if current_date is None:
            current_date = start_date - timedelta(days=1)
        current_date_str = current_date.strftime(DATE_FMT)
        sql = """SELECT distinct(Company) FROM sites_datas
            WHERE Site='%s' and Crawl_Date='%s'""" % (site, current_date_str)
        data_df = pd.read_sql(sql, conn)
        print("Date: {}, Unique company size = {}".format(
            current_date_str, data_df.shape[0]))
        df_all.append((current_date_str, data_df))

        if current_date >= end_date:
            break

        current_date += timedelta(days=1)
    print("\nTotal df retrieved = {}".format(len(df_all)))
    print("Dates of all dfs = {}\n".format([i[0] for i in df_all]))
    yest_df = None
    new_companies = pd.DataFrame.from_dict({'Company': [], 'Report Date': []})
    removed_companies = pd.DataFrame.from_dict({'Company': [], 'Report Date': []})
    for date_str, df in df_all:
        if yest_df is None:
            yest_df = df
            continue
        yest_list = yest_df['Company'].tolist()
        # if None in yest_list:
        #     yest_list.remove(None)
        today_list = df['Company'].tolist()
        # if None in today_list:
        #     today_list.remove(None)

        new = list(set(today_list) - set(yest_list))
        removed = list(set(yest_list) - set(today_list))

        new_temp = pd.DataFrame.from_dict({'Company': new,
            'Report Date': [date_str] * len(new)})
        removed_temp = pd.DataFrame.from_dict({'Company': removed,
            'Report Date': [date_str] * len(removed)})

        print("Report: Date {}: New={}, Removed={}".format(
            date_str, new_temp.shape[0], removed_temp.shape[0]))

        new_companies = new_companies.append(new_temp, ignore_index=True)
        removed_companies = removed_companies.append(removed_temp, ignore_index=True)
        print("Combined Report: Date {}: New={}, Removed={}".format(
            date_str, new_companies.shape[0], removed_companies.shape[0]))

        yest_df = df

    prefix = "{}_to_{}".format(
        start_date.strftime("%d-%m-%y"), end_date.strftime("%d-%m-%y"))
    new_companies.to_csv("{}_{}".format(prefix, "new_company_report_dump.csv"),
        index=False, encoding='utf-8')
    removed_companies.to_csv("{}_{}".format(prefix, "removed_company_report_dump.csv"),
        index=False, encoding='utf-8')

    total_new = new_companies['Company'].tolist()
    total_removed = removed_companies['Company'].tolist()
    total_new_distinct = set(total_new)
    total_removed_distinct = set(total_removed)
    print("Distinct companies in New companies report = {}".format(
        len(total_new_distinct)))
    print("Distinct companies in Removed companies report = {}".format(
        len(total_removed_distinct)))

    print("\nDone")

if __name__ == '__main__':
    import argparse
    from datetime import datetime, timedelta
    import pymysql
    import pandas as pd
    from jobcrawl import settings

    parser = argparse.ArgumentParser(description='Dump Client Changes')
    parser.add_argument('-s', '--site', help="Site", required=True)
    parser.add_argument('-sd', '--start_date', help="Start Date (dd/mm/yyyy)",
                                                            required=True)
    parser.add_argument('-ed', '--end_date', help="End Date (dd/mm/yyyy)",
                                                        required=False)
    args = parser.parse_args()
    main(args.site, args.start_date, args.end_date)
