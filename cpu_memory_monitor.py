import csv
import os
import time
import signal
import smtplib
import logging
import psutil
import pandas as pd
import matplotlib.dates as md
import matplotlib.pyplot as plt
from datetime import datetime


THRESHOLD = 40
CPU_MEMORY_MONITOR_DIR_NAME = 'cpu_memory_monitor'
END_TIME = datetime.utcnow().replace(hour=12, minute=50, second=0, microsecond=0)


def plot(filename):
    dataframe = pd.read_csv(filename)
    dataframe['time'] = pd.to_datetime(dataframe['time'])
    fig, ax = plt.subplots()
    ax.plot(dataframe['time'], dataframe['cpu'], label='cpu')
    ax.plot(dataframe['time'], dataframe['memory'], label='memory')
    xfmt = md.DateFormatter('%H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    # fig.autofmt_xdate()
    today_str = datetime.today().strftime("%Y-%m-%d")
    plt.title("CPU and Memory {}".format(today_str))
    plt.xlabel("Time")
    plt.ylabel("CPU/Memory")
    try:
        ylim_max = max(max(dataframe['cpu']), max(dataframe['memory']), 50)
    except:
        ylim_max = 50
    plt.ylim(0, ylim_max)
    plt.axhline(y=THRESHOLD, linestyle='--')
    plt.legend(loc="upper left")
    chart_fname = os.path.join(CPU_MEMORY_MONITOR_DIR_NAME, "cpu_memory_chart_{}.png".format(today_str))
    plt.savefig(chart_fname)


def run():
    if not os.path.exists(CPU_MEMORY_MONITOR_DIR_NAME):
        os.makedirs(CPU_MEMORY_MONITOR_DIR_NAME)
    filename = os.path.join(CPU_MEMORY_MONITOR_DIR_NAME, 'cpu_memory_dump.csv')
    fp = open(filename, 'w')
    writer = csv.writer(fp)
    writer.writerow(['time', 'cpu', 'memory'])
    t0 = time.time()
    while True:
        now_dt = datetime.utcnow()
        if now_dt > END_TIME:
            print("Reached endtime")
            fp.close()
            plot(filename)
            break
        now = now_dt.strftime('%H:%M:%S')
        cpu_util = psutil.cpu_percent(interval=1)
        used_memory = psutil.virtual_memory().percent
        
        row = now, cpu_util, used_memory
        writer.writerow(row)
        # print(row)
        if time.time() - t0 > 10:
            fp.close()
            plot(filename)
            t0 = time.time()
            fp = open(filename, 'a')
            writer = csv.writer(fp)
        time.sleep(5)



if __name__ == '__main__':
    run()
