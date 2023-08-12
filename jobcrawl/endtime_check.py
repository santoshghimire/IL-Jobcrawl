import datetime

def reached_endtime(self):
    now = datetime.datetime.utcnow()
    endtime = now.replace(hour=12, minute=30, second=0, microsecond=0)
    return now > endtime
