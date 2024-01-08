import datetime
import os
import datetime as dtt
from django.db import connection

from tzdemo.settings import base
from zdz.common.utils import db_utils

from zdz.common.utils import data_class

if not os.environ.get("DJANGO_SETTINGS_MODULE"):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                          "tzdemo.settings.%s" % base.ENV)


def zdz_minutes():
    # 开始
    worker = data_class.data2sql()
    worker.to_mysql()
    

def zdz_del():
    # 删除数据
    worker = data_class.data2sql()
    worker.del_mysql()
    worker.del_mysql_hour()


def zdz_hour_all():
    now = dtt.datetime.now()  
    offset = dtt.timedelta(minutes=(-60*8))
    start = (now+offset)
    start_times = start.strftime('%Y-%m-%d %H')+":00:00"
    end_times = start.strftime('%Y-%m-%d %H')+":59:00"
    worker = data_class.data2sql()
    worker.sql_rain(start_times,end_times)
    worker.sql_wind(start_times,end_times)
    worker.sql_temp(start_times,end_times)
    worker.sql_view(start_times,end_times)

def zdz_hour_int():
    now = dtt.datetime.now()  
    if now.minute<3:
        offset = dtt.timedelta(minutes=(-60*8-10))
        start = (now+offset)
        start_times = start.strftime('%Y-%m-%d %H')+":00:00"
        end_times = start.strftime('%Y-%m-%d %H')+":59:00"
        worker = data_class.data2sql()
        worker.sql_rain(start_times,end_times)
        worker.sql_wind(start_times,end_times)
        worker.sql_temp(start_times,end_times)
        worker.sql_view(start_times,end_times)


if __name__ == '__main__':
    zdz_minutes()
    zdz_decode_time()

