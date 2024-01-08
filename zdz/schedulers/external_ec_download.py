import datetime
import os

from django.db import connection

from tzdemo.settings import base
from zdz.common.utils import db_utils

from zdz.common.utils import data_class

if not os.environ.get("DJANGO_SETTINGS_MODULE"):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                          "tzdemo.settings.%s" % base.ENV)


def ec_download():
    # 开始 --- 每小时运行
    # ec_worker = data_class.ec_data_upload()
    # ec_worker.conuty_data()
    ftp = data_class.FTP_Ec()
    ftp.download_file()
    ftp.del_file()
    # 智能网格
    ftp2 = data_class.PredictGrid()
    ftp2.download_file()
    ftp2.del_file()
    # 短信和数据同步
    msger = data_class.message()
    msger.send_msg()
    # 同步每日数据
    preday = data_class.station_days()
    preday.data2mysql()
    

    
if __name__ == '__main__':
    ec_upload()
