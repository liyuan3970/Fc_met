import datetime
import os

from django.db import connection

from tzdemo.settings import base
from zdz.common.utils import db_utils

from zdz.common.utils import data_class

if not os.environ.get("DJANGO_SETTINGS_MODULE"):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                          "tzdemo.settings.%s" % base.ENV)


def radar_ftp():
    # 开始
    worker = data_class.FTP_Radar()
    worker.download_file()
    worker.del_file()



if __name__ == '__main__':
    zdz_minutes()
    zdz_decode_time()

