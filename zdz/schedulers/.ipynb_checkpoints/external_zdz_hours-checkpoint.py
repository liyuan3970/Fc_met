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
import pymysql
import pymssql 
import numpy as np
import pandas as pd
import pickle
import redis
import datetime as dtt
import json
from cma.music.DataQueryClient import DataQueryClient

class rain_hours():
    def __init__(self):
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
    def to_redis(self,name,data):
        self.rs.set(name, pickle.dumps(data))
        rs = redis.Redis(host='10.137.13.151', port=16379,password="tzqxj58660")
        rs.set(name, pickle.dumps(data))
    def rain_sum(self,delay):
        delay_time = delay+8
        linux = dtt.datetime.now()
        offset = dtt.timedelta(minutes=(-60*8))
        start_delay =  dtt.timedelta(minutes=(-60*delay_time))
        now = (linux+offset).strftime('%Y%m%d%H%M')+"00"
        old = (linux+start_delay).strftime('%Y%m%d%H%M')+"00"
        label = "["+old+","+now+"]"
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "statSurfEleInRect"
        params = {
            'dataCode':"SURF_CHN_PRE_MIN",  #SURF_CHN_MUL_HOR
            'elements':"Cnty,Province,Town,Station_levl,Station_Name,City,Station_Id_C,Lat,Lon,Alti",
            'statEles':'SUM_PRE',
            'timeRange':label,
            'minLat':"25",
            'minLon':"115",
            'maxLat':"35",
            'maxLon':"125",
            "statEleValueRanges":"SUM_PRE:(,10000)",
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns =['Cnty','Province','Town','Station_levl','Station_Name','City','Station_Id_C','Lat','Lon','Alti','PRE']
#         clomns = params['elements'].split(",")
        data = pd.DataFrame(result_json['DS'])
        data.columns = clomns
        data = data.astype({'Lat': 'float', 'Lon': 'float','Station_levl':'int','PRE': 'float','Alti':'float'})
        return data
    def rain_max_2sql(self):
        rain =  pickle.loads(self.rs.get("rain1"))
        tz_rain = rain[rain['City']=="台州市"]
        time = dtt.datetime.utcnow().strftime('%Y-%m-%d %H:%M')+":00"
        sql_data = tz_rain[['Station_Id_C','Cnty','City','Province','Station_levl','Station_Name','Town','Alti','Lat','Lon','PRE']]
        sql_data['Datetime'] = time
        datalist = sql_data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into taizhou_rain_max 
        (Station_Id_C,Cnty,City,Province,Station_levl,Station_Name,Town,Alti,Lat,Lon,PRE,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
        PRE = values(PRE),
        Station_Id_C = values(Station_Id_C),
        Cnty = values(Cnty),
        City = values(City),
        Province = values(Province),
        Station_levl = values(Station_levl),
        Station_Name = values(Station_Name),
        Town = values(Town),
        Alti = values(Alti),
        Lat = values(Lat),
        Lon = values(Lon),
        Datetime = values(Datetime)"""
        cursor.executemany(insql,sql_list)
        cursor.close()
        cursor = conn.cursor()
        conn.commit()
        conn.close()
        now = dtt.datetime.now()   
        offset = dtt.timedelta(days=-800)
        del_day = (now + offset).strftime('%Y-%m-%d %H:%M')+":00"
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        sql = """DELETE FROM taizhou_rain_max where Datetime <= '{del_day}'"""
        rsql = sql.format(del_day=del_day)
        cursor.execute(rsql)
        cursor.close()
        conn.commit()
        conn.close()
    def daily_comput(self):
        single = []
        # 每天7点定时运行并保存 --处理时间
        now = dtt.datetime.utcnow()
        end = dtt.datetime.utcnow().strftime('%Y-%m-%d %H:')+"00:00"
        start = (now+dtt.timedelta(days=-1)).strftime('%Y-%m-%d %H:')+"00:00"
        date_start = dtt.datetime.strptime(start,'%Y-%m-%d %H:%M:%S') 
        date_end = dtt.datetime.strptime(end,'%Y-%m-%d %H:%M:%S') 
        label_start = date_start.strftime('%Y%m%d%H%M') + "00"
        label_end = date_end.strftime('%Y%m%d%H%M') + "00"   
        labels = "[" + label_start + "," + label_end + "]"
        # 风力
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(wind) as wind
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and City="台州市"
            group by Station_Id_C""" 
        rsql = sql.format(start_times=start,end_times=end)
        data = pd.read_sql(rsql, con=self.conn)
        data = data[data['wind']>0]
        wind = max(data['wind'])
        #data['WIN_S_Gust_Max'] = data.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
        #data['WIN_D_Gust_Max'] = data.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
        #data['value'] = data['WIN_S_Gust_Max'] 
        data = data[['Station_Id_C','Province','City','Cnty','Town','Lat','Lon','Alti','Station_levl','Station_Name','wind']]
        data['Datetime'] = end
        datalist = data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into taizhou_daily 
        (Station_Id_C,Province,City,Cnty,Town,Lat,Lon,Alti,Station_levl,Station_Name,wind,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
        wind = values(wind),
        Station_Id_C = values(Station_Id_C),
        Cnty = values(Cnty),
        City = values(City),
        Province = values(Province),
        Station_levl = values(Station_levl),
        Station_Name = values(Station_Name),
        Town = values(Town),
        Alti = values(Alti),
        Lat = values(Lat),
        Lon = values(Lon),
        Datetime = values(Datetime)"""
        cursor.executemany(insql,sql_list)
        cursor.close()
        cursor = conn.cursor()
        conn.commit()
        conn.close() 
        # 降水、高温、低温、能见度
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "statSurfEleInRegion"  
        params = {
            'dataCode':"SURF_CHN_MUL_MIN",  #SURF_CHN_MUL_HOR
            'adminCodes':"331000",
            'elements':"Station_Id_C",
            'timeRange':labels,
            'statEles':'MAX_Province,MAX_City,MAX_Cnty,MAX_Town,MAX_Lat,MAX_Lon,MAX_Alti,MAX_Station_levl,MAX_Station_Name,MAX_TEM,MIN_TEM,SUM_PRE,MIN_VIS_HOR_1MI',
            #'orderBy':"MAX_WIN_S_Inst_Max:desc",
            'staLevels':"011,012,013,014,015,016", # 12国家站 14区域站
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = "Station_Id_C,MAX_Province,MAX_City,MAX_Cnty,MAX_Town,MAX_Lat,MAX_Lon,MAX_Alti,MAX_Station_levl,MAX_Station_Name,MAX_TEM,MIN_TEM,SUM_PRE,MIN_VIS_HOR_1MI".split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        data.columns = "Station_Id_C,Province,City,Cnty,Town,Lat,Lon,Alti,Station_levl,Station_Name,tmax,tmin,rain,view".split(",") 
        data['Datetime'] = end
        datalist = data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into taizhou_daily 
        (Station_Id_C,Province,City,Cnty,Town,Lat,Lon,Alti,Station_levl,Station_Name,tmax,tmin,rain,view,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
        rain = values(rain),
        tmax = values(tmax),
        tmin = values(tmin),
        view = values(view),
        Station_Id_C = values(Station_Id_C),
        Cnty = values(Cnty),
        City = values(City),
        Province = values(Province),
        Station_levl = values(Station_levl),
        Station_Name = values(Station_Name),
        Town = values(Town),
        Alti = values(Alti),
        Lat = values(Lat),
        Lon = values(Lon),
        Datetime = values(Datetime)"""
        cursor.executemany(insql,sql_list)
        cursor.close()
        cursor = conn.cursor()
        conn.commit()
        conn.close()
        data = data.astype({'tmax': 'float', 'tmin': 'float', 'rain': 'float', 'view': 'float'})
        rain = data[data['rain']<5000]['rain'].max()
        tmax = data[data['tmax']<5000]['tmax'].max()
        tmin = data[data['tmin']<5000]['tmin'].min()
        view = data[data['view']<30000]['view'].min()
        # 小时雨强 
        sql = """select Station_Id_C,max(PRE) as rain_max
            from taizhou_rain_max 
            where Datetime between '{start_times}' and '{end_times}'
            group by Station_Id_C""" 
        rsql = sql.format(start_times=start,end_times=end)
        data = pd.read_sql(rsql, con=self.conn)
        data['Datetime'] = end
        datalist = data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into taizhou_daily 
        (Station_Id_C,rain_max,Datetime) 
        values(%s,%s,%s)
        on duplicate key update
        rain_max = values(rain_max),
        Station_Id_C = values(Station_Id_C),
        Datetime = values(Datetime)"""
        cursor.executemany(insql,sql_list)
        cursor.close()
        cursor = conn.cursor()
        conn.commit()
        conn.close()
        #日历表
        single = [end[0:10],wind,tmax,tmin,rain,view]
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into taizhou_clander 
        (Datetime,wind,tmax,tmin,rain,view) 
        values(%s,%s,%s,%s,%s,%s)
        on duplicate key update
        Datetime = values(Datetime),
        wind = values(wind),
        tmax = values(tmax),
        tmin = values(tmin),
        rain = values(rain),
        view = values(view)
        """
        cursor.executemany(insql,[single])
        cursor.close()
        cursor = conn.cursor()
        conn.commit()
        conn.close()


def zdz_tz_rainmax():
    worker = rain_hours()
    worker.rain_max_2sql()

def zdz_tz_daily():
    worker = rain_hours()
    worker.daily_comput()
    

def zdz_rain_24():
    worker = rain_hours()
    delay = 24
    data = worker.rain_sum(delay)   
    name = "rain24"
    worker.to_redis(name,data)

def zdz_rain_12():
    worker = rain_hours()
    delay = 12
    data = worker.rain_sum(delay)   
    name = "rain12"
    worker.to_redis(name,data)

def zdz_rain_6():
    worker = rain_hours()
    delay = 6
    data = worker.rain_sum(delay)   
    name = "rain6"
    worker.to_redis(name,data)

def zdz_rain_3():
    worker = rain_hours()
    delay = 3
    data = worker.rain_sum(delay)   
    name = "rain3"
    worker.to_redis(name,data)

def zdz_rain_1():
    worker = rain_hours()
    delay = 1
    data = worker.rain_sum(delay)   
    name = "rain1"
    worker.to_redis(name,data)

if __name__ == '__main__':
    zdz_minutes()
    zdz_decode_time()

