import base64
import json
import os
from io import BytesIO
from math import isnan
import cinrad
from cinrad.visualize import Section
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

# import matplotlib.pyplot as plt
import netCDF4
import numpy as np
# import modin.pandas as pd
import pandas as pd
import shapefile
import xarray as xr
from affine import Affine
from matplotlib.patches import PathPatch
from matplotlib.path import Path
from mpl_toolkits.basemap import Basemap
from ncmaps import Cmaps
from rasterio import features
import matplotlib as mpl
from . import func

import xesmf as xe
import redis
from scipy.interpolate import griddata
from scipy.interpolate import interp1d
# import h5netcdf.legacyapi as netCDF4
from scipy.interpolate import Rbf
os.environ["HDF5_USE_FILE_LOCKING"] = 'FALSE'
import datetime as dtt
from datetime import timezone
from datetime import *
from matplotlib.colors import ListedColormap,LinearSegmentedColormap

from pylab import *
from matplotlib.font_manager import FontProperties
from matplotlib import font_manager
import pymysql
from pymysql.converters import escape_string
import pickle
import pymssql 
# 查询历史数据的calss

from astropy.convolution import convolve, Gaussian2DKernel, Tophat2DKernel
from astropy.modeling.models import Gaussian2D
## 同步分钟级数据库
from cma.music.DataQueryClient import DataQueryClient
# ftp
import geojsoncontour
from ftplib import FTP
import os
import datetime as dtt
import fnmatch
import shutil
import time


# 实况数据class

class station_zdz:
    def __init__(self):
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        #self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="051219",db="ZJSZDZDB")
        #self.conn = pymssql.connect("172.21.158.201","down","downx","ZJSZDZDB")
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
    def get_redis(self,date_type):
        '''根据date_type向redis中获取数据'''
        value = self.rs.get(date_type)
        if value:
            data = pickle.loads(value)
        else:
            data = None       
        #data = pickle.loads(self.rs.get(date_type))
        # 解析数据
        return data
# 单站数据解析 -----------     ----------     ----------         --------     ----------------------------
    def single_data(self,value,station_id,plot_range):
        # value, station_id, timesdelay 降水 站id 时间 
        timelabel = plot_range
        client = DataQueryClient(configFile = r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "getSurfEleByTimeRangeAndStaID"
        # 元素字典
        elem = {
            "rain":"Datetime,Station_Id_C,PRE,Station_Name,Q13011",# 降水
            "wind":"Datetime,Station_Id_C,WIN_S_INST,WIN_D_INST,Station_Name,Q11201,Q11202", #风力 
            "sped":"Datetime,Station_Id_C,WIN_S_INST,WIN_D_INST,Station_Name,Q11201,Q11202", #风力 
            "windave":"Station_Name,Station_Id_C,Datetime,WIN_D_Avg_1mi,WIN_S_Avg_1mi,Q11288,Q11289",
            "temp":"Datetime,Station_Id_C,TEM,Station_Name,Q12001",# 气温
            "tmax":"Datetime,Station_Id_C,TEM,Station_Name,Q12001",# 气温
            "tmin":"Datetime,Station_Id_C,TEM,Station_Name,Q12001",# 气温
            "dpt":"Station_Name,Station_Id_C,Datetime,DPT,Q12003",
            "view":"Datetime,Station_Id_C,VIS_HOR_1MI,Station_Name,Q20001_701_01",# 能见度
            "rhu":"Station_Name,Station_Id_C,Datetime,RHU,Q13003",
            "vap":"Station_Name,Station_Id_C,Datetime,VAP,Q13004",
            "Snow_Depth":"Station_Name,Station_Id_C,Datetime,Snow_Depth,Q13013",
            "prs":"Station_Name,Station_Id_C,Datetime,PRS,Q10004",
            "PRS_Sea": "Station_Name,Station_Id_C,Datetime,PRS_Sea,Q10051"        
        }
        rename = {
            "rain":"TTime,IIIII,Ri,StationName,verify",
            "temp":"TTime,IIIII,T,StationName,verify",
            "tmax":"TTime,IIIII,T,StationName,verify",
            "tmin":"TTime,IIIII,T,StationName,verify",
            "wind":"TTime,IIIII,fFy,dFy,StationName,verify1,verify2",
            "sped":"TTime,IIIII,fFy,dFy,StationName,verify1,verify2",
            "view":"TTime,IIIII,V,StationName,verify",
        }
        params = {
            'dataCode':"SURF_CHN_MUL_MIN",#SURF_CHN_MUL_HOR
            'elements':elem[value],  
            #'timeRange':"[20230118000000,20230119000000]",测试k8111的11级风
            'timeRange':timelabel,
            'staIds':station_id,
            'orderBy':"Datetime",
            'limitCnt':"50000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = elem[value].split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        data.columns = rename[value].split(",")
        return data
    def single_station(self,value,station_id,plot_time):
        if value=="sped":
            value = "wind"
        c_year = dtt.datetime.now().year
        c_month = dtt.datetime.now().month
        c_day = dtt.datetime.now().day
        ## --Disassemble--the--date
        date_list = plot_time.split(" ")[0].split("-")
        t_year = int(date_list[0])
        t_month = int(date_list[1])
        t_day = int(date_list[2])
        final = False
        if c_year == t_year and c_month == t_month and c_day == t_day:
            # 今天 --- 比较复杂
            offset = dtt.timedelta(minutes=-60*8)
            date_now = dtt.datetime.strptime(plot_time,"%Y-%m-%d %H:%M:%S")
            # 判断时间
            yesdelay = dtt.timedelta(days=-1)
            start = (date_now + yesdelay).strftime('%Y-%m-%d %H:%M:%S')
            end = plot_time
            today = end[0:10] + " 20:00:00"
            yesday = start[0:10] + " 20:00:00"
            hours = dtt.datetime.strptime(plot_time,'%Y-%m-%d %H:%M:%S').hour
            # 数据
            date_utc = (date_now + offset)
            delay = dtt.timedelta(minutes=-60*24)
            date_start = (date_utc+delay).strftime('%Y%m%d%H%M') +"00"
            date_end = date_utc.strftime('%Y%m%d%H%M') +"00"
            plot_range = "["+date_start +","+ date_end +"]"
            data = self.single_data(value,station_id,plot_range)
            # data["fFy"] = pd.to_numeric(data["fFy"])
            # data["dFy"] = pd.to_numeric(data["dFy"])
            data['TTime'] = pd.to_datetime(data['TTime'])
            data['TTime'] = data['TTime'] + dtt.timedelta(hours=8)
            if value=="wind":
                data['Year'] = data['TTime'].dt.year
                data['Month'] = data['TTime'].dt.month
                data['Day'] = data['TTime'].dt.day
                data['Hour'] = data['TTime'].dt.hour
                grouped_IIiii = data.groupby(['Day','Hour'])
                wind_time = []
                wind_dir = []
                wind_speed = []
                for i in grouped_IIiii.size().index:
                    single = grouped_IIiii.get_group(i)
                    value_wind = single[ single['fFy']== single['fFy'].max()].head(1)
                    wind_time.append(value_wind['TTime'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0])
                    wind_speed.append(value_wind['fFy'].values[0])
                    wind_dir.append(value_wind['dFy'].values[0])
            else:
                wind_time = []
                wind_dir = []
                wind_speed = [] 
            wind_data = pd.DataFrame(data={'TTime':wind_time,'dFy':wind_dir,'fFy':wind_speed})
            data['TTime'] = data['TTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            if hours>=20:
                # 实时数据
                now_data = data[data['TTime']>=today]
                now_data = now_data[now_data['TTime']<=end]      
                if value=="wind" :  
                    now_wind = wind_data[wind_data['TTime']>=today]
                    now_wind = now_wind[now_wind['TTime']<=end]
                else:
                    now_wind = now_data
                # 历史数据
                his_data = data[data['TTime']>start]
                his_data = his_data[his_data['TTime']<=today] 
                if value=="wind" : 
                    his_wind = wind_data[wind_data['TTime']>start]
                    his_wind = his_wind[his_wind['TTime']<=today]
                else:
                    his_wind = his_data                    
            else:
                # 实时数据
                now_data = data[data['TTime']>=yesday]
                now_data = now_data[now_data['TTime']<=end]
                if value=="wind" :
                    now_wind = wind_data[wind_data['TTime']>=yesday]
                    now_wind = now_wind[now_wind['TTime']<=end]
                else:
                    now_wind = now_data
                # 历史数据
                his_data = data[data['TTime']>start]
                his_data = his_data[his_data['TTime']<=yesday]
                if value=="wind" : 
                    his_wind = wind_data[wind_data['TTime']>start]
                    his_wind = his_wind[his_wind['TTime']<=yesday] 
                else:
                    his_wind = his_data
            history = his_data.to_json(orient='values',force_ascii=False)
            windhis = his_wind.to_json(orient='values',force_ascii=False)
        else:
            # 过去---进查询一个
            offset = dtt.timedelta(minutes=-60*8)
            date_now = dtt.datetime.strptime(plot_time,"%Y-%m-%d %H:%M:%S")
            date_utc = (date_now + offset)
            delay = dtt.timedelta(minutes=-60*24)
            date_start = (date_utc+delay).strftime('%Y%m%d') +"200000"
            date_end = date_utc.strftime('%Y%m%d') +"200000"
            plot_range = "["+date_start +","+ date_end +"]"
            data = self.single_data(value,station_id,plot_range)
            data['TTime'] = pd.to_datetime(data['TTime'])
            if value=="wind" :
                data['Year'] = data['TTime'].dt.year
                data['Month'] = data['TTime'].dt.month
                data['Day'] = data['TTime'].dt.day
                data['Hour'] = data['TTime'].dt.hour
                grouped_IIiii = data.groupby(['Day','Hour'])
                wind_time = []
                wind_dir = []
                wind_speed = []
                for i in grouped_IIiii.size().index:
                    single = grouped_IIiii.get_group(i)
                    value = single[ single['fFy']== single['fFy'].max()].head(1)
                    wind_time.append(value['TTime'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0])
                    wind_speed.append(value['fFy'].values[0])
                    wind_dir.append(value['dFy'].values[0])
            else:
                wind_time = []
                wind_dir = []
                wind_speed = [] 
            data['TTime'] = data['TTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            wind_data = pd.DataFrame(data={'TTime':wind_time,'dFy':wind_dir,'fFy':wind_speed})
            now_wind = wind_data
            his_wind = wind_data  
            now_data = data
            his_data = data           
            history = json.dumps("none")#his_data.to_json(orient='values',force_ascii=False)
            windhis = json.dumps("none")#his_wind.to_json(orient='values',force_ascii=False)
        nowdata = now_data.to_json(orient='values',force_ascii=False) 
        windnow = now_wind.to_json(orient='values',force_ascii=False)      
        return nowdata,history,windhis,windnow
    def sql_now(self,value_index,boundary):
        data = pickle.loads(self.rs.get("warring_zdz"))
        data = data.astype({'Lat': 'float', 'Lon': 'float','PRE': 'float','WIN_S_Inst_Max': 'float', 'WIN_D_INST_Max': 'float','TEM':'float','VIS_HOR_1MI':'float','Station_levl': 'int'})  
        lat0 = boundary[0]
        lat1 = boundary[1]
        lon0 = boundary[2]
        lon1 = boundary[3]
        data = data[(data['Lat']>lat0) & (data['Lat']<lat1)  &  (data['Lon']<lon1) & (data['Lon']>lon0)]
        if value_index==0:
            rain = data[(data['PRE']>=0)&(data['PRE']<5000)][['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','PRE']]
            rain_data = rain.groupby(['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['PRE'].sum().reset_index()
            rain_data['value'] = rain_data['PRE']
            output = rain_data      
        elif value_index==1:
            tmax = data[(data['TEM']>-50)&(data['TEM']<100)][['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','TEM']]
            tmax_data = tmax.groupby(['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['TEM'].max().reset_index()
            tmax_data['value'] = tmax_data['TEM']
            output = tmax_data
        elif value_index==2:
            tmin = data[(data['TEM']>-50)&(data['TEM']<100)][['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','TEM']]
            tmin_data = tmin.groupby(['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['TEM'].min().reset_index()
            tmin_data['value'] = tmin_data['TEM']
            output = tmin_data
        elif value_index==3:
            data['WIN_D_Gust_Max'] = data['WIN_D_INST_Max'] 
            data['WIN_S_Gust_Max'] = data['WIN_S_Inst_Max'] 
            wind = data[(data['WIN_S_Gust_Max']>0)&(data['WIN_S_Gust_Max']<5000)][['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','WIN_D_Gust_Max','WIN_S_Gust_Max']]
            wind_data = wind.groupby(['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','WIN_D_Gust_Max'])['WIN_S_Gust_Max'].max().reset_index().sort_values('WIN_S_Gust_Max', ascending=False).drop_duplicates(subset=['Station_Id_C'], keep='first')
            wind_data['value'] = wind_data['WIN_S_Gust_Max']
            output = wind_data
        elif value_index==4:
            view = data[data['VIS_HOR_1MI']<30000][['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','VIS_HOR_1MI']]
            view_data = view.groupby(['Cnty','Station_levl','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['VIS_HOR_1MI'].min().reset_index()
            view_data['value'] = view_data['VIS_HOR_1MI'] 
            output = view_data
        return output
    def sql_hours(self,boundary,value_index,tables_name):
        tables_name_dir = {
            "24hours":24*60,
            "12hours":12*60,
            "6hours":6*60,
            "3hours":3*60,
            "2hours":2*60,
            "1hours":1*60,
            "45mins":45,
            "30mins":30,
            "now":0,
        }
        utc_now = dtt.datetime.utcnow()
        offset = dtt.timedelta(minutes=tables_name_dir[tables_name])
        start_times = (utc_now - offset).strftime('%Y-%m-%d %H:%M:%S')
        end_times = utc_now.strftime('%Y-%m-%d %H:%M:%S') 
        if value_index==3:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(wind) as wind
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and wind>0 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['WIN_S_Gust_Max'] = data.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
            data['WIN_D_Gust_Max'] = data.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
            data['value'] = data['WIN_S_Gust_Max']
        elif value_index==0:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, sum(rain) as PRE_sum
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and rain<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['PRE_sum']
        elif value_index==1:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(tmax) as tmax
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and tmax<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['tmax']
        elif value_index==2:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, min(tmin) as tmin
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and tmin<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['tmin']
        elif value_index==4:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, min(view) as view
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and view<30000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['view']
        return data 
    def sql_range(self,boundary,value_index,tables_name):
        tables_name_dir = {
            "24hours":24*60,
            "12hours":12*60,
            "6hours":6*60,
            "3hours":3*60,
            "1hours":1*60,
            "45mins":45,
            "30mins":30,
            "now":0,
        }
        utc_now = dtt.datetime.utcnow()
        offset = dtt.timedelta(minutes=tables_name_dir[tables_name])
        start_times = (utc_now - offset).strftime('%Y-%m-%d %H:%M:%S')
        end_times = utc_now.strftime('%Y-%m-%d %H:%M:%S') 
        if value_index==3:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(WIN_S_Gust_Max*10000 + WIN_D_Gust_Max) as wind
            from table_station_min 
            where Datetime between '{start_times}' and '{end_times}' and WIN_S_Gust_Max<5000 and WIN_D_Gust_Max<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['WIN_S_Gust_Max'] = data.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
            data['WIN_D_Gust_Max'] = data.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
            data['value'] = data['WIN_S_Gust_Max']
        elif value_index==0:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, sum(PRE) as PRE_sum
            from table_station_min 
            where Datetime between '{start_times}' and '{end_times}' and PRE<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['PRE_sum']
        elif value_index==1:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(TEM) as tmax
            from table_station_min 
            where Datetime between '{start_times}' and '{end_times}' and TEM<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['tmax']
        elif value_index==2:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, min(TEM) as tmin
            from table_station_min 
            where Datetime between '{start_times}' and '{end_times}' and TEM<5000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['tmin']
        elif value_index==4:
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, min(VIS_HOR_1MI) as view
            from table_station_min 
            where Datetime between '{start_times}' and '{end_times}' and VIS_HOR_1MI<30000 and Lat> {lat0} and Lat< {lat1} and Lon >{lon0} and Lon<{lon1}
            group by Station_Id_C""" 
            rsql = sql.format(start_times=start_times,end_times=end_times,lat0=boundary[0],lat1 = boundary[1],lon0 = boundary[2],lon1 = boundary[3])
            data = pd.read_sql(rsql, con=self.conn)
            data['value'] = data['view']
        return data
    def get_redis_rain(self,name):
        data = pickle.loads(self.rs.get(name))
        return data
    def get_regin(self,boundary,table_type,tables_name,value_index,zoom):
        print("测试",table_type,tables_name,value_index,zoom)
        '''解码单站数据'''
        ascending_list = [False,False,True,False,True]
        if tables_name=="now":
            data = self.sql_now(value_index,boundary)
            boundary_data =  data
        else:
            if tables_name in ["45mins","30mins"]:
                data = self.sql_range(boundary,value_index,tables_name)
                boundary_data =  data
            else:
                if value_index==0:
                    if tables_name in ["24hours","12hours","6hours","3hours","1hours"]:
                        redis_name = {
                            "24hours":"rain24",
                            "12hours":"rain12",
                            "6hours":"rain6",
                            "3hours":"rain3",
                            "1hours":"rain1"
                        }
                        data = self.get_redis_rain(redis_name[tables_name])
                        data['value'] = data['PRE']
                    else:
                        data = self.sql_hours(boundary,value_index,tables_name)
                else:
                    data = self.sql_hours(boundary,value_index,tables_name)
                lat0 = boundary[0]
                lat1 = boundary[1]
                lon0 = boundary[2]
                lon1 = boundary[3]
                boundary_data =  data[(data['Lat']>lat0) & (data['Lat']<lat1)  &  (data['Lon']<lon1) & (data['Lon']>lon0)]  
        if table_type=="nation":
            remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
        if table_type=="regin":
            remain = boundary_data[boundary_data['Station_levl']==14]
        elif table_type=="all":
            remain = boundary_data
        elif table_type=="main":
            remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
        elif table_type=="auto":   
            if value_index!=0: 
                # 温度 --- 全自动模式添加额外站点
                if  value_index ==1 or value_index ==2:     
                    if (boundary[3]-boundary[2])<2.9:
                        remain = boundary_data
                    elif (boundary[3]-boundary[2])<6:
                        remain = boundary_data[(boundary_data['Station_Id_C'].isin(['K8218','K8301','K8505']))|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                    else:
                        remain = boundary_data[(boundary_data['Station_Id_C'].isin(['K8218','K8301','K8505']))|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                # 风力 --- 显示8及以上大风
                elif value_index ==3:
                    if (boundary[3]-boundary[2])<2.9:
                        remain = boundary_data
                    elif (boundary[3]-boundary[2])<6:
                        remain = boundary_data[(boundary_data['WIN_S_Gust_Max']>17)|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                    else:
                        remain = boundary_data[(boundary_data['WIN_S_Gust_Max']>17)|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                # 能见度 --- 显示500米以下站
                elif value_index ==4:  
                    if (boundary[3]-boundary[2])<2.9:
                        remain = boundary_data
                    elif (boundary[3]-boundary[2])<6:
                        remain = boundary_data[(boundary_data['value']<=500)|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                    else:
                        remain = boundary_data[(boundary_data['value']<=500)|(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
            else:
                remain = boundary_data
        remain.sort_values(by="value",axis=0,ascending=ascending_list[value_index],inplace=True) # 从大到小     
        output = remain.to_json(orient='records',force_ascii=False)
        return output


# 气象服务快报的相关
class station_text:
    def __init__(self,city_code,start,end):
        self.rs = redis.Redis(host='127.0.0.1', port=6379)
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
        self.city_code = city_code
        self.start = start
        self.end = end
        self.data = self.data_status(city_code,start,end)
        self.city_codes = [331000]
    def data_status(self,city_code,start,end):
        if start =="extra_geojosn_remain":
            data = None
        else:
            data = self.comput_city(city_code,start,end)
        return data
    def comput_city(self,city_code,start,end):
        """快报或者统计数据的接口"""
        city_str = str(self.city_code)
        # getSurfEleInRegionByTime
        date_start = dtt.datetime.strptime(start,'%Y-%m-%d %H:%M:%S') 
        date_end = dtt.datetime.strptime(end,'%Y-%m-%d %H:%M:%S') 
        offset = dtt.timedelta(minutes=-60*8)
        label_start = (date_start + offset).strftime('%Y%m%d%H%M') + "00"
        label_end = (date_end + offset).strftime('%Y%m%d%H%M') + "00"   
        labels = "[" + label_start + "," + label_end + "]"
        #timelabel = self.decode_time_str(timesdelay)
        # client = DataQueryClient()
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "statSurfEleInRegion"  
        params = {
            'dataCode':"SURF_CHN_MUL_MIN",  #SURF_CHN_MUL_HOR
            'adminCodes':city_str,
            'elements':"Station_Id_C",
            'timeRange':labels,
            'statEles':'MAX_Province,MAX_City,MAX_Cnty,MAX_Town,MAX_Lat,MAX_Lon,MAX_Alti,MAX_Station_levl,MAX_Station_Name,MAX_TEM,MIN_TEM,SUM_PRE,MAX_PRE_1h,MIN_VIS_HOR_10MI,MAX_WIN_S_Gust_Max,MAX_WIN_S_Avg_2mi',
            #'orderBy':"MAX_WIN_S_Inst_Max:desc",
            'staLevels':"011,012,013,014,015,016", # 12国家站 14区域站
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        #rint(result)
        result_json = json.loads(result)
        clomns = "Station_Id_C,MAX_Province,MAX_City,MAX_Cnty,MAX_Town,MAX_Lat,MAX_Lon,MAX_Alti,MAX_Station_levl,MAX_Station_Name,MAX_TEM,MIN_TEM,SUM_PRE,MAX_PRE_1h,MIN_VIS_HOR_10MI,MAX_WIN_S_Gust_Max,MAX_WIN_S_Avg_2mi".split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        data.columns = "IIIII,Province,City,Cnty,Town,Lat,Lon,Height,Stationlevl,StationName,tmax,tmin,rain,rainhour,view,wind,windave".split(",")
        data = data.astype({'Lat': 'float', 'Lon': 'float','rain': 'float','wind': 'float', 'windave': 'float','tmax':'float','tmin':'float','view':'float','Stationlevl': 'int'}) 
        return data
    def comupt_city_csv(self,city_code,start,end):
        data = pd.read_csv("static/data/downfile/comput.csv")
        # data = pd.read_csv("downfile/comput.csv")
        return data
    def text_wind(self,plot_data):
        if plot_data=="none":
            orig = self.data
            data = orig.sort_values(by="wind",ascending=False)#.head(5).to_dict()
            wind = data[(data['wind']>0) & (data['wind']<5009)]   
            # del wind['Unnamed: 0'] # 天擎不需要
            # wind.reset_index(drop=True)# 天擎不需要
        else:
            wind = pd.read_json(json.dumps(plot_data), orient='records')
        ## 数据分级
        bins=[0,10.7,13.8,17.1,20.7,24.4,28.4,32.6,36.9,41.4,46.1,50.9,56,80]
        labels=['6级风以下','6级风','7级风','8级风','9级风','10级风','11级风','12级风','13级风','14级风','15级风','16级风','17级风']
        wind['rank']=pd.cut(wind['wind'],bins,right=False,labels=labels)
        # 获取单站较大的有
        testframe = wind.head(5)
        wind_json = wind.sort_values(by="wind",ascending=False).to_json(orient='records',force_ascii=False) 
        indextext = ""
        for index,row in testframe.iterrows():
            if self.city_code in self.city_codes:
                indextext = indextext + row['Cnty'] + row['StationName'] + str(row['wind']) + "米/秒，"
                #print(index,row['City'],row['Cnty'],row['Town'],row['wind'])
            else:
                indextext = indextext + row['Town'] + row['StationName'] + str(row['wind']) + "米/秒，"
                #print(index,row['City'],row['Cnty'],row['Town'],row['wind'])
        indextext = indextext[:-1] + "。"
        rank_station = {
            '17级及以上风':len(wind[(wind['wind']>=56.4) & (wind['wind']<80)].value_counts()),
            '16级风':len(wind[(wind['wind']>=50.9) & (wind['wind']<56.4)].value_counts()),
            '15级风':len(wind[(wind['wind']>=46.1) & (wind['wind']<50.9)].value_counts()),
            '14级风':len(wind[(wind['wind']>=41.4) & (wind['wind']<46.1)].value_counts()),
            '13级风':len(wind[(wind['wind']>=36.9) & (wind['wind']<41.4)].value_counts()),
            '12级风':len(wind[(wind['wind']>=32.6) & (wind['wind']<36.9)].value_counts()),
            '11级风':len(wind[(wind['wind']>=28.4) & (wind['wind']<32.6)].value_counts()),
            '10级风':len(wind[(wind['wind']>=24.4) & (wind['wind']<28.4)].value_counts()),
            '9级风':len(wind[(wind['wind']>=20.7) & (wind['wind']<24.4)].value_counts()),
            '8级风':len(wind[(wind['wind']>=17.1) & (wind['wind']<20.7)].value_counts()),
            '7级风':len(wind[(wind['wind']>=13.8) & (wind['wind']<17.1)].value_counts()),
            '6级风':len(wind[(wind['wind']>=10.7) & (wind['wind']<13.8)].value_counts()),
            '6级风以下':len(wind[(wind['wind']>0) & (wind['wind']<10.7)].to_dict())
        }
        #  统计各等级风力的个数
        numbertext = ""
        for key, value in rank_station.items():
            if value>0:
                numbertext = numbertext + key +"有" + str(value) + "站,"
        numbertext = numbertext[:-1] + "。"
        if rank_station['17级及以上风'] >0 :
            text = "【风力通报】 全市出现17级及以上大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['16级风'] >0 :
            text = "【风力通报】 全市出现16级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext  
        elif rank_station['15级风'] >0 :
            text = "【风力通报】 全市出现15级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['14级风'] >0 :
            text = "【风力通报】 全市出现14级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['13级风'] >0 :
            text = "【风力通报】 全市出现13级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['12级风'] >0 :
            text = "【风力通报】 全市出现12级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['11级风'] >0 :
            text = "【风力通报】 全市出现11级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['10级风'] >0 :
            text = "【风力通报】 全市出现10级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['9级风'] >0 :
            text = "【风力通报】 全市出现7～9级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['8级风'] >0 :
            text = "【风力通报】 全市出现6～8级大风，风力较大的有："
            text = text + indextext + "其中，"  + numbertext
        else:
            text = ""
        return text,wind_json
    def text_view(self,plot_data):
        if plot_data=="none":
            orig = self.data
            data = orig.sort_values(by="view",ascending=True)#.head(5).to_dict()
            view = data[(data['view']>0) & (data['view']<30000)]
            # del view['Unnamed: 0'] # 天擎不需要
            # view.reset_index(drop=True)# 天擎不需要
        else:
            view = pd.read_json(json.dumps(plot_data), orient='records')
        bins=[0,50,200,500,99999]
        labels=['强浓雾','浓雾','大雾','正常']
        view['rank']=pd.cut(view['view'],bins,right=False,labels=labels)
        rank_station = {
           '强浓雾':len(view[(view['view']>0) & (view['view']<50)].to_dict()),
            '浓雾':len(view[(view['view']>=50) & (view['view']<200)].value_counts()),
            '大雾':len(view[(view['view']>=200) & (view['view']<500)].value_counts()),
            '正常':len(view[(view['view']>=500) & (view['view']<99990)].value_counts()) 
        }
        # 获取单站较大的有
        testframe = view.head(5) 
        view_json = view.sort_values(by="view",ascending=True).to_json(orient='records',force_ascii=False)
        indextext = ""
        for index,row in testframe.iterrows():
            if self.city_code in self.city_codes:
                indextext = indextext + row['Cnty'] + row['StationName'] + str(row['view']) + "米，"
            else:
                indextext = indextext + row['Town'] + row['StationName'] + str(row['view']) + "米，"
        indextext = indextext[:-1] + "。"
        #  统计各等级风力的个数
        numbertext = ""
        for key, value in rank_station.items():
            if value>0 and key!="正常":
                numbertext = numbertext + key +"的有" + str(value) + "站,"
        numbertext = numbertext[:-1] + "。"
        if rank_station['强浓雾'] >0 :
            text = "【能见度】 全市出现低于50米的强浓雾，能见度较低的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['浓雾'] >0 :
            text = "【能见度】 全市出现低于200米的浓雾，能见度较低的有："
            text = text + indextext + "其中，"  + numbertext  
        elif rank_station['大雾'] >0 :
            text = "【能见度】 全市出现低于500米的大雾，能见度较低的有："
            text = text + indextext + "其中，"  + numbertext
        else:
            text =""
        return text,view_json
    def text_tmax(self,plot_data):
        if plot_data=="none":
            orig = self.data
            data = orig.sort_values(by="tmax",ascending=False)#.head(5).to_dict()
            tmax = data[(data['tmax']>-40) & (data['tmax']<100)]
            # del tmax['Unnamed: 0'] # 天擎不需要
            # tmax.reset_index(drop=True)# 天擎不需要
        else:
            tmax = pd.read_json(json.dumps(plot_data), orient='records')
        bins=[-40,35,37,40,80]
        labels=['正常','35度以上','37度以上','40度以上']
        tmax['rank']=pd.cut(tmax['tmax'],bins,right=True,labels=labels)
        rank_station = {
            '40度以上':len(tmax[(tmax['tmax']>=40) & (tmax['tmax']<80)].value_counts()),
            '37度以上':len(tmax[(tmax['tmax']>=37) & (tmax['tmax']<40)].value_counts()),
            '35度以上':len(tmax[(tmax['tmax']>=35) & (tmax['tmax']<37)].value_counts()),
           '正常':len(tmax[(tmax['tmax']>-40) & (tmax['tmax']<35)].to_dict())      
        }
        # 获取单站较大的有
        testframe = tmax.head(5) 
        tmax_json = tmax.sort_values(by="tmax",ascending=False).to_json(orient='records',force_ascii=False)
        indextext = ""
        for index,row in testframe.iterrows():
            if self.city_code in self.city_codes:
                indextext = indextext + row['Cnty'] + row['StationName'] + str(row['tmax']) + "℃，"
            else:
                indextext = indextext + row['Town'] + row['StationName'] + str(row['tmin']) + "℃，"
        indextext = indextext[:-1] + "。"
        #  统计各等级风力的个数
        numbertext = ""
        for key, value in rank_station.items():
            if value>0 and key not in ["正常","35度以上"]:
                numbertext = numbertext + key +"的有" + str(value) + "站,"
        numbertext = numbertext[:-1] + "。"
        if rank_station['40度以上'] >0 :
            text = "【高温通报】 全市出现40℃以上的高温，温度较高的有："
            text = text + indextext + "其中，"  + numbertext
        elif rank_station['37度以上'] >0 :
            text = "【高温通报】 全市出现40℃以上的高温，温度较高的有："
            text = text + indextext + "其中，"  + numbertext  
        elif rank_station['35度以上'] >0 :
            text = "【高温通报】 全市出现40℃以上的高温，温度较高的有："
            text = text + indextext 
        else:
            text = ""
        return text,tmax_json
    def get_rain_max(self):
        if self.start =="extra_geojosn_remain":
            data = []
        else:
            date_start = dtt.datetime.strptime(self.start,'%Y-%m-%d %H:%M:%S') 
            date_end = dtt.datetime.strptime(self.end,'%Y-%m-%d %H:%M:%S') 
            offset = dtt.timedelta(minutes=-60*8)
            label_start = (date_start + offset).strftime('%Y-%m-%d %H:%M:%S')
            label_end = (date_end + offset).strftime('%Y-%m-%d %H:%M:%S')
            sql = """select max(City) as City,max(Station_Name) as Station_Name,max(Cnty) as Cnty,max(Town) as Town,Station_Id_C,max(PRE) as PRE from taizhou_rain_max 
            where City='台州市' and Datetime between '{label_start}'  and '{label_end}' group by Station_Id_C order by PRE desc limit 5""" 
            rsql = sql.format(label_start = label_start,label_end = label_end)
            data = pd.read_sql(rsql, con=self.conn)
        return data
    def text_rain(self,plot_data):
            # 面雨量
        if plot_data=="none":
            orig = self.data
            data = orig.sort_values(by="rain",ascending=False)
            rain = data[(data['rain']>=0) & (data['rain']<5009)]
            # del rain['Unnamed: 0'] # 天擎不需要
            # rain.reset_index(drop=True)
        else:
            rain = pd.read_json(json.dumps(plot_data), orient='records')
        bins=[0,30,50,80,100,150,200,250,300,400,500,800,1000,2000]
        labels=['≥0毫米','≥30毫米','≥50毫米','≥80毫米','≥100毫米','≥150毫米','≥200毫米','≥250毫米','≥300毫米','≥400毫米','≥500毫米','≥800毫米','≥1000毫米']
        bins_text = [0,5,16.9,37.9,74.5,5000]
        labels_text = ['小雨','小到中雨','中到大雨','大到暴雨','大暴雨']
        rain['rank']=pd.cut(rain['rain'],bins,right=False,labels=labels)
        rain['rank_label']=pd.cut(rain['rain'],bins_text,right=False,labels=labels_text)
        # 数据校验
        def vaild_rain(col):
            lat = col['Lat']
            lon = col['Lon']
            pre = col['rain']
            IIIII = col['IIIII']
            s = lat - 0.07
            n = lat + 0.07
            w = lon - 0.07
            e = lon + 0.07
            stdf = rain[(rain['Lat']>s)&(rain['Lat']<n)&(rain['Lon']<e)&(rain['Lon']>w)]
            stdq = stdf[(True^stdf['IIIII'].isin([IIIII]))]
            stdmean = stdq['rain'].mean()
            stds = stdq['rain'].std()
            stdabs = abs(pre - stdmean)
            if stdabs > stds*3:
                #value = stdabs - stds
                value = 1
            else:
                #value = stdabs - stds
                value = 0
            del stdf,stdq
            return value
        rain['std'] = rain.apply(vaild_rain,axis=1) 
        # 面雨量
        if self.city_code in self.city_codes:
            cnty = rain.groupby(['Cnty'])['rain'].mean().to_dict()
            
            text_average_city = "面雨量：" + str(round(rain['rain'].mean(),1)) + "毫米，" +"各市（县）面雨量如下:"
        else:
            cnty = rain.groupby(['Town'])['rain'].mean().to_dict()
            text_average_city = "各乡镇面雨量如下:"
        items = sorted(cnty.items())
        sorted_cnty = {k: v for k, v in sorted(cnty.items(), key=lambda x: x[1], reverse=True)}
        for key, value in sorted_cnty.items():
            if value>0.05:
                text_average_city = text_average_city + key + ":" + str(round(value,1)) + "毫米,"
        text_average_city = text_average_city[:-1] + "。"  
        # 流域面雨量
        river =  pd.read_csv("static/data/river_tz.csv")
        river_average = {
            "永宁江流域":pd.merge(rain,river.query('Property.str.contains("YN")'),on='IIIII',how='inner'),
            "始丰溪流域":pd.merge(rain,river.query('Property.str.contains("SF")'),on='IIIII',how='inner'),
            "永安溪流域":pd.merge(rain,river.query('Property.str.contains("YA")'),on='IIIII',how='inner'),
            "牛头山流域":pd.merge(rain,river.query('Property.str.contains("NTS")'),on='IIIII',how='inner'),
            "大田流域":pd.merge(rain,river.query('Property.str.contains("DT")'),on='IIIII',how='inner'),
            "金清港流域":pd.merge(rain,river.query('Property.str.contains("JQG")'),on='IIIII',how='inner'),
            "长潭水库流域":pd.merge(rain,river.query('Property.str.contains("CT")'),on='IIIII',how='inner')
        }
        text_river = ""
        sort_river = {}
        for key, value in river_average.items():
            river_data = value
            river_ave = round(river_data['rain'].mean(),1)
            if river_ave>0.1:
                sort_river[key] = river_ave
        river_list = sorted(sort_river.items(),key = lambda x:x[1],reverse = True)
        for item in river_list:
            text_river = text_river + item[0] + ":" + str(item[1]) + "毫米,"  
        if len(text_river)>1:
            text_river = "【流域雨量】面雨量较大的有"+text_river[:-1] + "。"
        else:
            text_river =""    
        # 单站前十
        rain_max = rain.sort_values(by="rain",ascending=False).head(10)
        rain_json = rain.sort_values(by="rain",ascending=False).to_json(orient='records',force_ascii=False)
        indextext = "【单站雨量】雨量较大的有："
        for index,row in rain_max.iterrows():
            if self.city_code in self.city_codes:
                indextext = indextext + row['Cnty'] + row['StationName'] + str(row['rain']) + "毫米，"
            else:
                indextext = indextext + row['Town'] + row['StationName'] + str(row['rain']) + "毫米，"
        indextext = indextext[:-1] + "。"
        # 小时雨强较大的有 ------------------------------
        rain_hours_max = self.get_rain_max()
        # print("测试",rain_hours_max)
        if len(rain_hours_max)>0:
            if max(rain_hours_max['PRE'])>20:
                rain_hours_max_text = ""
                for index,row in rain_hours_max.iterrows():
                    rain_hours_max_text = rain_hours_max_text + row['Cnty'] + row['Station_Name'] + str(row['PRE']) + "毫米，"
                rain_hours_max_text = "【小时雨强】雨强较大的有" +  rain_hours_max_text[:-1] + "。" 
            else:
                rain_hours_max_text = ""
        else:
            rain_hours_max_text = ""
        # 乡镇前十
        town_max = rain.groupby(['Town','Cnty'])['rain'].max().sort_values(ascending=False).head(10)
        indextext_town = "【乡镇雨量】雨量较大的有："
        for index,row in rain_max.iterrows():
            if self.city_code in self.city_codes:
                indextext_town = indextext_town + row['Cnty'] + row['StationName'] + str(row['rain']) + "毫米，"
            else:
                indextext_town = indextext_town + row['Town'] + row['StationName'] + str(row['rain']) + "毫米，"
        indextext_town = indextext_town[:-1] + "。"
        # 雨量个数统计
        rank_station = {
            '≥1000毫米':len(rain[(rain['rain']>=1000) & (rain['rain']<2000)].value_counts()),
            '≥800毫米':len(rain[(rain['rain']>=800) & (rain['rain']<1000)].value_counts()),
            '≥500毫米':len(rain[(rain['rain']>=500) & (rain['rain']<800)].value_counts()),
            '≥400毫米':len(rain[(rain['rain']>=400) & (rain['rain']<500)].value_counts()),
            '≥300毫米':len(rain[(rain['rain']>=300) & (rain['rain']<400)].value_counts()),
            '≥250毫米':len(rain[(rain['rain']>=250) & (rain['rain']<300)].value_counts()),
            '≥200毫米':len(rain[(rain['rain']>=200) & (rain['rain']<250)].value_counts()),
            '≥150毫米':len(rain[(rain['rain']>=150) & (rain['rain']<200)].value_counts()),
            '≥100毫米':len(rain[(rain['rain']>=100) & (rain['rain']<150)].value_counts()),
            '≥80毫米':len(rain[(rain['rain']>=80) & (rain['rain']<100)].value_counts()),
            '≥50毫米':len(rain[(rain['rain']>=50) & (rain['rain']<80)].value_counts()),
            '≥30毫米':len(rain[(rain['rain']>=30) & (rain['rain']<50)].value_counts()),
            '≥10毫米':len(rain[(rain['rain']>=10) & (rain['rain']<30)].value_counts()),
            '≥0毫米':len(rain[(rain['rain']>0) & (rain['rain']<10)].value_counts()),
        }
        numbertext = "其中，"
        for key, value in rank_station.items():
            if value>0 and key not in ["≥0毫米"]:
                numbertext = numbertext + key +"的有" + str(value) + "站,"
        numbertext = numbertext[:-1] + "。"
        # 雨量乡镇数统计
        town_group = rain.groupby(['Town'])['rain'].max()  
        dict_town = {'Town':town_group.index,'rain':town_group.values}
        town_range = pd.DataFrame(data =dict_town)
        town_range['rank']=pd.cut(town_range['rain'],bins,right=False,labels=labels)
        rank_station_town = {
            '≥1000毫米':len(town_range[(town_range['rain']>=1000) & (town_range['rain']<2000)].value_counts()),
            '≥800毫米':len(town_range[(town_range['rain']>=800) & (town_range['rain']<1000)].value_counts()),
            '≥500毫米':len(town_range[(town_range['rain']>=500) & (town_range['rain']<800)].value_counts()),
            '≥400毫米':len(town_range[(town_range['rain']>=400) & (town_range['rain']<500)].value_counts()),
            '≥300毫米':len(town_range[(town_range['rain']>=300) & (town_range['rain']<400)].value_counts()),
            '≥250毫米':len(town_range[(town_range['rain']>=250) & (town_range['rain']<300)].value_counts()),
            '≥200毫米':len(town_range[(town_range['rain']>=200) & (town_range['rain']<250)].value_counts()),
            '≥150毫米':len(town_range[(town_range['rain']>=150) & (town_range['rain']<200)].value_counts()),
            '≥100毫米':len(town_range[(town_range['rain']>=100) & (town_range['rain']<150)].value_counts()),
            '≥80毫米':len(town_range[(town_range['rain']>=80) & (town_range['rain']<100)].value_counts()),
            '≥50毫米':len(town_range[(town_range['rain']>=50) & (town_range['rain']<80)].value_counts()),
            '≥30毫米':len(town_range[(town_range['rain']>=30) & (town_range['rain']<50)].value_counts()),
            '≥10毫米':len(town_range[(town_range['rain']>=10) & (town_range['rain']<30)].value_counts()),
            '≥0毫米':len(town_range[(town_range['rain']>0) & (town_range['rain']<10)].value_counts()),
        }
        numbertext_town = "其中，"
        for key, value in rank_station_town.items():
            if value>0 and key not in ["≥0毫米"]:
                numbertext_town = numbertext_town + key +"的有" + str(value) + "站,"
        numbertext_town = numbertext_town[:-1] + "。"
        # 统计全市雨情
        rank_text = {
            '大暴雨':sum(rain[rain['rank_label']=='大暴雨']['rain']),
            '大到暴雨':sum(rain[rain['rank_label']=='大到暴雨']['rain']),
            '中到大雨':sum(rain[rain['rank_label']=='中到大雨']['rain']),
            '小到中雨':sum(rain[rain['rank_label']=='小到中雨']['rain']),
            '小雨':sum(rain[rain['rank_label']=='小雨']['rain'])
        }
        res = max(rank_text, key=lambda x: rank_text[x])
        if max(rank_text, key=lambda x: rank_text[x])=="大暴雨":
            text = "【雨情通报】 全市出现" + res +"。" + text_average_city + "<br>" + indextext + numbertext + "<br>" + rain_hours_max_text + "<br>" + indextext_town + numbertext_town + "<br>" + text_river
        elif max(rank_text, key=lambda x: rank_text[x])=="大到暴雨":
            text = "【雨情通报】 全市出现" + res +"。" + text_average_city + "<br>" + indextext + numbertext + "<br>" + rain_hours_max_text + "<br>" + indextext_town + numbertext_town + "<br>" + text_river
        elif max(rank_text, key=lambda x: rank_text[x])=="中到大雨":
            text = "【雨情通报】 全市出现" + res +"。" + text_average_city + "<br>" + indextext + numbertext + "<br>" + rain_hours_max_text + "<br>" + indextext_town + numbertext_town + "<br>" + text_river
        elif max(rank_text, key=lambda x: rank_text[x])=="小到中雨":
            text = "【雨情通报】 全市出现" + res +"。" + text_average_city + "<br>" + indextext + numbertext + "<br>" + rain_hours_max_text + "<br>" + indextext_town + numbertext_town + "<br>" + text_river
        elif max(rank_text, key=lambda x: rank_text[x])=="小雨":
            text = "【雨情通报】 全市出现" + res +"。" + text_average_city + "<br>" + indextext + numbertext + "<br>" + rain_hours_max_text + "<br>" + indextext_town + numbertext_town + "<br>" + text_river
        return text,rain_json
    def main(self):
        text = ""
        plot_data = "none"
        text_rain,rain_json = self.text_rain(plot_data)
        text_wind,wind_json = self.text_wind(plot_data)
        text_tmax,tmax_json = self.text_tmax(plot_data)
        text_view,view_json = self.text_view(plot_data)
        text = text + text_rain + "<br>" + text_wind + "<br>" + text_tmax + "<br>" + text_view
        return text,rain_json,wind_json,tmax_json,view_json
    def remain(self,plot_rain,plot_wind,plot_tmax,plot_view):
        text = ""
        text_rain,rain_json = self.text_rain(plot_rain)
        text_wind,wind_json = self.text_wind(plot_wind)
        text_tmax,tmax_json = self.text_tmax(plot_tmax)
        text_view,view_json = self.text_view(plot_view)
        text = text + text_rain + "<br>" + text_wind + "<br>" + text_tmax + "<br>" + text_view
        return text,rain_json,wind_json,tmax_json,view_json
    def plot_rain(self,plot_type,plot_data):
        if plot_type =="none":
            orig = self.data
            data = orig.sort_values(by="rain",ascending=False)
            rain = data[(data['rain']>=0) & (data['rain']<5009)]
        else:  
            rain = pd.read_json(json.dumps(plot_data), orient='records')
        lat = np.array(rain['Lat'].to_list())
        lon = np.array(rain['Lon'].to_list())
        Zi = np.array(rain['rain'].to_list())
        data_max = max(Zi)
        data_min = min(Zi)
        np.set_printoptions(precision = 2)
        x = np.arange(120.0,122.0,0.01)
        y = np.arange(27.8,29.5,0.01)
        nx0 =len(x)
        ny0 =len(y)
        X, Y = np.meshgrid(x, y)#100*100
        P = np.array([X.flatten(), Y.flatten() ]).transpose()    
        Pi =  np.array([lon, lat ]).transpose()
        Z_linear = griddata(Pi, Zi, P, method = "nearest").reshape([ny0,nx0])
        gauss_kernel = Gaussian2DKernel(0.1)
        smoothed_data_gauss = convolve(Z_linear, gauss_kernel)
        data_xr = xr.DataArray(Z_linear, coords=[ y,x], dims=["lat", "lon"])
        lat = data_xr.lat
        lon = data_xr.lon
        lons, lats = np.meshgrid(lon, lat)
        start_time = dtt.datetime.strptime(self.start, "%Y-%m-%d %H:%M:%S")
        end_time = dtt.datetime.strptime(self.end, "%Y-%m-%d %H:%M:%S")
        hours = (end_time-start_time).total_seconds()//3600
        if hours >12:
            colorslist = ['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 24降水
            levels = [-1,0.01, 5, 10, 15, 20, 25, 35, 50, 75, 100, 150, 200, 250, 350, 500]
            cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
            norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N) 
        elif hours <=12 and hours >=1:
                colorslist =['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 06降水
                levels = [-1,3, 4, 5, 10, 15, 20, 25, 35, 50, 60, 70, 80, 90, 100, 110]
                cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
                norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N)  # 基于离散区间生成颜色映射索引       
        elif hours <1:
            colorslist =['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 01降水
            levels = [-1,0.01, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13, 15, 17, 20]
            cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
            norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N)
        contour = plt.contourf(lons,lats,data_xr,cmap=cmap_nonlin,levels =levels,norm = norm )
        geojson = geojsoncontour.contourf_to_geojson(
            contourf=contour,
            ndigits=3,
            unit='mm'
        )
        plt.close()
        return geojson,hours

# redis数据轮询问
class station_sql_data:
    def __init__(self):
        #self.rs = redis.Redis(host='127.0.0.1', port=6379)
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        #self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="051219",db="ZJSZDZDB")
        self.conn = pymssql.connect("172.21.158.201","down","downx","ZJSZDZDB")
        self.redis_name = {
            "24hours":"table_24hours",
            "12hours":"table_12hours",
            "6hours":"table_6hours",
            "3hours":"table_3hours",
            "2hours":"table_2hours",
            "1hours":"table_1hours",
            "45mins":"table_45mins",
            "30mins":"table_30mins",
            "15mins":"table_15mins"         
        }
    def time_str(self,timesdelay):
        timestr = lambda x:str(x) if x >= 10 else "0"+str(x)
        SHA_TZ = timezone(
            dtt.timedelta(hours=8),
            name='Asia/Shanghai',
        )
        utc_now = dtt.datetime.utcnow().replace(tzinfo=dtt.timezone.utc)
        today = utc_now.astimezone(SHA_TZ)
        end_time = today.strftime('%Y-%m-%d %H:%M:%S') 
        offset = dtt.timedelta(minutes=-timesdelay)
        offday = (today + offset)
        # 当前时刻
        year_today = today.year
        year_today = timestr(year_today)
        month_today = today.month
        month_today = timestr(month_today)
        day_today = today.day
        day_today = timestr(day_today)
        hour_today = today.hour
        hour_today = timestr(hour_today)
        minutes_today = today.minute
        minutes_today = timestr(minutes_today)
        # 历史时刻
        year_offday = offday.year
        year_offday = timestr(year_offday)
        month_offday = offday.month
        month_offday = timestr(month_offday)
        day_offday = offday.day
        day_offday = timestr(day_offday)
        hour_offday = offday.hour
        hour_offday = timestr(hour_offday)
        minutes_offday = offday.minute
        minutes_offday = timestr(minutes_offday)
        offday_label = year_offday + "-" + month_offday + "-" + day_offday + " " + hour_offday + ":" + minutes_offday + ":00"
        today_label = year_today + "-" + month_today + "-" + day_today + " " + hour_today + ":" + minutes_today + ":00"
        return offday_label,today_label

    def rain_sql(self,tables_name,timesdelay):
        start_time,end_time = self.time_str(timesdelay)
        sql = """select ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon,sum(Ri) as value
        from Tab_AM_M as ta inner join TAB_StationInfo as station on ta.IIIII=station.IIiii and station.Province ='浙江' 
        where (TTime BETWEEN '{start}' AND '{end}' and  ta.Ri!=-9999) 
        group by  ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon"""
        rsql = sql.format(start=start_time,end = end_time)
        station_all = pd.read_sql(rsql, con=self.conn)
        # 设置redis的键值对儿
        data = {
            "time":start_time,
            "data":station_all
        }
        redis_name_str = self.redis_name[tables_name] + "_rain"
        self.rs.set(redis_name_str, pickle.dumps(data))
        
    def tmax_sql(self,tables_name,timesdelay):
        start_time,end_time = self.time_str(timesdelay)
        sql = """select ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon,max(T) as value
        from Tab_AM_M as ta inner join TAB_StationInfo as station on ta.IIIII=station.IIiii and station.Province ='浙江' 
        where (TTime BETWEEN '{start}' AND '{end}' and  ta.T!=-9999) 
        group by  ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon"""
        rsql = sql.format(start=start_time,end = end_time)
        station_all = pd.read_sql(rsql, con=self.conn)
        # 设置redis的键值对儿
        data = {
            "time":start_time,
            "data":station_all
        }
        redis_name_str = self.redis_name[tables_name] + "_tmax"
        self.rs.set(redis_name_str, pickle.dumps(data))
    def tmin_sql(self,tables_name,timesdelay):
        start_time,end_time = self.time_str(timesdelay)
        sql ="""select ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon,min(T) as value
        from Tab_AM_M as ta inner join TAB_StationInfo as station on ta.IIIII=station.IIiii and station.Province ='浙江' 
        where (TTime BETWEEN '{start}' AND '{end}' and  ta.T!=-9999) 
        group by  ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon"""
        rsql = sql.format(start=start_time,end = end_time)
        station_all = pd.read_sql(rsql, con=self.conn)
        # 设置redis的键值对儿
        data = {
            "time":start_time,
            "data":station_all
        }
        redis_name_str = self.redis_name[tables_name] + "_tmin"
        self.rs.set(redis_name_str, pickle.dumps(data))
    def view_sql(self,tables_name,timesdelay):
        start_time,end_time = self.time_str(timesdelay)
        sql = """select ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon,min(V) as value
        from Tab_AM_M as ta inner join TAB_StationInfo as station on ta.IIIII=station.IIiii and station.Province ='浙江' 
        where (TTime BETWEEN '{start}' AND '{end}' and ta.V!=-9999 ) 
        group by  ta.IIIII,station.StationName,station.Province,station.City,station.County,station.Town,station.ZoomLevel,station.Type,station.lat,station.lon"""
        rsql = sql.format(start=start_time,end = end_time)
        station_all = pd.read_sql(rsql, con=self.conn)
        # 设置redis的键值对儿
        data = {
            "time":start_time,
            "data":station_all
        }
        redis_name_str = self.redis_name[tables_name] + "_view"
        self.rs.set(redis_name_str, pickle.dumps(data))

    def wind_sql(self,tables_name,timesdelay):
        start_time,end_time = self.time_str(timesdelay)
        sql = """SELECT MAX(ta.FF*1000 +ta.DF)AS fff,ta.IIIII,max(ta.FF) AS fFy,
            max(station.StationName) as StationName, max(station.Province) as Province, max(station.City) as City,
            max(station.County) as County, max(station.Town) as Town, max(station.ZoomLevel) as ZoomLevel, max(station.Type) as Type, 
            max(station.lon) as lon, max(station.lat) as lat
            FROM Tab_AM_M as ta inner join TAB_StationInfo as station on ta.IIIII=station.IIiii and station.Province ='浙江' 
            WHERE (TTime BETWEEN '{start}' AND '{end}' AND ta.FF>0 AND ta.DF>0 ) GROUP BY ta.IIIII ORDER BY fff desc"""
        rsql = sql.format(start=start_time,end=end_time)
        station_data = pd.read_sql(rsql, con=self.conn)
        station_data["fstr"] =station_data["fff"].astype(str)
        station_data['dFy'] = station_data["fstr"].str.slice(-3).to_list()
        station_data['dFy'] = pd.to_numeric(station_data['dFy'])
        station_data['value'] = station_data["fFy"]
#         station_data['value'] = station_data["fstr"].str.slice(0,3).to_list()
#         station_data['value'] = pd.to_numeric(station_data['value'])
        # 设置redis的键值对儿
        data = {
            "time":start_time,
            "data":station_data
        }
        redis_name_str = self.redis_name[tables_name] + "_wind"
        self.rs.set(redis_name_str, pickle.dumps(data))


# 数据同步到station_zdz_min
class data2sql():
    def __init__(self):
        #self.rs = redis.Redis(host='127.0.0.1', port=6379)
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
    def get_range(self):
        linux = dtt.datetime.now()
        offset = dtt.timedelta(minutes=(-60*8))
        start_delay =  dtt.timedelta(minutes=(-60*8-5))
        now = (linux+offset).strftime('%Y-%m-%d %H:%M')+":00"
        old = (linux+start_delay).strftime('%Y-%m-%d %H:%M')+":00"
        start = old[0:4] + old[5:7] + old[8:10] + old[11:13] + old[14:16] +"00"
        end = now[0:4] + now[5:7] + now[8:10] + now[11:13] + now[14:16] +"00"
        label = "["+start+","+end+"]"
        return label
    def to_redis(self,data):
        self.rs.set("warring_zdz", pickle.dumps(data))
    def get_data(self):
        label = self.get_range()
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "getSurfEleInRectByTimeRange"
        params = {
            'dataCode':"SURF_CHN_MUL_MIN",  #SURF_CHN_MUL_HOR
            'elements':"Cnty,Province,Town,Station_levl,Station_Name,City,Station_Id_C,Lat,Lon,Alti,Datetime,PRS,PRE_1h,PRS_Sea,WIN_S_Inst_Max,WIN_D_INST_Max,WIN_D_Avg_2mi,WIN_S_Avg_2mi,TEM,DPT,RHU,VAP,PRE,Snow_Depth,VIS_HOR_10MI,CLO_Cov,CLO_Height_LoM",
            'timeRange':label,
            'minLat':"25",
            'minLon':"115",
            'maxLat':"35",
            'maxLon':"125",
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = params['elements'].split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        reclomns = ["Cnty","Province","Town","Station_levl","Station_Name","City",
                    "Station_Id_C","Lat","Lon","Alti","Datetime","PRS","PRE_1h","PRS_Sea","WIN_S_Inst_Max",
                    "WIN_D_INST_Max","WIN_D_Avg_2mi","WIN_S_Avg_2mi","TEM","DPT","RHU","VAP",
                    "PRE","Snow_Depth","VIS_HOR_1MI","CLO_Cov","CLO_Height_LoM"]
        data.columns=reclomns
        return data
    def to_mysql(self):
        data = self.get_data()
        #data[data==""] = None
        datalist = data.to_json(orient='values') # 生成数据列表
        sql_list = json.loads(datalist) 
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        sql = """insert ignore into table_station_min 
        (Cnty,Province,Town,Station_levl,Station_Name,City,Station_Id_C,Lat,Lon,Alti,Datetime,PRS,PRE_1h,PRS_Sea,WIN_S_Gust_Max,WIN_D_Gust_Max,WIN_D_Avg_2mi,WIN_S_Avg_2mi,TEM,DPT,RHU,VAP,PRE,Snow_Depth,VIS_HOR_1MI,CLO_Cov,CLO_Height_LoM) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.executemany(sql,sql_list)
        cursor.close()
        conn.commit()
        conn.close()
        self.to_redis(data)
    def del_mysql(self):
        now = dtt.datetime.now()   
        offset = dtt.timedelta(days=-1)
        del_day = (now + offset).strftime('%Y-%m-%d %H:%M')+":00"
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        sql = """DELETE FROM table_station_min where Datetime <= '{del_day}' limit 10000"""
        rsql = sql.format(del_day=del_day)
        cursor.execute(rsql)
        cursor.close()
        conn.commit()
        conn.close()
    def del_mysql_hour(self):
        now = dtt.datetime.now()   
        offset = dtt.timedelta(days=-5)
        del_day = (now + offset).strftime('%Y-%m-%d %H:%M')+":00"
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        sql = """DELETE FROM table_station_hour where Datetime <= '{del_day}' limit 500"""
        rsql = sql.format(del_day=del_day)
        cursor.execute(rsql)
        cursor.close()
        conn.commit()
        conn.close()
    def sql_wind(self,start_times,end_times):
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province, max(Station_levl) as Station_levl,
        max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(WIN_S_Gust_Max*10000 + WIN_D_Gust_Max) as wind
        from table_station_min 
        where Datetime between '{start_times}' and '{end_times}' and WIN_S_Gust_Max<5000 and WIN_D_Gust_Max<5000
        group by Station_Id_C""" 
        rsql = sql.format(start_times=start_times,end_times=end_times)
        data = pd.read_sql(rsql, con=self.conn)
        sql_data = data[['Station_Id_C','Cnty','City','Province','Station_levl','Station_Name','Town','Alti','Lat','Lon','wind']]
        sql_data['Datetime'] = start_times
        datalist = sql_data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into table_station_hour 
        (Station_Id_C,Cnty,City,Province,Station_levl,Station_Name,Town,Alti,Lat,Lon,wind,Datetime) 
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
    def sql_temp(self,start_times,end_times):
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province, max(Station_levl) as Station_levl,
        max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(TEM) as tmax,min(TEM) as tmin
        from table_station_min 
        where Datetime between '{start_times}' and '{end_times}' and TEM<5000 
        group by Station_Id_C""" 
        rsql = sql.format(start_times=start_times,end_times=end_times)
        data = pd.read_sql(rsql, con=self.conn)
        sql_data = data[['Station_Id_C','Cnty','City','Province','Station_levl','Station_Name','Town','Alti','Lat','Lon','tmax','tmin']]
        sql_data['Datetime'] = start_times
        datalist = sql_data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into table_station_hour 
        (Station_Id_C,Cnty,City,Province,Station_levl,Station_Name,Town,Alti,Lat,Lon,tmax,tmin,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
        tmax = values(tmax),
        tmin = values(tmin),
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
    def sql_rain(self,start_times,end_times):
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province, max(Station_levl) as Station_levl,
        max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, sum(PRE) as rain , max(PRE_1h) as rain_max
        from table_station_min 
        where Datetime between '{start_times}' and '{end_times}' and PRE<5000  
        group by Station_Id_C""" 
        rsql = sql.format(start_times=start_times,end_times=end_times)
        data = pd.read_sql(rsql, con=self.conn)
        sql_data = data[['Station_Id_C','Cnty','City','Province','Station_levl','Station_Name','Town','Alti','Lat','Lon','rain','rain_max']]
        sql_data['Datetime'] = start_times
        datalist = sql_data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into table_station_hour 
        (Station_Id_C,Cnty,City,Province,Station_levl,Station_Name,Town,Alti,Lat,Lon,rain,rain_max,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
        rain = values(rain),
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
    def sql_view(self,start_times,end_times):
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
        max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, min(VIS_HOR_1MI) as view
        from table_station_min 
        where Datetime between '{start_times}' and '{end_times}' and VIS_HOR_1MI<30000 
        group by Station_Id_C""" 
        rsql = sql.format(start_times=start_times,end_times=end_times)
        data = pd.read_sql(rsql, con=self.conn)
        sql_data = data[['Station_Id_C','Cnty','City','Province','Station_levl','Station_Name','Town','Alti','Lat','Lon','view']]
        sql_data['Datetime'] = start_times
        datalist = sql_data.to_json(orient='values')
        sql_list = json.loads(datalist)
        conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        cursor = conn.cursor()
        insql = """insert into table_station_hour
        (Station_Id_C,Cnty,City,Province,Station_levl,Station_Name,Town,Alti,Lat,Lon,view,Datetime) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on duplicate key update
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

# 历史回放
class station_zdz_history():
    def __init__(self,time):
        #self.rs = redis.Redis(host='127.0.0.1', port=6379)
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
        self.time = time
    def get_regin(self,boundary,table_type,tables_name,value_index,zoom):
        ascending_list = [False,False,True,False,True]
        if value_index==0:
            boundary_data = self.get_rain(boundary,tables_name)
        elif value_index==1 or value_index==2:
            boundary_data = self.get_temp(boundary,tables_name)
        elif value_index==3:
            boundary_data = self.get_wind(boundary,tables_name)
        elif value_index==4:
            boundary_data = self.get_view(boundary,tables_name)
        if table_type=="nation":
            remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
        if table_type=="regin":
            remain = boundary_data[boundary_data['Station_levl']==14]
        elif table_type=="all":
            remain = boundary_data
        elif table_type=="main":
            remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
        elif table_type=="auto":   
            if value_index!=0:        
                if (boundary[3]-boundary[2])<2.9:
                    remain = boundary_data
                elif (boundary[3]-boundary[2])<6:
                    remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==15)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
                else:
                    remain = boundary_data[(boundary_data['Station_levl']==11)| (boundary_data['Station_levl']==12)| (boundary_data['Station_levl']==13)| (boundary_data['Station_levl']==16)]
            else:
                remain = boundary_data    
        remain.sort_values(by="value",axis=0,ascending=ascending_list[value_index],inplace=True) # 从大到小     
        output = remain.to_json(orient='records',force_ascii=False)
        return output
    def get_rain(self,boundary,tables_name):
        # boundary 边界 value_index 数据类型  tables_name 数据字段   
        table_dir = {
            "3hours":"PRE_3h",
            "6hours":"PRE_6h",
            "12hours":"PRE_12h",
            "24hours":"PRE_24h",
            "48hours":47,
            "72hours":71,
            "96hours":95
        }
        if tables_name not in ['48hours','72hours','96hours']:
            elementstr = "Datetime,Station_Id_C,Station_Name,Province,City,Cnty,Town,Station_levl,Station_Type,Lat,Lon,Alti," + table_dir[tables_name]
            eleValueRangestr = table_dir[tables_name] +  ":(0,10000)"
            end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
            offset = dtt.timedelta(minutes=(-60*8))
            label = (end_time+offset).strftime('%Y%m%d%H')+"0000"
            client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
            interfaceId = "getSurfEleInRectByTime"
            params = {
                'dataCode':"SURF_CHN_MUL_HOR",  #SURF_CHN_MUL_HOR
                'elements':elementstr,
                'times':label,
                'minLat':str(boundary[0]),
                'minLon':str(boundary[2]),
                'maxLat':str(boundary[1]),
                'maxLon':str(boundary[3]),
                'eleValueRanges':eleValueRangestr,
                'limitCnt':"100000000"
            }
            result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
            result_json = json.loads(result)
            clomns = params['elements'].split(",")
            data = pd.DataFrame(result_json['DS'],columns=clomns)
            data = data.astype({'Lat': 'float', 'Lon': 'float', 'Station_levl': 'int', table_dir[tables_name]: 'float'})
            data['value'] = data[table_dir[tables_name]]
        else:
            end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
            offset = dtt.timedelta(minutes=(-60*8))
            end = (end_time+offset).strftime('%Y%m%d%H')+"0000"
            start = (end_time+dtt.timedelta(minutes=(-60*(8+table_dir[tables_name])))).strftime('%Y%m%d%H')+"0000"
            label = "[" + start +","+ end +"]"
            client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
            interfaceId = "statSurfEleInRect"
            params = {
                'dataCode':"SURF_CHN_MUL_HOR",  #SURF_CHN_MUL_HOR
                'elements':"Station_Id_C",
                'statEles':"SUM_PRE_1h,MIN_Station_Name,MIN_Province,MIN_City,MIN_Cnty,MIN_Town,MIN_Station_levl,MIN_Lat,MIN_Lon,MIN_Alti",
                'timeRange':label,#"[20190810222138,20190811222138]",
                'minLat':str(boundary[0]),
                'minLon':str(boundary[2]),
                'maxLat':str(boundary[1]),
                'maxLon':str(boundary[3]),
                'eleValueRanges':"PRE_1h:(0,30000]",
               'limitCnt':"100000000"
            }
            result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
            result_json = json.loads(result)
            clomns = ['Station_Id_C','SUM_PRE_1h','Station_Name','Province','City','Cnty','Town','Station_levl','Lat','Lon','Alti']
            data = pd.DataFrame(result_json['DS'])
            data.columns = clomns
            data = data.astype({'Lat': 'float', 'Lon': 'float','Station_levl':'int','SUM_PRE_1h': 'float','Alti':'float'})
            data['value'] = data['SUM_PRE_1h']
        return data
    def get_wind(self,boundary,tables_name):
        # boundary 边界 value_index 数据类型  tables_name 数据字段  
        table_dir = {
            "6hours":["WIN_S_Inst_Max_6h","WIN_D_Inst_Max_6h"],
            "12hours":["WIN_S_Inst_Max_12h","WIN_D_Inst_Max_12h"]
        }
        elementstr = "Datetime,Station_Id_C,Station_Name,Province,City,Cnty,Town,Station_levl,Station_Type,Lat,Lon,Alti," + table_dir[tables_name][0]+","+table_dir[tables_name][1]
        eleValueRangestr = table_dir[tables_name][0] +  ":(0,1000)" + ";" +table_dir[tables_name][1] +  ":(0,1000)"
        end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
        offset = dtt.timedelta(minutes=(-60*8))
        label = (end_time+offset).strftime('%Y%m%d%H')+"0000"
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "getSurfEleInRectByTime"
        params = {
            'dataCode':"SURF_CHN_MUL_HOR",  #SURF_CHN_MUL_HOR
            'elements':elementstr,
            'times':label,
            'minLat':str(boundary[0]),
            'minLon':str(boundary[2]),
            'maxLat':str(boundary[1]),
            'maxLon':str(boundary[3]),
            'eleValueRanges':eleValueRangestr,
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = params['elements'].split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        reclomns = ['Datetime','Station_Id_C','Station_Name','Province','City','Cnty','Town','Station_levl','Station_Type','Lat','Lon','Alti','WIN_S_Gust_Max','WIN_D_Gust_Max']
        data.columns = reclomns
        data = data.astype({'Lat': 'float', 'Lon': 'float', 'Station_levl': 'int', 'WIN_S_Gust_Max': 'float', 'WIN_D_Gust_Max': 'float'})
        data['value'] = data['WIN_S_Gust_Max']
        return data  
    def get_temp(self,boundary,tables_name):
         # boundary 边界 value_index 数据类型  tables_name 数据字段   
        table_dir = {
            "tmax":"TEM_Max_24h",
            "tmin":"TEM_Min_24h",
            "tchange":"TEM_ChANGE_24h"
        }
        elementstr = "Datetime,Station_Id_C,Station_Name,Province,City,Cnty,Town,Station_levl,Station_Type,Lat,Lon,Alti," + table_dir[tables_name]
        eleValueRangestr = table_dir[tables_name] +  ":(0,100)"
        end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
        offset = dtt.timedelta(minutes=(-60*8))
        label = (end_time+offset).strftime('%Y%m%d%H')+"0000"
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "getSurfEleInRectByTime"
        params = {
            'dataCode':"SURF_CHN_MUL_HOR",  #SURF_CHN_MUL_HOR
            'elements':elementstr,
            'times':label,
            'minLat':str(boundary[0]),
            'minLon':str(boundary[2]),
            'maxLat':str(boundary[1]),
            'maxLon':str(boundary[3]),
            'eleValueRanges':eleValueRangestr,
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = params['elements'].split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        data = data.astype({'Lat': 'float', 'Lon': 'float', 'Station_levl': 'int', table_dir[tables_name]: 'float'})
        data['value'] = data[table_dir[tables_name]]
        return data
    def get_view(self,boundary,tables_name):
        table_dir = {
            "24hours":24,
            "12hours":12
        }
        end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
        offset = dtt.timedelta(minutes=(-60*8))
        end = (end_time+offset).strftime('%Y%m%d%H')+"0000"
        start = (end_time+dtt.timedelta(minutes=(-60*(8+table_dir[tables_name])))).strftime('%Y%m%d%H')+"0000"
        label = "[" + start +","+ end +"]"
        client = DataQueryClient(configFile=r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "statSurfEleInRect"
        params = {
            'dataCode':"SURF_CHN_MUL_HOR",  #SURF_CHN_MUL_HOR
            'elements':"Station_Id_C",
            'statEles':"MIN_VIS_Min,MIN_Station_Name,MIN_Province,MIN_City,MIN_Cnty,MIN_Town,MIN_Station_levl,MIN_Lat,MIN_Lon,MIN_Alti",
            'timeRange':label,#"[20190810222138,20190811222138]",
            'minLat':str(boundary[0]),
            'minLon':str(boundary[2]),
            'maxLat':str(boundary[1]),
            'maxLon':str(boundary[3]),
            'eleValueRanges':"VIS_Min:(0,30000]",
            'limitCnt':"100000000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = ['Station_Id_C','VIS_Min','Station_Name','Province','City','Cnty','Town','Station_levl','Lat','Lon','Alti']
        data = pd.DataFrame(result_json['DS'])
        data.columns = clomns
        data = data.astype({'Lat': 'float', 'Lon': 'float','Station_levl':'int','VIS_Min': 'float','Alti':'float'})
        data['value'] = data['VIS_Min']
        return data
    def single_data(self,value,station_id,plot_range):
        timelabel = plot_range
        client = DataQueryClient(configFile = r"/home/workspace/Data/My_Git/web_met/bak/zdz/common/utils/client.config")
        interfaceId = "getSurfEleByTimeRangeAndStaID"
        # 元素字典
        elem = {
            "rain":"Datetime,Station_Id_C,PRE_1h,Station_Name,Q_PRE_1h",# 降水
            "wind":"Datetime,Station_Id_C,WIN_S_Inst_Max,WIN_D_INST_Max,Station_Name,Q_WIN_S_Inst_Max,Q_WIN_D_Inst_Max", #风力 
            "sped":"Datetime,Station_Id_C,WIN_S_Inst_Max,WIN_D_INST_Max,Station_Name,Q_WIN_S_Inst_Max,Q_WIN_D_Inst_Max", #风力 
            "windave":"Station_Name,Station_Id_C,Datetime,WIN_D_Avg_1mi,WIN_S_Avg_1mi,Q11288,Q11289",
            "temp":"Datetime,Station_Id_C,TEM,Station_Name,Q_TEM",# 气温
            "tmax":"Datetime,Station_Id_C,TEM,Station_Name,Q_TEM",# 气温
            "tmin":"Datetime,Station_Id_C,TEM,Station_Name,Q_TEM",# 气温
            "dpt":"Station_Name,Station_Id_C,Datetime,DPT,Q12003",
            "view":"Datetime,Station_Id_C,VIS_HOR_1MI,Station_Name,Q_VIS_HOR_1MI",# 能见度
            "rhu":"Station_Name,Station_Id_C,Datetime,RHU,Q13003",
            "vap":"Station_Name,Station_Id_C,Datetime,VAP,Q13004",
            "Snow_Depth":"Station_Name,Station_Id_C,Datetime,Snow_Depth,Q13013",
            "prs":"Station_Name,Station_Id_C,Datetime,PRS,Q10004",
            "PRS_Sea": "Station_Name,Station_Id_C,Datetime,PRS_Sea,Q10051"        
        }
        rename = {
            "rain":"TTime,IIIII,Ri,StationName,verify",
            "temp":"TTime,IIIII,T,StationName,verify",
            "tmax":"TTime,IIIII,T,StationName,verify",
            "tmin":"TTime,IIIII,T,StationName,verify",
            "wind":"TTime,IIIII,fFy,dFy,StationName,verify1,verify2",
            "sped":"TTime,IIIII,fFy,dFy,StationName,verify1,verify2",
            "view":"TTime,IIIII,V,StationName,verify",
        }
        params = {
            'dataCode':"SURF_CHN_MUL_HOR",#SURF_CHN_MUL_HOR
            'elements':elem[value],  
            #'timeRange':"[20230118000000,20230119000000]",测试k8111的11级风
            'timeRange':timelabel,
            'staIds':station_id,
            'orderBy':"Datetime",
            'limitCnt':"50000"
        }
        result = client.callAPI_to_serializedStr(self.userId, self.pwd, interfaceId, params, self.dataFormat)
        result_json = json.loads(result)
        clomns = elem[value].split(",")
        data = pd.DataFrame(result_json['DS'],columns=clomns)
        data.columns = rename[value].split(",")
        data['TTime'] = pd.to_datetime(data['TTime'])
        data['TTime'] = (data['TTime']+ pd.to_timedelta(8, unit='H')).dt.strftime('%Y-%m-%d %H:%M:%S')
        return data
    def single_station(self,value,station_id):
        end_time = dtt.datetime.strptime(self.time, "%Y-%m-%d %H:%M:%S")
        offset = dtt.timedelta(minutes=(-60*8))
        end = (end_time+offset).strftime('%Y%m%d')+"120000"
        start = (end_time+dtt.timedelta(minutes=(-60*(8+24)))).strftime('%Y%m%d')+"120000"
        plot_range = "[" + start +","+ end +"]"
        data = self.single_data(value,station_id,plot_range)
        if value=="wind" :
            wind_time = data['TTime'].to_list()
            wind_dir = data['dFy'].to_list()
            wind_speed = data['fFy'].to_list()
        else:
            wind_time = []
            wind_dir = []
            wind_speed = []
        wind_data = pd.DataFrame(data={'TTime':wind_time,'dFy':wind_dir,'fFy':wind_speed})    
        now_wind = wind_data
        his_wind = wind_data  
        now_data = data
        his_data = data 
        history = json.dumps("none")#his_data.to_json(orient='values',force_ascii=False)
        windhis = json.dumps("none")#his_wind.to_json(orient='values',force_ascii=False)
        nowdata = now_data.to_json(orient='values',force_ascii=False) 
        windnow = now_wind.to_json(orient='values',force_ascii=False)      
        return nowdata,history,windhis,windnow

# ftp 雷达
class FTP_Radar(object):
    def __init__(self):
        """
        初始化ftp
        :param host: ftp主机ip
        :param username: ftp用户名
        :param password: ftp密码
        :param port: ftp端口 （默认21）
        """
        self.host = "172.21.158.216"
        self.username = "guest"
        self.password = "Guest2022"
        self.port = 21
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
    def to_redis(self,imd):
        imglist = [imd]
        data = {
            "imglist":imglist
        }
        self.rs.set("radar", pickle.dumps(data))  
    def get_radar(self):
        data = pickle.loads(self.rs.get("radar"))
        return data
    def ftp_connect(self):
        """
        连接ftp
        :return:
        """
        ftp = FTP()
        ftp.set_debuglevel(1) # 不开启调试模式
        ftp.connect(host=self.host, port=self.port) # 连接ftp
        ftp.login(self.username, self.password) # 登录ftp
        ftp.set_pasv(False)##ftp有主动 被动模式 需要调整 
        return ftp
    def download_file(self):
        """
        从ftp下载文件到本地
        :param ftp_file_path: ftp下载文件路径
        :param dst_file_path: 本地存放路径
        :return:
        """
        dst_file_path = "/home/workspace/Data/radar"
        download_list = []
        today = dtt.datetime.utcnow()
        ftp_file_path = today.strftime('%Y%m%d') +"/"
        buffer_size = 102400 #默认是8192
        ftp = self.ftp_connect()
        file_list = ftp.nlst(ftp_file_path)
        if os.path.isdir(dst_file_path+"/" + ftp_file_path):
            #print("文件夹存在")
            local_files = os.listdir(dst_file_path+"/" + ftp_file_path)
            if file_list[-1][9:] not in local_files:
                download_list.append(file_list[-1][9:])                
        else:
            #print("文件夹不存在就创建")
            os.makedirs(dst_file_path+"/" + ftp_file_path)
        for file_name in download_list: 
            ftp_file = os.path.join(ftp_file_path, file_name)
            write_file = dst_file_path+"/"+ftp_file_path + file_name ##在这里如果使用os.path.join 进行拼接的话 会丢失dst_file_path路径,与上面的拼接路径不一样
            with open(write_file, "wb") as f:        
                ftp.retrbinary('RETR %s' % ftp_file, f.write, buffer_size)
            f.close()
            time.sleep(0.5)
            self.plot_data(write_file)     
        ftp.quit()
    def plot_data(self,write_file):    
#         path ="/home/workspace/Data/My_Git/web_met/static/data/downfile/" 
#         f = cinrad.io.StandardData(path+'Z_RADR_I_Z9576_20230702105550_O_DOR_SAD_CAP_FMT.bin.bz2')
        plt.ioff()
        file_path = write_file
        f = cinrad.io.StandardData(write_file)
        rl = list(f.iter_tilt(230, 'REF'))
        v = f.get_data(0,230, 'REF')
        fg =cinrad.calc.GridMapper([v])
        output = fg(0.01)
        colorslist = ['#00C800','#019000','#FFFF00','#E7C000','#FF9000','#D60000','#C00000','#FF00F0','#780084','#AD90F0','#AE0AF5']# 组合反射率
        levels1 = [20,25,30,35,40,45,50,55,60,65,70,75]
        cmaps = LinearSegmentedColormap.from_list('mylist',colorslist,N=10)
        contour = plt.contourf(output.longitude, output.latitude, output.REF,cmap=cmaps,levels = levels1)
        # contour = plt.contourf(data.longitude, data.latitude, data,cmap=cmaps,levels = levels1)
        geojson = geojsoncontour.contourf_to_geojson(
            contourf=contour,
            ndigits=3,
            unit='dbz'
        )
        self.to_redis(geojson)
        self.to_vcs(rl)
        plt.axis('off') 
        plt.close()  
    def del_file(self):
        today = dtt.datetime.utcnow()
        offset = dtt.timedelta(days=-10)
        delday = (today + offset).strftime('%Y%m%d')
        path = "/home/workspace/Data/radar/" + delday
        if os.path.isdir(path): 
            shutil.rmtree(path)
    def to_vcs(self,rl):
        vcs = cinrad.calc.VCS(rl)
        data = {
            "vcs":vcs
        }
        self.rs.set("radar_vsc", pickle.dumps(data))
    def get_vcs(self):
        data = pickle.loads(self.rs.get("radar_vsc"))
        return data['vcs']
    def plot_sec(self,start,end):
        #fig = plt.figure(figsize=(16, 8))
        vcs = self.get_vcs()
        sec = vcs.get_section(start_cart=(start[0], start[1]), end_cart=(end[0], end[1])) 
        Section(sec)
        buffer = BytesIO()
        plt.savefig(buffer,bbox_inches='tight')  
        plot_img = buffer.getvalue()
        imb = base64.b64encode(plot_img) 
        ims = imb.decode()
        imd = "data:image/png;base64,"+ims
        plt.close()
        return imd
# 同步ec
class FTP_Ec(object):
    def __init__(self):
        """
        初始化ftp
        :param host: ftp主机ip
        :param username: ftp用户名
        :param password: ftp密码
        :param port: ftp端口 （默认21）
        """
        self.host = "10.137.4.59"
        self.username = "tzqxt"
        self.password = "Tzqxt@2022"
        self.port = 21
    def ftp_connect(self):
        """
        连接ftp
        :return:
        """
        ftp = FTP()
        ftp.set_debuglevel(1) # 不开启调试模式
        ftp.connect(host=self.host, port=self.port) # 连接ftp
        ftp.login(self.username, self.password) # 登录ftp
        ftp.set_pasv(False)##ftp有主动 被动模式 需要调整 
        return ftp
    def download_file(self):
        """
        从ftp下载文件到本地
        :param ftp_file_path: ftp下载文件路径
        :param dst_file_path: 本地存放路径
        :return:
        """
        dst_file_path = "/home/workspace/Data/ec"
        download_list = []
        now = dtt.datetime.now()
        offset = dtt.timedelta(days=-1)
        today = (now + offset)
        ftp_file_path = "/home/data/output/fruit/nc_ecf/"+today.strftime('%Y.%m') +"/" +today.strftime('%Y%m%d')+"12" +"/"
        local_file_path = today.strftime('%Y%m%d')+"12" +"/"
        buffer_size = 102400 #默认是8192
        ftp = self.ftp_connect()
        file_list = ftp.nlst(ftp_file_path)
        #print(file_list) 
        if os.path.isdir(dst_file_path+"/" + local_file_path):
            #print("文件夹存在")
            local_files = os.listdir(dst_file_path+"/" +  local_file_path)
            for download in file_list:
                if download[50:] not in local_files:
                    download_list.append(download[50:])                
        else:
            #print("文件夹不存在就创建")
            os.makedirs(dst_file_path+"/" + local_file_path) 
        for file_name in download_list: 
            ftp_file = os.path.join(ftp_file_path, file_name)
            write_file = dst_file_path+"/"+local_file_path + file_name ##在这里如果使用os.path.join 进行拼接的话 会丢失dst_file_path路径,与上面的拼接路径不一样
            with open(write_file, "wb") as f:        
                ftp.retrbinary('RETR %s' % ftp_file, f.write, buffer_size)
            f.close()
        ftp.quit()
    def del_file(self):
        today = dtt.datetime.utcnow()
        offset = dtt.timedelta(days=-10)
        delday = (today + offset).strftime('%Y%m%d')+"12"
        path = "/home/workspace/Data/ec/" + delday
        if os.path.isdir(path): 
            shutil.rmtree(path)
    def comput_va(self,path,filelist):
        lat = 28.5
        lon = 121.91
        lev = 1000
        # 计算参数
        rlist = []
        tlist = []
        speedlist = []
        # 开始读取数据
        for i in filelist:
            f = xr.open_dataset(path+i)
            # 湿度
            rlist.append(round(float(f.r.sel(lonP=lon, latP=lat,lev=1000,method='nearest').data),2)) 
            # 气温
            tlist.append(round(float(f.t2.sel(lonS=lon, latS=lat,method='nearest').data),2))
            # 风力
            u = round(float(f.u10.sel(lonS=lon, latS=lat,method='nearest').data),2) 
            v = round(float(f.v10.sel(lonS=lon, latS=lat,method='nearest').data),2) 
            speedlist.append(round(math.sqrt(u**2+v**2),2))
            # 
            del f
        single = {
            "temp":round(sum(tlist)/len(tlist),2),
            "wind":round(sum(speedlist)/len(speedlist),2),
            "rh":round(sum(rlist)/len(rlist)/100,2),
            "fy":round(max(speedlist),2),
            "sun":9.5,
            "result":1
        }
        return single
    def return_daily_var(self):
        now = dtt.datetime.now()
        start = (now+dtt.timedelta(days=-1)).strftime('%Y%m%d')+"12/"
        path = "/home/workspace/Data/ec/" + start
        files = os.listdir(path)
        dayone = ['003','006','009','012','015','018','024']
        daytwo = ['027','030','033','036','039','042','045','048']
        onefile = []
        twofile = []
        for i in dayone:
            vaildstr = 'ecfine.I*.'+ i+".*.nc"
            for file in files:
                if fnmatch.fnmatch(file,vaildstr):
                    onefile.append(file) 
        for i in daytwo:
            vaildstr = 'ecfine.I*.'+ i+".*.nc"
            for file in files:
                if fnmatch.fnmatch(file,vaildstr):
                    twofile.append(file)
        today = self.comput_va(path,onefile)
        nextday = self.comput_va(path,twofile)
        today["time"] = (now+dtt.timedelta(days=0)).strftime('%m-%d')
        nextday["time"] = (now+dtt.timedelta(days=1)).strftime('%m-%d')
        data = [today,nextday]
        return data


# 数据加载    
class server_plot():
    def __init__(self,time_hours,city,plot_type,js_status,recv_data):
        self.start = "2023-09-18 19:30:25"
        self.end = "2023-09-20 19:30:25"
        self.time_hours = float(time_hours)
        self.city = city
        self.plot_type = plot_type
        self.js_status = js_status
        self.recv_data = recv_data
        self.max = None
        self.min = None
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"
        self.shp_path = "static/data/shpfile/xian/"
        self.max = None
        self.min = None
        self.rain = None
        self.wind = None
        self.verify = self.get_city_code()
    def get_city_code(self):
        data = pd.read_csv("static/data/city_code.csv")
        target = data[data['code']==self.city]
        return target.iloc[0].to_dict()
    def wind_from_sql(self):
        start_time = dtt.datetime.strptime(self.start, "%Y-%m-%d %H:%M:%S")
        end_time = dtt.datetime.strptime(self.end, "%Y-%m-%d %H:%M:%S")
        offset = dtt.timedelta(minutes=(-60*8))
        now = (end_time+offset).strftime('%Y-%m-%d %H:%M:%S')
        old = (start_time+offset).strftime('%Y-%m-%d %H:%M:%S')
        sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
            max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(wind) as wind
            from table_station_hour 
            where Datetime between '{start_times}' and '{end_times}' and wind>0 and City='台州市'
            group by Station_Id_C""" 
        rsql = sql.format(start_times=old,end_times=now)
        data = pd.read_sql(rsql, con=self.conn)
        data['WIN_S_Gust_Max'] = data.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
        data['WIN_D_Gust_Max'] = data.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
        data['value'] = data['WIN_S_Gust_Max']
        self.city_info[self.city]['data']['平均'] = round(data['WIN_S_Gust_Max'].mean(),1)
        self.city_info[self.city]['data']['最大'] = str(round(max(data['WIN_S_Gust_Max']),1))      
        return data
    def rain_from_redis(self):
        redis_dir = {
            24:"rain24",
            12:"rain12",
            6:"rain6",
            3:"rain3",
            1:"rain1",
            0.1:"warring_zdz"
        }
        data = pickle.loads(self.rs.get(redis_dir[self.time_hours]))
        if self.time_hours !=0.1:
            data = data.astype({'Lat': 'float', 'Lon': 'float','PRE': 'float'}) 
            data = data[(data['PRE']>=0) &(data['PRE']<5000)]
            data['value'] = data['PRE']
        else:
            data = data.astype({'Lat': 'float', 'Lon': 'float','PRE': 'float','WIN_S_Inst_Max': 'float', 'WIN_D_INST_Max': 'float','TEM':'float','VIS_HOR_1MI':'float'})
            #data = data[(data['PRE']>=0) &(data['PRE']<5000)]
            rain = data[(data['PRE']>=0)&(data['PRE']<5000)][['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','PRE']]
            data = rain.groupby(['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['PRE'].sum().reset_index()
            data['value'] = data['PRE'] 
        #print("数据",data[data['Station_Id_C']=='K8422'])
        self.rain = data
        return data
    def color_map(self):
        if self.plot_type=="rain":
            hours = self.time_hours
            if hours >12:
                colorslist = ['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 24降水
                levels = [-1,0.01, 5, 10, 15, 20, 25, 35, 50, 75, 100, 150, 200, 250, 350, 500]
                cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
                norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N)  # 基于离散区间生成颜色映射索引
            elif hours <=12 and hours >=1:
                colorslist =['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 06降水
                levels = [-1,3, 4, 5, 10, 15, 20, 25, 35, 50, 60, 70, 80, 90, 100, 110]
                cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
                norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N)  # 基于离散区间生成颜色映射索引       
            elif hours <1:
                colorslist =['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 01降水
                levels = [-1,0.01, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13, 15, 17, 20]
                cmap_nonlin = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
                norm = mpl.colors.BoundaryNorm(levels, cmap_nonlin.N)  # 基于离散区间生成颜色映射索引 
        return cmap_nonlin,levels
    def decode_xarray(self):
        if self.plot_type=="rain":
            data = self.rain_from_redis()
            self.recv_data = data
        lat = np.array(data['Lat'].to_list())
        lon = np.array(data['Lon'].to_list())
        Zi = np.array(data['value'].to_list())
        data_max = max(Zi)
        data_min = min(Zi)
        self.max = data_max
        self.min = data_min
        np.set_printoptions(precision = 2)
        x = np.arange(self.verify['lon0'],self.verify['lon1'],0.01)
        y = np.arange(self.verify['lat0'],self.verify['lat1'],0.01)
        nx0 =len(x)
        ny0 =len(y)
        X, Y = np.meshgrid(x, y)#100*100
        P = np.array([X.flatten(), Y.flatten() ]).transpose()    
        Pi =  np.array([lon, lat ]).transpose()
        Z_linear = griddata(Pi, Zi, P, method = "nearest").reshape([ny0,nx0])
        gauss_kernel = Gaussian2DKernel(0.8)
        smoothed_data_gauss = convolve(Z_linear, gauss_kernel)
        data_xr = xr.DataArray(smoothed_data_gauss, coords=[ y,x], dims=["lat", "lon"])
        return data_xr
    def wind_to_json(self):
        redis_dir = {
            24:24*60,
            12:12*60,
            6:6*60,
            3:3*60,
            1:60,
            0.1:10
        }
        if redis_dir[self.time_hours]!=10:
            end_date = dtt.datetime.now()
            start_date = (end_date-dtt.timedelta(minutes=(redis_dir[self.time_hours])))
            offset = dtt.timedelta(minutes=(-60*8))
            now = (end_date+offset).strftime('%Y-%m-%d %H:%M:%S')
            old = (start_date+offset).strftime('%Y-%m-%d %H:%M:%S')
            sql = """select max(City) as City,max(Cnty) as Cnty, Station_Id_C , max(Province) as Province,max(Station_levl) as Station_levl,
                max(Station_Name) as Station_Name, max(Town) as Town, max(Alti) as Alti, max(Lat) as Lat,max(Lon) as Lon, max(wind) as wind
                from table_station_hour 
                where Datetime between '{start_times}' and '{end_times}' and wind>0 and Province='浙江省'
                group by Station_Id_C""" 
            rsql = sql.format(start_times=old,end_times=now)
            data = pd.read_sql(rsql, con=self.conn)
            data['WIN_S_Gust_Max'] = data.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
            data['WIN_D_Gust_Max'] = data.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
            data['value'] = data['WIN_S_Gust_Max']
            wind = data[(data['value']>17.1)&(data['value']<100)]
        else:
            data = pickle.loads(self.rs.get("warring_zdz"))
            data = data.astype({'Lat': 'float', 'Lon': 'float','PRE': 'float','WIN_S_Inst_Max': 'float', 'WIN_D_INST_Max': 'float','TEM':'float','VIS_HOR_1MI':'float'})
            data['WIN_S_Gust_Max'] = data['WIN_S_Inst_Max']
            data['WIN_D_Gust_Max'] = data['WIN_D_INST_Max']
            data['value'] = data['WIN_S_Gust_Max']
            wind = data[(data['value']>17.1)&(data['value']<100)]
        # if self.recv_data =="province":
        #     wind = wind
        if self.recv_data =="none":
            wind = wind[wind['City']=="台州市"]
        self.wind = wind
        return wind
    def plot_rain(self):
        wind = self.wind_to_json()
        wind_json = wind.to_json(orient = "records", force_ascii=False)
        data_xr = self.decode_xarray()
        cmaps ,norm = self.color_map()
        lat = data_xr.lat
        lon = data_xr.lon
        lons, lats = np.meshgrid(lon, lat)
        contour = plt.contourf(lons,lats,data_xr,cmap=cmaps,levels = norm)
        geojson = geojsoncontour.contourf_to_geojson(
            contourf=contour,
            ndigits=3,
            unit='mm'
        )
        plt.close()
        return geojson,wind_json    
    def text_wind_rain(self):
        wind = self.wind
        rain = self.rain
        text = ""
        # rain = pd.read_csv("rian1.csv")
        # wind = pd.read_csv("wind1.csv")
        if self.time_hours ==0.1:   
            text_rain = "目前:"
            if self.verify['city_type']=='city':
                rain_tz = rain[rain['City']==self.verify['name']]
            elif self.verify['city_type']=='cnty':
                rain_tz = rain[rain['Cnty']==self.verify['name']]
            elif self.verify['city_type']=='province':
                rain_tz = rain[rain['Province']==self.verify['name']]
            bins = [0,5,10,100]
            labels = [0,1,2]
            rain_tz['rank']=pd.cut(rain_tz['PRE'],bins,right=False,labels=labels)
            rain_max = rain_tz.sort_values(by="PRE",ascending=False).head(3)
            raintop = rain_max.head(1)
            if raintop['PRE'].values[0]>0.5: 
                text_rain = text_rain + raintop['Cnty'].values[0] + raintop['Town'].values[0] + "五分钟降水为" + str(raintop['PRE'].values[0]) +"毫米。"
                if len(rain_tz[rain_tz['rank']==1])>0:
                    indextext = "单站雨量较大的有："
                    for index,row in rain_max.iterrows():
                        if row['PRE']>0.5:
                            indextext = indextext + row['Cnty'] + row['Station_Name'] + str(row['PRE']) + "毫米，"
                    indextext = indextext[:-1] + "。"
                    text_rain = text_rain + indextext
                    if len(rain_tz[rain_tz['rank']==2])>0:
                        text_rain = text_rain + "其中超过10毫米的站有" + str(len(rain_tz[rain_tz['rank']==2])) + "个；"          
            else:
                text_rain = ""
        elif self.time_hours ==1:
            text_rain = "目前:"
            if self.verify['city_type']=='city':
                rain_tz = rain[rain['City']==self.verify['name']]
            elif self.verify['city_type']=='cnty':
                rain_tz = rain[rain['Cnty']==self.verify['name']]
            elif self.verify['city_type']=='province':
                rain_tz = rain[rain['Province']==self.verify['name']]
            bins = [0,10,20,50,80,100,5000]
            labels = [0,1,2,3,4,5]
            rain_tz['rank']=pd.cut(rain_tz['PRE'],bins,right=False,labels=labels)
            rain_max = rain_tz.sort_values(by="PRE",ascending=False).head(3)
            raintop = rain_max.head(1)
            town_max = rain_tz.groupby(['Town','Cnty'])['PRE'].max().sort_values(ascending=False).head(3).to_frame() 
            if raintop['PRE'].values[0]>0.5: 
                text_rain = text_rain + raintop['Cnty'].values[0] + raintop['Town'].values[0] + "近一小时降水为" + str(raintop['PRE'].values[0]) +"毫米。"
                if len(rain_tz[rain_tz['rank']>=0])>0:
                    indextext = "单站雨量较大的有："
                    for index,row in rain_max.iterrows():
                        if row['PRE']>0:
                            indextext = indextext + row['Cnty'] + row['Station_Name'] + str(row['PRE']) + "毫米，"    
                    indextext = indextext[:-1] + "。"
                    text_rain = text_rain + indextext
                    if len(rain_tz[rain_tz['rank']>1])>0:
                        indextext_town = "单站雨量较大的乡镇有：" 
                        for index,row in town_max.iterrows():
                            if row['PRE']>0.5:
                                indextext_town = indextext_town + index[1] + index[0] + str(row['PRE']) + "毫米," 
                        indextext_town = indextext_town[:-1] + "。"
                        text_rain = text_rain + indextext_town                            
        elif self.time_hours ==3:
            text_rain = "目前:"
            if self.verify['city_type']=='city':
                rain_tz = rain[rain['City']==self.verify['name']]
            elif self.verify['city_type']=='cnty':
                rain_tz = rain[rain['Cnty']==self.verify['name']]
            elif self.verify['city_type']=='province':
                rain_tz = rain[rain['Province']==self.verify['name']]
            bins = [0,30,50,80,100,5000]
            labels = [0,1,2,3,4]
            rain_tz['rank']=pd.cut(rain_tz['PRE'],bins,right=False,labels=labels)
            rain_max = rain_tz.sort_values(by="PRE",ascending=False).head(3)
            raintop = rain_max.head(1)
            town = rain_tz.groupby(['Town','Cnty'])['PRE'].mean().head(5).sort_values(ascending=False).to_frame() 
            if raintop['PRE'].values[0]>0: 
                text_rain = text_rain + raintop['Cnty'].values[0] + raintop['Town'].values[0] + "近三小时降水为" + str(raintop['PRE'].values[0]) +"毫米。"
                if raintop['PRE'].values[0]>20:
                    indextext_town = "乡镇面雨量较大的有：" 
                    for index,row in town.iterrows():
                        if row['PRE']>0.5:
                            indextext_town = indextext_town + index[1] + index[0] + str(round(row['PRE'],1)) + "毫米," 
                    indextext_town = indextext_town[:-1] + "。"
                    text_rain = text_rain + indextext_town 
                    if raintop['PRE'].values[0]>50:
                        town_max_text = "其中，出现暴雨及以上的乡镇有："
                        town_max = rain_tz.groupby(['Town','Cnty'])['PRE'].max().sort_values(ascending=False).to_frame()
                        town_nums = town_max[town_max['PRE']>=50]
                        for index,row in town_nums.iterrows():
                            town_max_text = town_max_text + index[1] + index[0] + ","
                        town_max_text = town_max_text[:-1] + "。" 
                        text_rain = text_rain + town_max_text 
        else: 
            if self.verify['city_type']=='city':
                rain_tz = rain[rain['City']==self.verify['name']]
                average = rain_tz.groupby(['Cnty'])['PRE'].mean().to_frame().sort_values(by="PRE",ascending=False)
            elif self.verify['city_type']=='cnty':
                rain_tz = rain[rain['Cnty']==self.verify['name']]
                average = rain_tz.groupby(['Town'])['PRE'].mean().to_frame().sort_values(by="PRE",ascending=False)
            elif self.verify['city_type']=='province':
                rain_tz = rain[rain['Province']==self.verify['name']]
                average = rain_tz.groupby(['City'])['PRE'].mean().to_frame().sort_values(by="PRE",ascending=False)
            city_mean = round(rain_tz[rain_tz['PRE']>0]['PRE'].mean(),1)
            bins = [0,30,50,80,100,5000]
            labels = [0,1,2,3,4]
            rain_tz['rank']=pd.cut(rain_tz['PRE'],bins,right=False,labels=labels)
            rain_max = rain_tz.sort_values(by="PRE",ascending=False).head(3)
            raintop = rain_max.head(1)
            text_rain = "目前:全市面雨量为" + str(city_mean) + "毫米。"
            text_cnty_average = "雨量较大的有："
            for index,row in average.iterrows():
                if row['PRE']>0.1:
                    text_cnty_average = text_cnty_average + index + str(round(row['PRE'],1)) + "毫米,"  
            text_cnty_average = text_cnty_average[:-1] + "。"
            text_rain = text_rain + text_cnty_average
            if raintop['PRE'].values[0]>0: 
                text_rain = text_rain +"其中，单站累计最大"+ raintop['Cnty'].values[0] + raintop['Town'].values[0] + str(raintop['PRE'].values[0]) +"毫米。"        
        # 风力的统计
        if self.verify['city_type']=='city':
            wind = wind[(wind['value']>17.2)&(wind['City']==self.verify['name'])].sort_values(by="value",ascending=False)
        elif self.verify['city_type']=='cnty':
            wind = wind[(wind['value']>17.2)&(wind['Cnty']==self.verify['name'])].sort_values(by="value",ascending=False)  
        elif self.verify['city_type']=='province':
            wind = wind[(wind['value']>17.2)&(wind['Province']==self.verify['name'])].sort_values(by="value",ascending=False)
        #wind = wind[(wind['value']>10)&(wind['value'])].sort_values(by="value",ascending=False)
        wind_text = ""
        if len(wind)>0:
            wind_text = wind_text + "全市出现8级以上大风，风力较大的有"
            text_maxwind = ""
            if len(wind)>3:
                wind = wind.head(3)
            else:
                wind = wind
            for index,row in wind.iterrows():
                text_maxwind = text_maxwind + row['Cnty'] + row['Town'] + "-"+ row['Station_Name'] + str(row['value']) +"m/s,"
            text_maxwind = text_maxwind[:-1] + "。"
            wind_text =  wind_text + text_maxwind  
        text = text + text_rain + wind_text
        return text
    def return_province(self):
        data = self.rain_from_redis()
        lat = np.array(data['Lat'].to_list())
        lon = np.array(data['Lon'].to_list())
        Zi = np.array(data['value'].to_list())
        np.set_printoptions(precision = 2)
        x = np.arange(118.0,123.0,0.01)
        y = np.arange(26,31,0.01)
        nx0 =len(x)
        ny0 =len(y)
        X, Y = np.meshgrid(x, y)#100*100
        P = np.array([X.flatten(), Y.flatten() ]).transpose()    
        Pi =  np.array([lon, lat ]).transpose()
        Z_linear = griddata(Pi, Zi, P, method = "nearest").reshape([ny0,nx0])
        gauss_kernel = Gaussian2DKernel(0.8)
        smoothed_data_gauss = convolve(Z_linear, gauss_kernel)
        data_xr = xr.DataArray(smoothed_data_gauss, coords=[ y,x], dims=["lat", "lon"])
        cmaps ,norm = self.color_map()
        lat = data_xr.lat
        lon = data_xr.lon
        lons, lats = np.meshgrid(lon, lat)
        contour = plt.contourf(lons,lats,data_xr,cmap=cmaps,levels = norm)
        geojson = geojsoncontour.contourf_to_geojson(
            contourf=contour,
            ndigits=3,
            unit='mm'
        )
        plt.close()
        # 风力
        wind = self.wind_to_json()
        wind_json = wind.to_json(orient = "records", force_ascii=False)
        # 文字
        rain = data[data['Province']=="浙江省"]
        rain_max = rain.sort_values(by="PRE",ascending=False).head(3)
        raintop = rain_max.head(1)
        text_rain = ""
        if raintop['PRE'].values[0]>0: 
            text_rain = text_rain +"目前" + raintop['City'].values[0] + raintop['Cnty'].values[0] + raintop['Town'].values[0] +raintop['Station_Name'].values[0] +"出现"+ str(raintop['PRE'].values[0]) +"毫米的降水。"
        average = rain.groupby(['City'])['PRE'].mean().to_frame().sort_values(by="PRE",ascending=False)
        text_cnty_average = "各市面雨量较大的有："
        for index,row in average.iterrows():
            if row['PRE']>0.2:
                text_cnty_average = text_cnty_average + index + str(round(row['PRE'],1)) + "毫米,"  
        text_cnty_average = text_cnty_average[:-1] + "。"
        text_rain = text_rain + text_cnty_average
        wind = wind[wind['value']>10].sort_values(by="value",ascending=False)
        text = ""
        wind_text = ""
        if len(wind)>0:
            wind_text = wind_text + "全省出现8级以上大风，风力较大的有"
            text_maxwind = ""
            if len(wind)>3:
                wind = wind.head(3)
            else:
                wind = wind
            for index,row in wind.iterrows():
                text_maxwind = text_maxwind + row['Cnty'] + row['Town'] + "-"+ row['Station_Name'] + str(row['value']) +"m/s,"
            text_maxwind = text_maxwind[:-1] + "。"
            wind_text =  wind_text + text_maxwind  
        text = text + text_rain + wind_text
        return geojson,text,wind_json



# 日历
class clander:
    def __init__(self,city,click_type):
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.city = city
        self.type = click_type
    def get_clander(self):
        now = dtt.datetime.utcnow()
        times = (now+dtt.timedelta(days=-31)).strftime('%Y-%m-%d ')+"23:00:00"
        sql = """select * from taizhou_clander where Datetime >= '{times}' """ 
        rsql = sql.format(times=times)
        data = pd.read_sql(rsql, con=self.conn)
        data['Datetime'] = data['Datetime'].dt.strftime('%Y-%m-%d')
        data_json = data.to_json(orient = "values")
        return data_json
    def get_daily(self,times):
        # data = pd.read_csv("static/data/downfile/daily.csv")
        # data = data[(data['rain']<5000)&(data['Datetime']=='2023-10-01 23:00:00')]
        sql = """select * from taizhou_daily where Datetime like '{times}' """ 
        rsql = sql.format(times=times)
        data = pd.read_sql(rsql, con=self.conn)
        data =data[data['rain']<5000]
        return data
    def plot_rain(self,times):
        data = self.get_daily(times)
        data['value'] = data['rain']
        lat = np.array(data['Lat'].to_list())
        lon = np.array(data['Lon'].to_list())
        Zi = np.array(data['value'].to_list())
        np.set_printoptions(precision = 2)
        x = np.arange(118.0,123.0,0.01)
        y = np.arange(26,31,0.01)
        nx0 =len(x)
        ny0 =len(y)
        X, Y = np.meshgrid(x, y)#100*100
        P = np.array([X.flatten(), Y.flatten() ]).transpose()    
        Pi =  np.array([lon, lat ]).transpose()
        Z_linear = griddata(Pi, Zi, P, method = "nearest").reshape([ny0,nx0])
        gauss_kernel = Gaussian2DKernel(0.8)
        smoothed_data_gauss = convolve(Z_linear, gauss_kernel)
        data_xr = xr.DataArray(smoothed_data_gauss, coords=[ y,x], dims=["lat", "lon"])
        colorslist = ['#FFFFFF','#A3FAFD', '#29D3FD', '#29AAFF', '#2983FF', '#4EAB37', '#46FA35', '#F1F837', '#F1D139', '#F2A932', '#F13237', '#C4343A', '#A43237', '#A632B4', '#D032E1', '#E431FF']# 24降水
        levels = [-1,0.01, 5, 10, 15, 20, 25, 35, 50, 75, 100, 150, 200, 250, 350, 500]
        cmaps = mpl.colors.ListedColormap(colorslist)  # 自定义颜色映射 color-map
        norm = mpl.colors.BoundaryNorm(levels, cmaps.N)  # 基于离散区间生成颜色映射索引
        lat = data_xr.lat
        lon = data_xr.lon
        lons, lats = np.meshgrid(lon, lat)
        contour = plt.contourf(lons,lats,data_xr,cmap = cmaps,norm = norm ,levels = levels)
        geojson = geojsoncontour.contourf_to_geojson(
            contourf=contour,
            ndigits=3,
            unit='mm'
        )
        plt.close()
        # 风力
        # wind = data
        wind = data[data['wind']>0]
        wind['WIN_S_Gust_Max'] = wind.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
        wind['WIN_D_Gust_Max'] = wind.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
        wind_json = wind.to_json(orient = "records", force_ascii=False)
        return geojson,wind_json
    def return_text(self,times):
        data = self.get_daily(times)
        rain_max = data.sort_values(by="rain",ascending=False).head(3)
        raintop = rain_max.head(1)
        text_rain = ""
        if raintop['rain'].values[0]>0: 
            text_rain = text_rain +"近24小时内" + raintop['City'].values[0] + raintop['Cnty'].values[0] + raintop['Town'].values[0] +raintop['Station_Name'].values[0] +"出现"+ str(raintop['rain'].values[0]) +"毫米的降水。"
        average = data.groupby(['Cnty'])['rain'].mean().to_frame().sort_values(by="rain",ascending=False)
        text_cnty_average = "各市面雨量较大的有："
        for index,row in average.iterrows():
            if row['rain']>0:
                text_cnty_average = text_cnty_average + index + str(round(row['rain'],2)) + "毫米,"  
        text_cnty_average = text_cnty_average[:-1] + "。"
        text_rain = text_rain + text_cnty_average
        wind = data[data['wind']>0]
        wind['WIN_S_Gust_Max'] = wind.apply(lambda x: (x.wind - int(str(int(x.wind))[-3:]))/10000, axis = 1)
        wind['WIN_D_Gust_Max'] = wind.apply(lambda x: int(str(int(x.wind))[-3:]), axis = 1)
        wind['value'] = wind['WIN_S_Gust_Max'] 
        wind = wind[wind['value']>10].sort_values(by="value",ascending=False)
        text = ""
        wind_text = ""
        if len(wind)>0:
            wind_text = wind_text + "全市出现8级以上大风，风力较大的有"
            text_maxwind = ""
            if len(wind)>3:
                wind = wind.head(3)
            else:
                wind = wind
            for index,row in wind.iterrows():
                text_maxwind = text_maxwind + row['Cnty'] + row['Town'] + "-"+ row['Station_Name'] + str(row['value']) +"m/s,"
            text_maxwind = text_maxwind[:-1] + "。"
            wind_text =  wind_text + text_maxwind  
        text = text + text_rain + wind_text
        return text


# 报警程序的模块
class warring_alert():
    def __init__(self,rain_type):
        self.rs = redis.Redis(host='127.0.0.1', port=6379,password="tzqxj58660")
        self.conn = pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="tzqxj58660",db="ZJSZDZDB")
        self.userId = "BEHZ_TZSJ_TZSERVICE" 
        self.pwd = "Liyuan3970!@" 
        self.dataFormat = "json"   
        self.rain_type = rain_type
        self.now = self.get_latest()
    def get_rain(self):
        rain_dir = {
            "5min":"warring_zdz",
            "rain1":"rain1",
            "rain3":"rain3",
            "rain6":"rain6",
            "rain12":"rain12",
            "rain24":"rain24"        
        }
        if self.rain_type =="5min":
            data = self.now
            rain = data[(data['PRE']>0)&(data['PRE']<5000)][['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','PRE']]
            rain_data = rain.groupby(['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['PRE'].sum().reset_index()
        else:
            value = self.rs.get(rain_dir[self.rain_type])
            rain_data = pickle.loads(value)
        return rain_data
    def get_latest(self):
        value = self.rs.get("warring_zdz")
        data = pickle.loads(value)
        data = data.astype({'Lat': 'float', 'Lon': 'float','PRE': 'float','WIN_S_Inst_Max': 'float', 'WIN_D_INST_Max': 'float','TEM':'float','VIS_HOR_1MI':'float'})
        return data        
    def get_wind(self):
        data = self.now
        wind = data[(data['WIN_S_Inst_Max']>17)&(data['WIN_S_Inst_Max']<5000)][['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','WIN_S_Inst_Max','WIN_D_INST_Max']]
        wind_data = wind.groupby(['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','WIN_D_INST_Max'])['WIN_S_Inst_Max'].max().reset_index().sort_values('WIN_S_Inst_Max', ascending=False).drop_duplicates(subset=['Station_Id_C'], keep='first')
        return wind_data
    def get_view(self):
        data = self.now
        view = data[(data['VIS_HOR_1MI']<1000)&(data['VIS_HOR_1MI']<30000)][['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti','VIS_HOR_1MI']]
        view_data = view.groupby(['Cnty','Province','Town','Station_Name','City','Station_Id_C','Lat','Lon','Alti'])['VIS_HOR_1MI'].min().reset_index().to_json(orient = "records", force_ascii=False)
        return view_data
    def get_radar(self):
        '''获取雷达数据'''
        img = pickle.loads(self.rs.get("radar"))
        return img
    def warring_data(self):
        # 开始编写风雨数据模型
#         radar = self.get_radar()
        rain = self.get_rain()
        wind = self.get_wind()
        view_data = self.get_view()
        rain_data = rain.to_json(orient = "records", force_ascii=False)
        wind_data = wind.to_json(orient = "records", force_ascii=False)
        return rain_data,wind_data,view_data