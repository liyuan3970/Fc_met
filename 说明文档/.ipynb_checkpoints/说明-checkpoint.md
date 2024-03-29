# 台州市气象快报统计系统
2022.4.4编辑数据

# 需要的三方包
pip install -r requirements.txt
碰到安装错误的包，可以使用conda安装

# 本地项目启动需要输入
redis-server
service mysql start


# 初始化数据库
python manage.py makemigrations zdz
python manage.py migrate

# 本地数据库创建同步表
'''  CREATE TABLE `station_data` (
  `datetime` datetime(6) NOT NULL COMMENT '时间',
  `station_no` char(5) NOT NULL COMMENT '站点编号（IIiii）',
  `station_province` char(20) DEFAULT NULL COMMENT '省（province）',
  `station_city` char(20) DEFAULT NULL COMMENT '市（city）',
  `station_county` char(20) DEFAULT NULL COMMENT '县（county）',
  `station_town` char(20) DEFAULT NULL COMMENT '乡（town）',
  `station_village` char(20) DEFAULT NULL COMMENT '街道（village）',
  `station_country` char(20) DEFAULT NULL COMMENT '国家（country）',
  `station_name` varchar(20) NOT NULL COMMENT '站点名称',
  `lon` decimal(10,6) NOT NULL COMMENT '经度',
  `lat` decimal(10,6) NOT NULL COMMENT '纬度',
  `station_type` int NOT NULL COMMENT '1：国家站；2：省站',
  `p_total` int DEFAULT NULL COMMENT '累计降水',
  `t_max` int DEFAULT NULL COMMENT '最高温度',
  `t_min` int DEFAULT NULL COMMENT '最低温度',
  `w_max` int DEFAULT NULL COMMENT '最大风速',
  `w_dir` int DEFAULT NULL COMMENT '风向',
  `vis` int DEFAULT NULL COMMENT '能见度',
  PRIMARY KEY (`datetime`,`station_no`) USING BTREE,
  KEY `station_no_index` (`station_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `table_station_min` (
  `Datetime` datetime(6) NOT NULL,
  `Station_Id_C` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
  `Cnty` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Province` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Town` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Station_levl` int DEFAULT NULL,
  `Station_Name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `City` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Lat` decimal(8,4) DEFAULT NULL,
  `Lon` decimal(8,4) DEFAULT NULL,
  `Alti` decimal(10,2) DEFAULT NULL,
  `PRS` decimal(10,2) DEFAULT NULL,
  `PRS_Sea` decimal(10,2) DEFAULT NULL,
  `WIN_S_Gust_Max` decimal(10,2) DEFAULT NULL,
  `WIN_D_Gust_Max` decimal(10,2) DEFAULT NULL,
  `WIN_D_Avg_2mi` decimal(10,2) DEFAULT NULL,
  `WIN_S_Avg_2mi` decimal(10,2) DEFAULT NULL,
  `TEM` decimal(10,2) DEFAULT NULL,
  `DPT` decimal(10,2) DEFAULT NULL,
  `RHU` decimal(10,2) DEFAULT NULL,
  `VAP` decimal(10,2) DEFAULT NULL,
  `PRE` decimal(10,2) DEFAULT NULL,
  `PRE_1h` decimal(10,2) DEFAULT NULL,
  `Snow_Depth` decimal(10,2) DEFAULT NULL,
  `VIS_HOR_1MI` decimal(10,2) DEFAULT NULL,
  `CLO_Cov` decimal(10,2) DEFAULT NULL,
  `CLO_Height_LoM` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`Datetime`,`Station_Id_C`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE `table_station_hour` (
  `Datetime` datetime(6) NOT NULL,
  `Station_Id_C` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
  `Cnty` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Province` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Town` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Station_levl` int DEFAULT NULL,
  `Station_Name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `City` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Lat` decimal(8,4) DEFAULT NULL,
  `Lon` decimal(8,4) DEFAULT NULL,
  `Alti` decimal(10,2) DEFAULT NULL,
  `wind` decimal(10,2) DEFAULT NULL,
  `tmax` decimal(10,2) DEFAULT NULL,
  `tmin` decimal(10,2) DEFAULT NULL,
  `rain` decimal(10,2) DEFAULT NULL,
  `rain_max` decimal(10,2) DEFAULT NULL,
  `view` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`Datetime`,`Station_Id_C`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

'''

# 台州逐日数据
CREATE TABLE `taizhou_daily` (
  `Datetime` datetime(6) NOT NULL,
  `Station_Id_C` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
  `Cnty` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Province` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Town` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Station_levl` int DEFAULT NULL,
  `Station_Name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `City` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Lat` decimal(8,4) DEFAULT NULL,
  `Lon` decimal(8,4) DEFAULT NULL,
  `Alti` decimal(10,2) DEFAULT NULL,
  `wind` decimal(10,2) DEFAULT NULL,
  `tmax` decimal(10,2) DEFAULT NULL,
  `tmin` decimal(10,2) DEFAULT NULL,
  `rain` decimal(10,2) DEFAULT NULL,
  `rain_max` decimal(10,2) DEFAULT NULL,
  `view` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`Datetime`,`Station_Id_C`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `taizhou_rain_max` (
  `Datetime` datetime(6) NOT NULL,
  `Station_Id_C` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
  `Cnty` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Province` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Town` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Station_levl` int DEFAULT NULL,
  `Station_Name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `City` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `Lat` decimal(8,4) DEFAULT NULL,
  `Lon` decimal(8,4) DEFAULT NULL,
  `Alti` decimal(10,2) DEFAULT NULL,
  `PRE` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`Datetime`,`Station_Id_C`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

# 挂载tensorflow-gpu
docker run --gpus all -it -p 8000:8888 -d -m 20G --memory-swap -1 --restart=always -v F:\workspace:/tensorflow-tutorials/ --name tensorflow_gpu 877b387c6aa2db9ffdaeb3264ccd426db72b7b10d1ea3302b5b5d11e8ae27cdd


docker run --name jupyter_gpu --gpus all -itd -v f:/workspace/liyuan3970/:/home/liyuan3970/ -p 8888:8888 --ipc=host 6f44c3faf8e85b23292aa00e1df6e8cad94e692c284472cb282e7436ee1ce221 jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --NotebookApp.token= --notebook-dir='/home/liyuan3970/'



# 提交当前容器为镜像
docker commit 37ed3fc2d2b6016f070a9264910a00a2a5ff68fbc4c56f4179fea2908e2948b9 met_202309:latest
# 保存镜像到本地
docker save -o met202309.tar met_202309:latest  # 第一是文件名称，第二个是标签
# 加载本地镜像
docker load -i met_202309.tar


# 项目启动指令
python manage.py runapscheduler （定时任务）
python manage.py runserver 0.0.0.0:9001 （服务器）

# 项目启动
uwsgi -d --ini uwsgi.ini    //启动

uwsgi  uwsgi.ini   --deamonize //后台运行启动  

uwsgi --stop uwsgi.pid  //停止服务  

uwsgi --reload ./uwsgi/uwsgi.pid  //可以无缝重启服务

killall -s INT uwsgi // 项目关闭

sudo date -s "2023-11-18 01:40:00"
