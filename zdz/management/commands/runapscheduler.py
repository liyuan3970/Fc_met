import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django.conf import settings
from django.core.management.base import BaseCommand
from django_apscheduler import util
from django_apscheduler.jobstores import DjangoJobStore
from django_apscheduler.models import DjangoJobExecution
# 定时任务脚本

from zdz.schedulers.external_ec_download import ec_download
from zdz.schedulers.external_radar_ftp import radar_ftp
from zdz.schedulers.external_zdz_minutes import *
from zdz.schedulers.external_zdz_hours import *
# from zdz.schedulers.external_station_sql import *
logger = logging.getLogger(__name__)


@util.close_old_connections
def delete_old_job_executions(max_age=604_800):
    DjangoJobExecution.objects.delete_old_job_executions(max_age)


class Command(BaseCommand):
    help = "Runs APScheduler."

    def handle(self, *args, **options):
        # 阻塞程序
        executors = {
            'default': ThreadPoolExecutor(8),
            'processpool': ProcessPoolExecutor(2)
        }
        scheduler = BlockingScheduler(executors=executors,timezone=settings.TIME_ZONE)
        # 非阻塞程序
        # executors = {
        #     'default': ThreadPoolExecutor(20),
        #     'processpool': ProcessPoolExecutor(5)
        # }
        # job_defaults = {
        #     'coalesce': False,  # 关闭作业合并
        #     'max_instances': 3
        # }
        # scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE)
        scheduler.add_jobstore(DjangoJobStore(), "default")
        ############################################################################################# 

        #############################################################################################
        ############################################################################################# 
        # 雷达数据同步
        scheduler.add_job(
            radar_ftp,# 函数名称
            'interval', # 间隔
            seconds=60*3, # 3分钟运行一次
            id="radar_ftp",  # The `id` assigned to each job MUST be unique
            coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=13,
            misfire_grace_time=480   
        )
        ########################## ec 模式数据##############################################################
        scheduler.add_job(
            ec_download,# 函数名称
            'interval', # 间隔
            seconds=60*60, # 2小时运行一次
            id="ec_download",  # The `id` assigned to each job MUST be unique
            coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=13,
            misfire_grace_time=600*3   
        )
        #############################################################################################
        # 台州本站数据的同步
        scheduler.add_job(
            zdz_tz_rainmax,# 函数名称
            'interval', # 间隔
            seconds=60*5, # 30s运行一次
            id="zdz_tz_rainmax",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=23,
            misfire_grace_time=10*60    
        )
        scheduler.add_job(
            zdz_tz_daily,# 函数名称
            'cron', # 间隔
            day_of_week='*',
            hour=7, # 
            minute = '0,5',
            id="zdz_tz_daily",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=33,
            misfire_grace_time=60*10    
        )
        # 自动站数据同步 =====================分钟级
        scheduler.add_job(
            zdz_minutes,# 函数名称
            'interval', # 间隔
            seconds=30, # 30s运行一次
            id="table_station_min",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=34,
            misfire_grace_time=600    
        )

        scheduler.add_job(
            zdz_del,# 函数名称
            'interval', # 间隔
            seconds = 30,#60*60*24, # 30s运行一次
            id="zdz_del",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=37,
            misfire_grace_time=600   
        )

        scheduler.add_job(
            zdz_hour_all,# 函数名称
            'interval', # 间隔
            seconds=60, # 1分钟运行一次
            id="zdz_hour_all",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=16,
            misfire_grace_time=600    
        )

        scheduler.add_job(
            zdz_hour_int,# 函数名称
            'interval', # 间隔
            seconds=60, # 1分钟运行一次
            id="zdz_hour_int",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=39,
            misfire_grace_time=600    
        )
        # 自动站数据同步 =====================小时级别
        scheduler.add_job(
            zdz_rain_24,# 函数名称
            'interval', # 间隔
            seconds=25, # 30s运行一次
            id="zdz_rain_24",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=29,
            misfire_grace_time=300    
        )

        scheduler.add_job(
            zdz_rain_12,# 函数名称
            'interval', # 间隔
            seconds=25, # 30s运行一次
            id="zdz_rain_12",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=30,
            misfire_grace_time=300    
        )

        scheduler.add_job(
            zdz_rain_6,# 函数名称
            'interval', # 间隔
            seconds=25, # 30s运行一次
            id="zdz_rain_6",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=31,
            misfire_grace_time=120    
        )

        scheduler.add_job(
            zdz_rain_3,# 函数名称
            'interval', # 间隔
            seconds=25, # 30s运行一次
            id="zdz_rain_3",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=13,
            misfire_grace_time=120    
        )

        scheduler.add_job(
            zdz_rain_1,# 函数名称
            'interval', # 间隔
            seconds=25, # 30s运行一次
            id="zdz_rain_1",  # The `id` assigned to each job MUST be unique
            # coalesce=True, #如果为 True，则在执行任务时忽略其之前的所有未执行任务
            replace_existing=True,
            max_instances=3,
            jitter=39,
            misfire_grace_time=120    
        )


        logger.info(
            "Added weekly job: 'delete_old_job_executions'."
        )

        try:
            logger.info("Starting scheduler...")
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("Stopping scheduler...")
            scheduler.shutdown()
            logger.info("Scheduler shut down successfully!")
