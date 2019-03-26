import datetime
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
import time

def job_func():
    for i in range(100):
        # print("当前时间：", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        time.sleep(i)

# scheduler = BackgroundScheduler()
scheduler = BlockingScheduler()
# 在每年 1-3、7-9 月份中的每个星期一、二中的 00:00, 01:00, 02:00 和 03:00 执行 job_func 任务
# scheduler .add_job(job_func, 'cron', month='1-3,7-9',day='0, tue', hour='0-3')
# scheduler.add_job(job_func, 'cron', hour=12, minute=36)
scheduler.add_job(job_func, 'cron',second = '*/5')

scheduler.start()
