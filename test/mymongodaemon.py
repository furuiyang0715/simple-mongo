import sys
import time
import logging.config
import logging
import os
from multiprocessing import Process

from importlib import util
from apscheduler.schedulers.blocking import BlockingScheduler

from daemon import Daemon

logging.config.fileConfig('../conf/test.conf')
logger = logging.getLogger('test')


class MyMongoDaemon(Daemon):
    def run(self):
        self.logger = logger
        sys.stderr = self.log_err

        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('mymongo')
        except ImportError:
            self.setproctitle = False
    
        self.logger.info("Running")

        # self.queues = dict()
        # self.queues['replicator_out'] = Queue()

        procs = dict()
        procs['scheduler'] = Process(name='scheduler', target=self.scheduler)
        procs['scheduler'].daemon = True
        procs['scheduler'].start()

        # procs['replicator'] = Process(name='replicator', target=self.replicator)
        # procs['replicator'].daemon = True
        # procs['replicator'].start()
        #
        # procs['datamunging'] = Process(name='datamunging', target=self.data_munging)
        # procs['datamunging'].daemon = True
        # procs['datamunging'].start()
        #
        # procs['dataprocess'] = Process(name='dataprocess', target=self.data_process)
        # procs['dataprocess'].daemon = True
        # procs['dataprocess'].start()

        while True:
            # fork 出的守护进程（主进程） 持续运行
            self.logger.info('Working...')
            time.sleep(60)

    def scheduler(self):
        self.write_pid(str(os.getpid()))
        if self.setproctitle:
            import setproctitle
            setproctitle.setproctitle('mymongo_scheduler')
        sched = BlockingScheduler()
        try:
            # 设置定时任务
            sched.add_job(self.dummy_sched, 'interval', minutes=1)
            sched.start()
        except Exception as e:
            self.logger.error('Cannot start scheduler. Error: ' + str(e))

    def dummy_sched(self):
        # 顺序执行的任务 等于是 1分钟执行一次
        self.logger.info('Scheduler works!')
        self.logger.info("定时任务执行开启 ......")
        time.sleep(10)
        self.logger.info("定时任务执行结束 ......")

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


if __name__ == "__main__":
    pid_file = "/Users/furuiyang/Desktop/temp/simple_mongo/test/mymongo.pid"
    log_err = LoggerWriter(logger, logging.ERROR)
    dd = MyMongoDaemon(pidfile=pid_file, log_err=log_err)
    dd.start()

