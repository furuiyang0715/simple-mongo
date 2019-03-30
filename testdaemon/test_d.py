import sys
import time
import logging.config

from importlib import util

from daemon import Daemon
from apscheduler.schedulers.blocking import BlockingScheduler


class testDaemon(Daemon):
    def run(self):
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('justtest')
        except ImportError:
            self.setproctitle = False

        self.logger.info("Running ......")
        self.work()

        self.scheduler()

    def scheduler(self):
        job_defaults = {'max_instances': 1}
        sched = BlockingScheduler(job_defaults=job_defaults)
        try:
            sched.add_job(self.work, 'interval', seconds=5)
            sched.start()
        except Exception as e:
            self.logger.error('Cannot start scheduler. Error: ' + str(e))
            sys.exit(1)

    def work(self):
        sed = time.time()

        for i in range(3):
            self.logger.info(f"hello, {i}, {sed}")
            time.sleep(10)


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


logging.config.fileConfig("conf/logging.conf")

testlogger = logging.getLogger("root")

log_err = LoggerWriter(testlogger, logging.ERROR)

testins = testDaemon(pidfile="/Users/furuiyang/Desktop/temp/simple_mongo/testdaemon/testone.pid", log_err=log_err)

testins.start()
