import sys
import configparser
import requests
import json

from importlib import util

import logging.config

from daemon import Daemon


config = configparser.ConfigParser()
config.read('conf/config.ini')


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


class MonitorDaemon(Daemon):

    def run(self):
        sys.stderr = self.log_err

        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('mymonitor')
        except ImportError:
            self.setproctitle = False

        self.logger.info("Monitoring on.")
        while True:
            try:
                self.poke()
            except Exception as e:
                logging.warning(f"上报异常 {e}")

            try:
                self.oversee()
            except Exception as e:
                self.logger.warning(f"出现异常 {e}，停止上报")
                sys.exit(1)

    def oversee(self):
        checked_pid_file = config['log']['pidfile']
        try:
            with open(checked_pid_file) as f:
                pids = f.readlines()
        except IOError:
            message = "There is not PID file. Daemon is not running\n"
            # sys.stderr.write(message)
            # sys.exit(1)
            self.logger.warning(f"{message}")
            raise
        for pid in pids:
            # sys.stdout.write(f'{pids}')
            self.logger.info(f"文件中读取到的 pids 是： {pid}")
            try:
                procfile = open("/proc/{}/status".format(pid), 'r')
                procfile.close()
                message = "There is a process with the PID {}\n".format(pid)
                # sys.stdout.write(message)
                self.logger.info(f"{message}, 正常运行.")
            except IOError:
                message = "There is not a process with the PID {}\n".format(config['log']['pidfile'])
                # sys.stdout.write(message)
                raise SystemError(message)

    def poke(self):
        """戳一下"""

        url = "http://172.17.0.1:9999/metrics"
        d = {
            "container_id": "0001",
            "instance": "sync_daemon",
            "job": "sync_daemon",
            "name": "sync_daemon"
        }

        try:
            res = requests.post(url, data=json.dumps(d), timeout=0.5)
        except requests.exceptions.ConnectTimeout:
            raise SystemError("连接超时")
        except Exception:
            raise
        if res.status_code != 200:
            raise SystemError(f"状态码异常 {res.status_code}")


if __name__ == "__main__":
    logging.config.fileConfig('conf/logging2.conf')
    monitor_logger = logging.getLogger('root')

    pid_file = config['monitor']['pidfile']
    log_err = LoggerWriter(monitor_logger, logging.ERROR)

    monitor = MonitorDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            monitor.start()
        elif 'stop' == sys.argv[1]:
            monitor.stop()
        elif 'restart' == sys.argv[1]:
            monitor.restart()
        elif 'status' == sys.argv[1]:
            monitor.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)
