import sys
import os
import time
import atexit
import logging

from signal import SIGTERM


class Daemon(object):
    """Class to demonize the application

    Args:
        pidfile (str): path for the pidfile
        stdin (Optional[str]): path to stdin. Default to /dev/null
        stdout (Optional[str]): path to stdout. Default to /dev/null
        stderr (Optional[str]): path to stderr. Default to /dev/null
        log_err (Optional[object]): :class:`.LoggerWriter` object. Default to None

    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null',
                 log_err=None):
        # 定义一个守护进程的标准输入流 输出流 错误流 以及日志
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        if log_err is not None:
            self.log_err = log_err
        self.logger = logging.getLogger("test")
        self.modules = dict()

    def daemonize(self):
        """Deamonize, do double-fork magic.
        """
        try:
            pid = os.fork()
            # 调用一次 返回两次 分别在父进程和子进程中返回
            # 在父进程中 pid>0 子进程中 pid<0
            if pid > 0:
                # Exit first parent.
                # self.logger.info(" ")
                # self.logger.info(os.environ)
                self.logger.info("Done first fork")
                # 位于父进程时，父进程退出
                sys.exit(0)
        except OSError as e:
            message = "Fork #1 failed: {}\n".format(e)
            self.logger.error(message)
            sys.stderr.write(message)
            sys.exit(1)

        # self.logger.info(f"pid is {pid}, 此时进入到第一次fork出的子进程中")
        # Decouple from parent environment.
        # 分离启动进程的环境变量 开始设置自己的环境变量
        os.chdir("/")  # 切换到 / 目录下，将守护进程放在总是存在的目录中
        os.setsid()  # 创建新的会话，成为会话首领 接下来将其变为新的会话组的首领
        os.umask(0)  # 修改文件权限 使进程有较大权限 保证进程有读写执行权限

        # Do second fork.
        # 第二次 fork 不是必须的
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from second parent.
                self.logger.info("Done second fork")
                sys.exit(0)
        except OSError as e:
            message = "Fork #2 failed: {}\n".format(e)
            self.logger.error(message)
            sys.stderr.write(message)
            sys.exit(1)
        # self.logger.info(f"此时进入放到第二次 fork 出的子进程中...pid = {pid}")

        self.logger.info('deamon going to background, PID: {}'.format(os.getpid()))

        # Redirect standard file descriptors.
        # 重定向标准的输入 输出 错误流
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # Write pidfile.
        pid = str(os.getpid())
        # self.logger.info(f"----------> {pid}")
        # 将当前的进程写入文件
        # self.logger.info(self.pidfile)
        open(self.pidfile, 'w+').write("{}\n".format(pid))

        # Register a function to clean up.
        atexit.register(self.delpid)

    def delpid(self):
        """Delete pid file created by the daemon

        """
        os.remove(self.pidfile)

    def start(self):
        """Start daemon.
        """
        pids = None
        # print(pids)

        # Check pidfile to see if the daemon already runs.
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError:
            # print("no file")
            pids = None

        if pids:
            message = "Pidfile {} already exist. Daemon already running?\n".format(self.pidfile)
            self.logger.error(message)
            sys.stderr.write(message)
            sys.exit(1)

        # Start daemon.
        self.daemonize()

        self.logger.info("Demonized. Start run")
        # print("Demonized. Start run")
        self.run()

    def status(self):
        """Get status of daemon.
        """
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError:
            message = "There is not PID file. Daemon is not running\n"
            sys.stderr.write(message)
            sys.exit(1)

        for pid in pids:
            sys.stdout.write(f'{pids}')
            # 此处是 Linux 上的状态信息 MacOS 上的与此有区别
            try:
                procfile = open("/proc/{}/status".format(pid), 'r')
                procfile.close()
                message = "There is a process with the PID {}\n".format(pid)
                sys.stdout.write(message)
            except IOError:
                message = "There is not a process with the PID {}\n".format(self.pidfile)
                sys.stdout.write(message)

    def stop(self):
        """Stop the daemon.
        """
        # Get the pid from pidfile.
        try:
            with open(self.pidfile) as f:
                pids = f.readlines()
        except IOError as e:
            message = str(e) + "\nDaemon not running?\n"
            sys.stderr.write(message)
            self.logger.error(message)
            sys.exit(1)

        for pid in pids:
            # Try killing daemon process.
            try:
                logging.info('Trying to kill pid: '+pid.strip())
                os.kill(int(pid.strip()), SIGTERM)
                self.logger.info('Killed pid: '+pid.strip())
                time.sleep(1)
            except OSError as e:
                self.logger.error('Cannot kill process with pid: '+pid.strip())
                self.logger.error(str(e))
            # sys.exit(1)

        try:
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
        except IOError as e:
            message = str(e) + "\nCan not remove pid file {}".format(self.pidfile)
            sys.stderr.write(message)
            self.logger.error(message)
            sys.exit(1)

    def restart(self):
        """Restart daemon.
        """
        self.stop()
        time.sleep(1)
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon.
        It will be called after the process has been daemonized by start() or restart().

        Example:

        class MyDaemon(Daemon):
            def run(self):
                while True:
                    time.sleep(1)
        """
