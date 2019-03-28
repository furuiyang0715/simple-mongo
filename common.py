import sys
import copy
import time
import datetime
import decimal
import pymysql
import configparser

import os
from multiprocessing import Process

from importlib import util
from apscheduler.schedulers.blocking import BlockingScheduler

import logging.config

from daemon import Daemon
from mongodb import MyMongoDB
from myrandom import RandomCheck
from mysql import MySql
from tables import tables

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


class SyncData:
    def __init__(self, mongo, mysql, config, logger):
        self.conf = config
        self.mongo = mongo
        self.mysql = mysql
        self.logger = logger

    def td_format(self, td_object):
        seconds = int(td_object.total_seconds())
        return seconds

    def check_each_sql_table_data(self, dict_data):
        #  在测试的过程中发现需要转换的类型有：
        #  (1) decimal.Decimal
        # （2) datetime.timedelta(seconds=75600)

        for key, value in dict_data.items():
            if isinstance(value, decimal.Decimal):
                if value.as_tuple().exponent == 0:
                    dict_data[key] = int(value)
                else:
                    dict_data[key] = float(value)

            elif isinstance(value, datetime.timedelta):
                dict_data[key] = self.td_format(value)

        return dict_data

    def write_datas2mongo(self, mongo_collection, sql_table_datas_list):
        try:
            for yield_list in sql_table_datas_list:
                j_list = list()
                for j in yield_list:
                    j = self.check_each_sql_table_data(j)
                    j_list.append(j)

                # Error: unhashable type: 'dict'
                # j_set = set(j_list)
                # if len(j_set) != len(j_list):
                #     raise SystemError("批量数据中存在至少两个相同的数目，请进行检查 ...")
                # j_list 中有重复元素 会报错： batch op errors occurred
                # 参考： https://stackoverflow.com/questions/38361916/pymongo-insert-many-bulkwriteerror

                # logger.info(f'{j_list}')
                res = mongo_collection.insert_many(j_list)

        except Exception as e:
            self.logger.warning(f"批量插入失败， 失败的原因是 {e}")
            raise SystemError(e)

        self.logger.info("插入数据成功 ！, {}".format(res))

    def do_process(self, last_pos, cur_pos, table_name_list):
        conn = self.mysql.gen_con()
        for table_name in table_name_list:

            if (cur_pos.get(table_name) == last_pos.get(table_name)) and (last_pos.get(table_name) != -1):
                self.logger.info("  {}   当前数据库无更新".format(table_name))
                continue

            elif cur_pos.get(table_name) < last_pos.get(table_name):
                self.logger.info("  {}   当前数据库可能存在删除操作".format(table_name))
                self.logger.info(f"{table_name} 数据库上次的 pos 为 {last_pos.get(table_name)} 当前的 pos 为 {cur_pos.get(table_name)}")

                try:
                    self.mongo.get_coll(table_name, "datacenter").drop()
                    self.logger.info(f"数据库 {table_name} 被drop掉啦")
                    self.logger.info("  ")
                    last_pos[table_name] = -1
                    cur_pos[table_name] = -1
                except Exception as e:
                    self.logger.warning(f"drop 掉 table {table_name} 时出现了异常: {e}")
                    raise SystemError(e)
                continue

            else:
                self.logger.info(f"   {table_name}     表开始插入更新数据")
                pos = last_pos.get(table_name)
                if (not pos) or (pos == -1):
                    pos = 0

                head_name_list = self.mysql.gen_sql_head_name_list(conn, 'datacenter', table_name)

                sql_table_datas_list = self.mysql.gen_sql_table_datas_list(conn, table_name, head_name_list, pos)

                mongo_collection = self.mongo.get_coll(table_name, "datacenter")

                self.write_datas2mongo(mongo_collection, sql_table_datas_list)

                last_pos[table_name] = cur_pos[table_name]

    def sync_data(self, tables):
        # 某个时刻 mysql 数据库中的数量信息
        con = self.mysql.gen_con()
        cur_pos = self.mysql.gen_sql_table_length(con, tables)
        self.logger.info(f"当前 pos 信息：{cur_pos} ")

        # log_pos 中的记录信息 用于 upsert 更新写入
        last_pos1 = self.mongo.get_log_pos()
        self.logger.info(f"pos_log 中查询记录： {last_pos1}")

        last_pos = copy.copy(last_pos1)

        if last_pos:
            last_pos.pop("_id")
        last_pos = self.mongo.calibration_last_location(last_pos, tables)
        self.logger.info(f"根据实际情况校正记录： {last_pos}")  # have no ObjectId

        flag = (-1 in list(last_pos.values()) or -1 in list(cur_pos.values()))

        f1 = True
        for table in tables:
            if last_pos.get(table) != cur_pos.get(table):
                f1 = False

        while (not f1) or flag:
            self.logger.info("...... ...... 当前数据尚未一致， 进入处理流程...... ......")
            self.logger.info(f"last_pos:{last_pos}")
            self.logger.info(f"cur_pos:{cur_pos}")
            # 目的： 根据 last_pos 和 cur_pos 处理到两者一致
            self.do_process(last_pos, cur_pos, tables)

            flag = (-1 in list(last_pos.values()) or -1 in list(cur_pos.values()))
            f1 = True
            for table in tables:
                if last_pos.get(table) != cur_pos.get(table):
                    f1 = False

        # 保持查询 mySQL 的时刻的数据一致性
        self.logger.info(f"上一次的记录数据是： {last_pos1}, 本次的更入校正数据是 {cur_pos}")
        try:
            self.mongo.write_log_pos(last_pos1, cur_pos)
        except Exception as e:
            raise SystemError(e)


class MyMongoDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err

        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('mymongo')
        except ImportError:
            self.setproctitle = False

        self.logger.info("Running")

        self.dummy_sched()

        self.scheduler()

        # while True:
        #     # fork 出的守护进程（主进程） 持续运行
        #     self.logger.info('During...')   # 每分钟打印一个 During
        #     time.sleep(60)

    def scheduler(self):
        # self.write_pid(str(os.getpid()))
        # if self.setproctitle:
        #     import setproctitle
        #     setproctitle.setproctitle('mymongo_scheduler')
        sched = BlockingScheduler()
        try:
            sched.add_job(self.dummy_sched, 'interval', minutes=20)
            # sched.add_job(self.dummy_sched, 'interval', hours=24)
            sched.start()
        except Exception as e:
            self.logger.error('Cannot start scheduler. Error: ' + str(e))

    def dummy_sched(self):
        self.logger.info("  " * 1000)
        self.logger.info("  " * 1000)
        sync_moment = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f'当前时间是 {sync_moment}  开始同步数据啦')

        """发送进程开启的监控信息"""
        # import requests
        # import json
        #
        # url = "http://172.17.0.1:6399/metrics"
        # d = {
        #     "container_id": "0001",
        #     "instance": "sync ins",
        #     "job": "sync data from mysql to mongodb",
        #     "name": "sync-01"
        # }
        # for i in range(10):
        #     res = requests.post(url, data=json.dumps(d))
        #     code = res.status_code
        #     if code == 200:
        #         break
        # self.logger.info(f"已向监控报告开启， 回复状态码是 {code}")

        # 再读取一次 config
        # config = configparser.ConfigParser()
        # config.read('conf/config.ini')

        mongo = MyMongoDB(config['mongodb'], self.logger)
        mysql = MySql(config, self.logger)
        rundemo = SyncData(mongo, mysql, config, self.logger)

        start_ = time.time()

        try:
            rundemo.sync_data(tables)
        except Exception as e:
            self.logger.warning(f"同步失败， 失败的原因是：{e} ")

        end_ = time.time()
        self.logger.info(f'同步数据结束, 本次同步所用时间 {round((end_ - start_) / 60, 2)} min')
        self.logger.info("  " * 1000)
        self.logger.info("  " * 1000)

        self.logger.info(f"开始进行抽样检查")
        t1 = time.time()

        myrandom = RandomCheck(10, mongo, config, self.logger)
        fail_tables = myrandom.check(tables)

        t2 = time.time()
        self.logger.info(f'抽样失败列表: {fail_tables} 耗时 {round((t2 - t1) / 60, 2)} min')

        t3 = time.time()
        while fail_tables:
            self.logger.info("抽样失败，开始重建....")
            for table in fail_tables:
                try:
                    mongo.get_coll(table, "datacenter").drop()
                    self.logger.info(f"数据库 {table} 被drop掉啦")
                except Exception as e:
                    self.logger.warning(f"drop 掉 table {table} 时出现了异常: {e}")
                    raise SystemError(e)

            rundemo.sync_data(fail_tables)
            # 再次进行抽样
            fail_tables = myrandom.check(fail_tables)
            self.logger.info(f"重建后抽样结果： {fail_tables}")
        t4 = time.time()
        self.logger.info(f"重建耗时： {round((t4 - t3) / 60, 2)} min")

        self.logger.info("over")

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


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

        self.oversee()

    def oversee(self):
        flag = True
        while flag:
            try:
                self.poke()
                self.logger.info("上报成功.")
                # 20 分钟上报一次
                # time.sleep(20*60)
                # time.sleep(2)
            except Exception as e:
                self.logger.info(f"上报回复异常. 原因： {e}")

    def poke(self):
        """戳一下"""
        import requests
        import json

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


def sync_start():

    logging.config.fileConfig('conf/logging.conf')
    logger = logging.getLogger('common')

    pid_file = config['log']['pidfile']
    log_err = LoggerWriter(logger, logging.ERROR)

    worker = MyMongoDaemon(pidfile=pid_file, log_err=log_err)
    worker.start()


def monitor_start():
    logging.config.fileConfig('conf/monitorlog.conf')
    logger = logging.getLogger('monitor')

    log_err = LoggerWriter(logger, logging.ERROR)
    pid_file = config['monitor']['pidfile']
    monitor = MonitorDaemon(pidfile=pid_file, log_err=log_err)
    monitor.start()


if __name__ == '__main__':
    # sync_start()
    monitor_start()
