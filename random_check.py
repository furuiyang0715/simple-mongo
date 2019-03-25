# 抽样 写成一个单脚本的模式即可

import configparser
import logging
import random

from common import SyncData
from mongodb import MyMongoDB

config = configparser.ConfigParser()
config.read('./conf/config.ini')

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('root')

mongo = MyMongoDB(config)
run_demo = SyncData(mongo)


def each_check(table):

    pass

    return True


def main_check(tables):
    suc_tables = list()
    fail_tables = list()
    try:
        for table in tables:
            suc_tables.append(table) if each_check(table) else fail_tables.append(table)
    except Exception as e:
        logger.warning(f'some thing have been wrong during the check: {e}')
        raise
    logger.info(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")
    return


def SystematicSampling(table, group):
    try:
        coll = mongo.get_coll(table, "datacenter")
        total = mongo.gen_count(coll)
    except Exception as e:
        logger.warning(f"系统抽样中计算总数失败， {e}")
        raise SystemError(e)
    sub_num = round(total/group)  # round(101/5) == 20
    samples = list()
    for i in range(group):
        samples.append(RandomSampling(total, sub_num, i))
    return samples


def RandomSampling(total, sub_num, i):
    start = i * sub_num   # 0, 1, 2, 3, 4    # 0, 20, 40, 60, 80
    end = (i+1) * sub_num - 1                # 20, 40, 60, 80, 100
    # 0-19 20-39 40-59 60-79 80-99
    try:
        sample = random.randint(start, end)
        return sample
    except Exception as e:
        logger.warning(f'简单随机抽样过程失败， {e}')
        raise SystemError(e)
