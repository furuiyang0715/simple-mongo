# 抽样

import configparser
import logging

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







