# import sys
import copy
import time
import datetime
import decimal
import pymysql
import configparser
import logging.config
from mongodb import MyMongoDB
from tables import tables

config = configparser.ConfigParser()
config.read('./conf/config.ini')

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('root')


class SyncData:
    def __init__(self, mongo):
        self.conf = config

        self.mysql_host = self.conf['mysql']['host']
        self.mysql_port = int(self.conf['mysql']['port'])
        self.mysql_username = self.conf['mysql']['user']
        self.mysql_password = self.conf['mysql']['password']
        self.mysql_DBname = self.conf['mysql']['databases']

        # 生成的 mongodb 数据库和 mysql 数据库同名
        self.mongo_dbname = self.conf['mysql']['databases']
        self.mongo = mongo

        self.check_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

    def generate_mysqlconnection(self):
        try:
            mysql_con = pymysql.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                charset='utf8mb4',
                db=self.mysql_DBname
            )
        except Exception:
            raise
        return mysql_con


    @staticmethod
    def generate_sql_head_name_list(connection, db_name, table_name):
        query_sql = """
        select COLUMN_NAME, DATA_TYPE, column_comment from information_schema.COLUMNS 
        where table_name="{}" and table_schema="{}";
        """.format(table_name, db_name)

        head_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for i in res:
                    head_name_list.append(i[0])
        except Exception:
            raise
        finally:
            connection.commit()
        return head_name_list

    def generate_sql_table_length(self, connection, table_name_list):
        if not isinstance(table_name_list, list):
            table_name_list = [table_name_list]

        query_sql = """select count(*) from {};"""

        _res_dict = dict()

        try:
            with connection.cursor() as cursor:
                for table_name in table_name_list:
                    q_sql = query_sql.format(table_name)
                    cursor.execute(q_sql)
                    res = cursor.fetchall()

                    try:
                        table_length = res[0][0]
                    except:
                        raise

                    _res_dict.update({table_name: table_length})
        except Exception:
            raise
        finally:
            connection.commit()
        return _res_dict

    def generate_sql_table_datas_list(self, connection, table_name, name_list, pos):
        try:
            with connection.cursor() as cursor:
                num = 10
                start = pos
                while True:
                    query_sql = """
                    select * from {} limit {},{};""".format(table_name, start, num)

                    cursor.execute(query_sql)

                    res = cursor.fetchall()
                    if not res:
                        break
                    start += num

                    yield_column_list = list()
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield_column_list.append(column_dict)
                    yield yield_column_list
        except Exception:
            raise
        finally:
            connection.commit()

    @staticmethod
    def zip_doc_dict(name_list, column_tuple):
        if len(name_list) != len(column_tuple):
            return None

        name_tuple = tuple(name_list)
        column_dict = dict(zip(name_tuple, column_tuple))
        return column_dict

    def td_format(self, td_object):
        seconds = int(td_object.total_seconds())
        # 只保留一个秒数的字符串
        return seconds
        # periods = [
        #     ('year', 60 * 60 * 24 * 365),
        #     ('month', 60 * 60 * 24 * 30),
        #     ('day', 60 * 60 * 24),
        #     ('hour', 60 * 60),
        #     ('minute', 60),
        #     ('second', 1)
        # ]
        #
        # strings = []
        # for period_name, period_seconds in periods:
        #     if seconds > period_seconds:
        #         period_value, seconds = divmod(seconds, period_seconds)
        #         has_s = 's' if period_value > 1 else ''
        #         strings.append("%s %s%s" % (period_value, period_name, has_s))
        #
        # return ", ".join(strings)

    def check_each_sql_table_data(self, dict_data):
        # （TODO） mongodb是无法对一个对象进行编码存储的，所以这里需要对读取到的结果进行强制类型转换
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

                logger.info(f'{j_list}')
                res = mongo_collection.insert_many(j_list)

        except Exception as e:
            logger.info(f"批量插入失败， 失败的原因是 {e}")
            raise

        logger.info("插入数据成功 ！, {}".format(res))

    # def gen_sql_table_name_list(self, connection):
    #     query_sql ="""select table_name from information_schema.tables where table_schema="{}";""".format(self.mysql_DBname)
    #     sql_table_name_list = list()
    #     try:
    #         with connection.cursor() as cursor:
    #             cursor.execute(query_sql)
    #             res = cursor.fetchall()
    #             for column in res:
    #                 sql_table_name_list.append(column[0])
    #
    #     except Exception:
    #         raise
    #     finally:
    #         connection.commit()
    #     return sql_table_name_list

    def do_process(self, last_pos, cur_pos, table_name_list):
        conn = self.generate_mysqlconnection()
        for table_name in table_name_list:

            if (cur_pos.get(table_name) == last_pos.get(table_name)) and (last_pos.get(table_name) != -1):
                logger.info("{} 当前数据库无更新".format(table_name))
                continue

            elif cur_pos.get(table_name) < last_pos.get(table_name):
                logger.info("{} 当前数据库可能存在删除操作".format(table_name))
                logger.info(f"{table_name} 数据库上次的 pos 为 {last_pos.get(table_name)} 当前的 pos 为 {cur_pos.get(table_name)}")

                try:
                    self.mongo.get_coll(table_name, "datacenter").drop()
                    logger.info(f"数据库 {table_name} 被drop掉啦")
                    logger.info("  ")
                    last_pos[table_name] = -1
                    cur_pos[table_name] = -1
                except Exception as e:
                    logger.info(f"drop 掉 table 时出现了异常: {e}")
                    raise SystemError(e)
                continue

            else:
                logger.info(f"{table_name} 表开始插入更新数据")
                pos = last_pos.get(table_name)
                if (not pos) or (pos == -1):
                    pos = 0

                head_name_list = self.generate_sql_head_name_list(conn, self.mysql_DBname, table_name)

                sql_table_datas_list = self.generate_sql_table_datas_list(conn, table_name, head_name_list, pos)

                mongo_collection = self.mongo.get_coll(table_name, "datacenter")

                self.write_datas2mongo(mongo_collection, sql_table_datas_list)

                last_pos[table_name] = cur_pos[table_name]

    def sync_data(self):
        conn = self.generate_mysqlconnection()

        # sql_table_name_list = self.gen_sql_table_name_list(conn)
        # sql_table_name_list = tables
        sql_table_name_list = ['risk_data', 'economic_gdp', 'futures_basic', 'comcn_embeddedvaluechange',
                               'comcn_embeddedvalue', 'comcn_embeddedvalueindex','comcn_financespecialindexic',
                               'economic_moneysupply', 'const_keywords', 'comcn_conceptlist',
                               'const_personal', 'comcn_bankindiconst']

        cur_pos = self.generate_sql_table_length(conn, sql_table_name_list)

        logger.info(f"当前的 pos 信息是：{cur_pos} ")  # no ObjectId 从 mysql 中查询出的

        last_pos1 = self.mongo.get_log_pos()
        logger.info(f"从mongodb数据库中查询出的上一次的记录是： {last_pos1}")

        last_pos = copy.copy(last_pos1)

        if last_pos:
            last_pos.pop("_id")
        last_pos = self.mongo.calibration_last_location(last_pos, sql_table_name_list)
        logger.info(f"自查当前的mongodb数据库，校正后的上一次的 pos 信息是：{last_pos}")  # have no ObjectId

        # ignore_re = last_pos - cur_pos 忽略上次做了同步 但不在本次同步范围内的
        reserved_pos = [{key: last_pos.get(key, 0)} for key in cur_pos.keys()]

        last_pos = dict()
        for _dict in reserved_pos:
            last_pos.update(_dict)
        logger.info(f"忽略未在本次同步的数据后，上一次的 pos 信息是：{last_pos}")  # have no ObjectId

        flag = (-1 in list(last_pos.values()) or -1 in list(cur_pos.values()))  # 说明有数据库被 drop 掉...
        while last_pos != cur_pos or flag:
            logger.info("当前数据尚未一致， 进入处理流程...... ......")
            self.do_process(last_pos, cur_pos, sql_table_name_list)

        self.mongo.write_log_pos(last_pos1, last_pos)


if __name__ == '__main__':
    logger.info("  "*1000)
    logger.info("  "*1000)
    sync_moment = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'当前时间是 {sync_moment}  开始同步数据啦')

    mongo = MyMongoDB(config['mongodb'])
    rundemo = SyncData(mongo)

    start_ = time.time()

    try:
        rundemo.sync_data()
    except Exception as e:
        logger.debug(f"同步失败， 失败的原因是：{e} ")

    end_ = time.time()
    logger.info(f'同步数据结束, 本次同步所用时间 {round((end_ - start_) / 60, 2)} min')
    logger.info("  " * 1000)
    logger.info("  " * 1000)
