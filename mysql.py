import datetime
import decimal
import pymysql
import logging.config


logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('mysql')


class SysException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class MySql:
    def __init__(self, config):
        self.mysql_host = config['mysql']['host']
        self.mysql_port = int(config['mysql']['port'])
        self.mysql_username = config['mysql']['user']
        self.mysql_password = config['mysql']['password']
        self.mysql_DBname = config['mysql']['databases']

    def gen_con(self):
        try:
            mysql_con = pymysql.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                charset='utf8mb4',
                db=self.mysql_DBname
            )
        except Exception as e:
            logger.warning(f"创建数据库连接失败： {e}")
            raise SystemError(e)
        return mysql_con

    def gen_mysql_info(self, con, table, pos):
        query_sql = """select * from {} limit {}, 1""".format(table, pos)
        # print(query_sql)
        try:
            with con.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()[0]
        except Exception as e:
            logger.warning(f"查询pos info失败, 原因 {e}")
            raise SystemError(e)
        finally:
            con.commit()
        return res

    def generate_sql_head_name_list(self, connection, db_name, table_name):
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
        except Exception as e:
            logger.warning(f"gen sql head name list {db_name}.{table_name} 失败，原因 {e}")
            raise SystemError(e)
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
                    except Exception:
                        raise

                    _res_dict.update({table_name: table_length})
        except Exception as e:
            logger.warning(f"查询 mysql 中当前每一张 table 的长度失败了， 具体的原因是 {e}")
            raise SystemError(e)
        finally:
            connection.commit()
        return _res_dict

    def zip_doc_dict(self, name_list, column_tuple):
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

    def gen_sql_table_name_list(self, connection):
        query_sql ="""select table_name from information_schema.tables where 
        table_schema="{}";""".format(self.mysql_DBname)
        sql_table_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    sql_table_name_list.append(column[0])

        except Exception:
            raise
        finally:
            connection.commit()
        return sql_table_name_list
