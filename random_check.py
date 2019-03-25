import configparser
import logging
import myrandom

from mongodb import MyMongoDB
from mysql import MySql

config = configparser.ConfigParser()
config.read('./conf/config.ini')

logger = logging.getLogger(__name__)  #

mongo = MyMongoDB(config['mongodb'])

GROUP = 10  # 抽取的样本数


def each_check(table):
    positions = SystematicSampling(table, GROUP)
    logger.info(f"positions:  {positions}")
    print(f"positions:  {positions}")

    sql = MySql(config)
    con = sql.gen_con()  # mysql

    coll = mongo.get_coll(table, 'datacenter')   # mongodb

    # 生成表头
    heads = sql.generate_sql_head_name_list(con, 'datacenter', table)
    # print("headers: ", heads)
    # 生成数据
    for pos in positions:
        data = sql.gen_mysql_info(con, table, pos)
        dict_data = sql.zip_doc_dict(heads, data)
        final_data = sql.check_each_sql_table_data(dict_data)
        # print("------> ", dict_data)
        # print("------> ", final_data)
        flag = None
        try:
            flag = coll.find(final_data, {"id"}).next()
        except Exception as e:
            # print(e)
            flag = False
        if not flag:
            print(f"未查找到当前的数据： {table} --> {pos} ---> {final_data}")
            # logger.warning(f"未查找到当前的数据： {final_data}")
            return False
    return True


def SystematicSampling(table, group):
    try:
        coll = mongo.get_coll(table, "datacenter")
        total = mongo.gen_count(coll)
        # print("table: ", table, "total: ", total)
        logger.warning("table: ", table, "total: ", total)
    except Exception as e:
        logger.warning(f"系统抽样中计算总数失败， {e}")
        raise SystemError(e)

    # print("table: ", table, "total: ", total)
    # sub_num = round(total/group)  # round(101/5) == 20

    # 根据数量级别确认系统抽样的分组数
    # group = GROUP

    sub_num = int(total/group)  # 向下取证  防止 index out of range
    # print("sub_num: ", sub_num)
    samples = list()
    for i in range(group):
        samples.append(RandomSampling(sub_num, i))

    # 头尾数据校验
    samples.insert(0, 0)
    samples.insert(0, total-1)

    # samples.insert(0, 209747)

    # samples.extend([0, total-1])
    # print("samples: ", samples)
    # logger.info(f"samples: {samples}")
    return samples


def RandomSampling(sub_num, i):
    start = i * sub_num   # 0, 1, 2, 3, 4    # 0, 20, 40, 60, 80
    end = (i+1) * sub_num - 1                # 20, 40, 60, 80, 100
    # 0-19 20-39 40-59 60-79 80-99
    try:
        sample = myrandom.randint(start, end)
        return sample
    except Exception as e:
        logger.warning(f'简单随机抽样过程失败， {e}')
        raise SystemError(e)


def main_check(tables):
    suc_tables = list()
    fail_tables = list()
    try:
        for table in tables:

            if each_check(table):
                suc_tables.append(table)
            else:
                fail_tables.append(table)
    except Exception as e:
        # logger.warning(f'some thing have been wrong during the check: {e}')
        print(f'some thing have been wrong during the check: {e}')
    # logger.info(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")
    print(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")
    return fail_tables

