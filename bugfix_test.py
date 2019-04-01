# 问题描述： 抽样校验的时候 最后一个容易出问题
import sys
import copy
import random
import decimal
import pymysql
import pymongo
import datetime

from mongodb import MyMongoDB


def gen_con():
    try:
        mysql_con = pymysql.connect(
            host="139.159.176.118",
            port=3306,
            user="dcr",
            password="acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4",
            charset='utf8mb4',
            db="datacenter"
        )
    except Exception as e:
        print(f"创建数据库连接失败： {e}")
    return mysql_con


def gen_sql_table_len(con, tables):
    if not isinstance(tables, list):
        tables= [tables]

    query_sql = """select count(*) from {};"""

    _res_dict = dict()

    try:
        with con.cursor() as cursor:
            for table in tables:
                q_sql = query_sql.format(table)
                cursor.execute(q_sql)
                res = cursor.fetchall()

                try:
                    table_length = res[0][0]
                except Exception:
                    raise

                _res_dict.update({table: table_length})
    except Exception as e:
        print(f"查询 mysql 中当前每一张 table 的长度失败了， 具体的原因是 {e}")
        raise SystemError(e)
    finally:
        con.commit()
    return _res_dict


def get_mdb():
    try:
        mdb = pymongo.MongoClient("mongodb://127.0.0.1:27017/", connect=False)
    except Exception as e:
        print(f'创建 pymongo 连接失败， 失败的原因是： {e}')
    return mdb


def get_db(db_name):
    mdb = get_mdb()
    try:
        db = mdb[db_name]
    except:
        try:
            db = mdb.get_database(db_name)
        except Exception as e:
            print(f'创建 {db_name} 数据库连接失败， 失败的原因是 {e}')

    return db


def get_mcon(table, db):
    mdb = get_db(db)
    mcon = mdb[table]
    return mcon


def get_log_pos():
    coll = get_mcon('pos_log', "datacenter")
    try:
        last_log = coll.find_one()  # pos_log 表格里始终只有一条数据
    except Exception as e:
        print(f"获取 log_pos 信息失败，失败原因 {e}")
    return last_log


def calibration_last_location(last_pos, tables):
    for table in tables:
        coll = get_mcon(table, "datacenter")
        count = coll.find().count()
        print("--------------->", count)

        last_pos = dict() if not last_pos else last_pos

        if count != last_pos.get(table):
            last_pos.update({table: count})
    return last_pos


def gen_heads(con, db, table):
    query_sql = """
            select COLUMN_NAME, DATA_TYPE, column_comment from information_schema.COLUMNS 
            where table_name="{}" and table_schema="{}";
            """.format(table, db)

    heads = list()
    try:
        with con.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()
            for i in res:
                heads.append(i[0])
    except Exception as e:
        print(f"gen sql head name list {db}.{table} 失败，原因 {e}")
    finally:
        con.commit()
    return heads


def zip_doc_dict(heads, column_tuple):
    if len(heads) != len(column_tuple):
        return None

    name_tuple = tuple(heads)
    column_dict = dict(zip(name_tuple, column_tuple))
    return column_dict


def gen_datas(con, table, heads, pos):
    try:
        with con.cursor() as cursor:
            # num 的值在同步的时候可以设置较大 且不打印数据 在增量更新阶段 可以设置小一点 且在日志中打印插入的 items
            num = 1000
            start = pos
            while True:
                query_sql = """
                select * from {} limit {},{};""".format(table, start, num)

                cursor.execute(query_sql)

                res = cursor.fetchall()
                if not res:
                    break
                start += num

                yield_column_list = list()
                for column in res:
                    column_dict = zip_doc_dict(heads, column)
                    yield_column_list.append(column_dict)
                yield yield_column_list
    except Exception as e:
        print(f'gen table data list 失败， {table} at position {pos}, 原因 {e}')
    finally:
        con.commit()


def td_format(td_object):
    seconds = int(td_object.total_seconds())
    return seconds


def check_each_sql_table_data(dict_data):
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
            dict_data[key] = td_format(value)

    return dict_data


def write_datas2mongo(mcon, datas):
    try:
        for yield_list in datas:
            j_list = list()
            for j in yield_list:
                j = check_each_sql_table_data(j)
                j_list.append(j)

            res = mcon.insert_many(j_list)

    except Exception as e:
        print(f"批量插入失败， 失败的原因是 {e}")
        raise SystemError(e)

    print("插入数据成功 ！, {}".format(res))


def write_log_pos(last_pos, cur_pos):
    coll = get_mcon('pos_log', "datacenter")
    try:
        doc = list(coll.find())
    except Exception as e:
        print(f'查找 log_pos 失败，失败的原因是 {e}')

    if doc:
        # print(doc)
        try:
            print("update......")
            # print(f"被跟新进入的日志结构是： {cur_pos}")
            # coll.replace_one(last_pos, cur_pos)
            coll.update(last_pos, cur_pos, upsert=True)

        except Exception as e:
            print(f'更新日志结构失败，原因 {e}')

    else:  # rewrite
        try:
            print("rewrite......")
            coll.insert_one(cur_pos)
        except Exception as e:
            print(f'首次写入 log_pos 失败，原因 {e}')

    res = coll.find().next()
    # print(f"更新结果： {res}")


def do_process(cali_pos, cur_pos, tables):
    # 生成一个 mysql 的连接
    con = gen_con()

    for table in tables:
        if cur_pos.get(table) == cali_pos.get(table):
            print(f'当前数据库无更新 : {table}')
            continue

        elif cur_pos.get(table) < cali_pos.get(table):
            print(f'数据库可能存在误删除操作：{table}')
            # 删除该数据库
            mcon = get_mcon(table, "datacenter")
            mcon.drop()
            # 将当前的数量和校正后的数量都置为 -1
            # cur_pos[table] = -1
            cali_pos[table] = -1
            continue

        else:
            pos = cali_pos.get(table)
            if not pos or pos == -1:
                # not pos 说明是第一次同步 pos == "" 说明被重建
                pos = 0
            heads = gen_heads(con, "datacenter", table)
            print(f"heads--> {heads}")

            datas = gen_datas(con, table, heads, pos)

            # 创建mongo数据库的连接
            mcon = get_mcon(table, "datacenter")

            # 写入mongodb数据库 datas 是一个生成器 在该方法中去遍历生成器
            write_datas2mongo(mcon, datas)

            # cur_pos 赋值给 last_pos
            cali_pos[table] = cur_pos[table]


# 定义进行系统抽样的函数
def sys_sampling(table, group):
    total = count_res.get(table)
    # 每一层的样本数 例如总数为999，分10层抽样，sub_num为100，每层抽取1个
    sub_num = int(total/group)  # 999/10 = 99.9 int(99.9) = 99
    # 存放抽样数据
    samples = list()
    for i in range(group):   # 0, 1, 2, 3, 4, ... 9
        start = i * sub_num  # 0, 99, 99*2, ... 99*9
        end = (i+1) * sub_num - 1  # 98, 99*2-1, 99*3-1... 99*10-1
        start, end = (start, end) if start <= end else (end, start)
        # sub_num - 1 < 0  --> sub_num < 1 的情况
        # total / group < 1  --> total < group
        sample = random.randint(start, end)
        samples.append(sample)

    # print(f'当前的表是{table}, \n抽取的样本数是{samples}')
    samples.append(total-1)
    samples.append(0)

    return samples


def gen_pos_info(con, table, pos):
    query_sql = """select * from {} limit {}, 1""".format(table, pos)
    try:
        with con.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()[0]
    except Exception as e:
        print(f"查询pos info失败, 原因 {e}")
    finally:
        con.commit()
    return res


# 创建mysql连接
con = gen_con()

# 输入要查询的数据库名字
tables = [
    'economic_gdp',
    'futures_basic',
    'comcn_embeddedvaluechange',
    'comcn_embeddedvalue',
    'comcn_embeddedvalueindex',
    'comcn_financespecialindexic',
    'economic_moneysupply',
    'const_keywords',
    'comcn_conceptlist',
    'const_personal',
    'comcn_bankindiconst',
    'comcn_financespecialindexsc',
    'hkland_historycashflow',
    'const_industry',
    'derived_institution_summary',
    'stk_specialtrade',
    'hkland_historytradestat',
    'index_indexprepcomponent',
    'stk_codechangeserial',
    'const_secumain',
    'const_areacode',
    'const_newsconst',
    'stk_liststatus',
    'comcn_controllingshareholders',
    'comcn_actualcontroller',
    'comcn_violationhalding',
    'const_product',
    'comcn_financespecialindex',
    'index_basicinfo',
    'comcn_msecufinance',
    'comcn_issuanceexamination',
    'stk_abbrchangeserial',
    'stk_secuchange',
    'const_industrytype',
    'const_ussecumain',
    'comcn_performanceletters',
    'comcn_bankassetsliability',
    'comcn_sharesfloatingschedule',
]
# tables = ["comcn_fsderiveddata", "comcn_actualcontroller", "const_industry"]

# 查询当前的 pos 信息
cur_pos = gen_sql_table_len(con, tables)

print(f"当前的时间戳是{datetime.datetime.now()}, cur pos 为 {cur_pos}")
# 当前的时间戳是2019-04-01 09:28:32.858347, cur pos 为 {'comcn_fsderiveddata': 315263}

# 创建 mongodb 的连接
last_pos1 = get_log_pos()
print(f'log_pos 字典：{last_pos1}')

# pos = last_pos1.get(tables[0])
# print(f'从 log_pos 中相应  table 的 pos 是： {pos}')

# 根据实际情况校正记录
last_pos = copy.copy(last_pos1)  # !!! 保证 last_pos1 不变

cali_pos = calibration_last_location(last_pos, tables)
print(f"校正后的全部pos信息是 {cali_pos}")

# c_pos = cali_pos.get(tables[0])
# print(f'校正后的单个pos信息： {c_pos}')

print("当前数据尚未一致进入处理流程......")
f1 = True
for table in tables:
    if cali_pos.get(table) != cur_pos.get(table):
        f1 = False
f2 = -1 in list(cali_pos.values())

while f2 or not f1:
    do_process(cali_pos, cur_pos, tables)
    # 每一次 do_process后改变判断条件 否则进入死循环
    f1 = True
    for table in tables:
        if cali_pos.get(table) != cur_pos.get(table):
            f1 = False
    f2 = -1 in list(cali_pos.values())

print("校正完毕 .")
write_log_pos(last_pos1, cur_pos)

# 开始进行随机抽样
# 确定每个数据库抽样的个数
check_num = 20
# 总数是从 pos_log 中获取到的
count_coll = get_mcon("pos_log", "datacenter")
count_res = dict()
for table in tables:
    total = count_coll.find().next().get(table)
    count_res.update({table: total})
print(f"随机抽样 计算总数的结果是： {count_res}")


for table in tables:
    """对表进行系统抽样"""
    print(f'当前的表是 {table}')
    samples = sys_sampling(table, check_num)
    print(f'抽取的样本号码是 {samples}')

    # 对于抽取出的每一个点拼接数据
    heads = gen_heads(con, 'datacenter', table)
    for pos in samples:
        data = gen_pos_info(con, table, pos)
        complete_data = zip_doc_dict(heads, data)
        final_data = check_each_sql_table_data(complete_data)
        try:
            exit = get_mcon(table, 'datacenter').find(final_data)
        except Exception as e:
            print(f"查找失败 ,失败的原因是 {e}")
            print(f'未找到当前的数据 {table}--{pos}--> {final_data}')
            # sys.exit(0)
            continue
        # print(f"查找成功 ~~ {pos}")
