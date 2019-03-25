import configparser
import logging
import random

from common import SyncData
from mongodb import MyMongoDB
from mysql import MySql

config = configparser.ConfigParser()
config.read('./conf/config.ini')

logging.config.fileConfig('conf/l2.conf')
logger = logging.getLogger('mysql')

mongo = MyMongoDB(config['mongodb'])
run_demo = SyncData(mongo)

GROUP = 10  # 抽取的样本数


def each_check(table):
    positions = SystematicSampling(table, GROUP)
    print("positions: ", positions)

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
            print(e)
        if not flag:
            print(f"未查找到当前的数据： {final_data}")
            # logger.warning(f"未查找到当前的数据： {final_data}")
            return False
    return True


def SystematicSampling(table, group):
    try:
        coll = mongo.get_coll(table, "datacenter")
        total = mongo.gen_count(coll)
        print("table: ", table, "total: ", total)
    except Exception as e:
        logger.warning(f"系统抽样中计算总数失败， {e}")
        raise SystemError(e)

    print("table: ", table, "total: ", total)
    # sub_num = round(total/group)  # round(101/5) == 20

    # 根据数量级别确认系统抽样的分组数
    if total < 1:
        return
    elif total < 100:
        group = 5
    elif total < 1000:
        group = 10
    elif group < 10000:
        group = 50
    elif group < 1000000:
        group = 500
    else:
        group = 1000

    sub_num = int(total/group)  # 向下取证  防止 index out of range
    # print("sub_num: ", sub_num)
    samples = list()
    for i in range(group):
        samples.append(RandomSampling(sub_num, i))
    print("samples: ", samples)
    return samples


def RandomSampling(sub_num, i):
    start = i * sub_num   # 0, 1, 2, 3, 4    # 0, 20, 40, 60, 80
    end = (i+1) * sub_num - 1                # 20, 40, 60, 80, 100
    # 0-19 20-39 40-59 60-79 80-99
    try:
        sample = random.randint(start, end)
        return sample
    except Exception as e:
        logger.warning(f'简单随机抽样过程失败， {e}')
        raise SystemError(e)


def main_check(tables):
    suc_tables = list()
    fail_tables = list()
    try:
        for table in tables:
            print("table: ", table)
            if each_check(table):
                suc_tables.append(table)
            else:
                fail_tables.append(table)
            # suc_tables.append(table) if each_check(table) else fail_tables.append(table)
    except Exception as e:
        # logger.warning(f'some thing have been wrong during the check: {e}')
        print(f'some thing have been wrong during the check: {e}')
    # logger.info(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")
    print(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")


if __name__ == "__main__":
    tables = [
        # 'risk_data',  # 0
        # 'economic_gdp',  # 52
        # 'futures_basic',  # 57
        # 'comcn_embeddedvaluechange',  # 70
        # 'comcn_embeddedvalue',  # 89
        #
        # 'comcn_embeddedvalueindex',  # 89
        # 'comcn_financespecialindexic',  # 99
        # 'economic_moneysupply',  # 134
        # 'const_keywords',  # 448
        # 'comcn_conceptlist',  # 571

        # 'const_personal',
        # 'comcn_bankindiconst',
        # 'comcn_financespecialindexsc',
        # 'hkland_historycashflow',
        # 'const_industry',
        # 'derived_institution_summary',  # ！！！
        # 'stk_specialtrade',
        # 'hkland_historytradestat',
        # 'index_indexprepcomponent',
        # 'stk_codechangeserial',

        # 'const_secumain',
        # 'const_areacode',
        # 'const_newsconst',
        # 'stk_liststatus',
        # 'comcn_controllingshareholders',

        # 'comcn_actualcontroller',
        # 'comcn_violationhalding',
        # 'const_product',
        # 'comcn_financespecialindex',
        # 'index_basicinfo',
        # 'comcn_msecufinance',

        # 'comcn_issuanceexamination',
        # 'stk_abbrchangeserial',
        # 'stk_secuchange',
        # 'const_industrytype',
        # 'const_ussecumain',

        'comcn_performanceletters',
        'comcn_bankassetsliability',
        'comcn_sharesfloatingschedule',
        'comcn_mainquarterdata',
        'stk_business',
        'comcn_bankloan',
        'comcn_bankincomeexpense',
        'const_systemconst',
        'comcn_rewardstat',
        'derived_institution_detail',
        'comcn_coconcept',
        'news_secu',
        'comcn_bankregulator',
        'comcn_relatedsh',
        'comcn_exgindustry',
        'comcn_sharefpsta',
        'comcn_sharefp',
        'comcn_dividend',
        'comcn_managersstockalteration',
        'comcn_financialreportauditingopinion',
        'comcn_performanceforecast',
        'comcn_sharestru',
        'comcn_dividendprogress',
        'comcn_stockholdingst',
        'comcn_reservereportdate',
        'comcn_guaranteedetail',
        'stk_codechange',
        'const_tradingday',
        'const_hksecumain',
        'const_secumainall',
        'comcn_leaderposition',
        'stk_7percentchange',
        'stk_specialnotice',
        'comcn_qcashflowstatement',
        'comcn_qincomestatement',
        'comcn_qfinancialindex',
        'const_jydbdeleterec',
        'comcn_fsderiveddata',
        'comcn_fspecialindicators',
        'comcn_maindatanew',
        'comcn_cashflowstatementall_jy',
        'comcn_cashflowstatement',
        'comcn_balancesheetall_jy',
        'comcn_balancesheet',
        'comcn_incomestatement',
        'comcn_incomestatementall_jy',
        'comcn_nonrecurringevent',
        'comcn_executivesholdings',
        'comcn_mainoperincome',
        'hkland_shares',
        'index_sywgindexquote',
        'index_quot_day',
        'comcn_equitychangesstatement',
        'comcn_mainshlistnew',
        'trans_valuations',

              ]
    main_check(tables)
