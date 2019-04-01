import random
from mysql import MySql


class RandomCheck:
    def __init__(self, group, mongo, config, logger):
        # self.group = group
        self.group = int(group)
        self.logger = logger
        self.mongo = mongo
        self.config = config

    def check(self, tables):
        suc_tables = list()
        fail_tables = list()
        try:
            for table in tables:
                if self.each_check(table):
                    suc_tables.append(table)
                else:
                    fail_tables.append(table)
        except Exception as e:
            self.logger.warning(f'some thing have been wrong during the check: {e}')
            raise
        self.logger.info(f"The ret of check is success: {suc_tables}, fail: {fail_tables}")
        return fail_tables

    def each_check(self, table):
        positions = self.SystematicSampling(table, self.group)
        self.logger.info(f"positions:  {positions}")

        sql = MySql(self.config, self.logger)
        con = sql.gen_con()
        coll = self.mongo.get_coll(table, 'datacenter')

        heads = sql.gen_sql_head_name_list(con, 'datacenter', table)

        for pos in positions:
            data = sql.gen_mysql_info(con, table, pos)
            dict_data = sql.zip_doc_dict(heads, data)
            final_data = sql.check_each_sql_table_data(dict_data)

            try:
                flag = coll.find(final_data, {"id"}).next()
            except Exception:
                flag = False
            if not flag:
                self.logger.warning(f"未查找到当前的数据： {table} --> {pos} ---> {final_data}")
                return False
        return True

    def SystematicSampling(self, table, group):
        try:
            # coll = self.mongo.get_coll(table, "datacenter")
            # total = self.mongo.gen_count(coll)

            # 应该从pos_log中获取total
            coll = self.mongo.get_coll("pos_log", "datacenter")
            total = coll.find().next().get(table)

            self.logger.info(f"table: {table} , total: {total}")
        except Exception as e:
            self.logger.warning(f"系统抽样中计算总数失败， {e}")
            raise SystemError(e)
        # self.logger.info(f'{total}, {type(total)}')
        # self.logger.info(f'{group}, {type(group)}')
        sub_num = int(total/group)  # 向下取整 防止 index out of range
        samples = list()
        for i in range(group):
            samples.append(self.RandomSampling(sub_num, i))

        # 头尾数据校验
        samples.insert(0, 0)
        samples.insert(0, total - 1)

        self.logger.info(f"samples: {samples}")
        return samples

    def RandomSampling(self, sub_num, i):
        start = i * sub_num
        end = (i + 1) * sub_num - 1
        start, end = (start, end) if start <= end else (end, start)
        try:
            sample = random.randint(start, end)
            return sample
        except Exception as e:
            self.logger.warning(f'简单随机抽样过程失败， {e}')
            raise SystemError(e)
