import pymongo
import urllib.parse
import logging

from pymongo.errors import CollectionInvalid

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('mongodb')


class SysException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class MyMongoDB:

    mdb = None

    def __init__(self, conf):
        # self.logger = logging.getLogger(__name__)
        self.logger = logger
        try:
            password = urllib.parse.quote(conf['password'])
        except Exception as e:
            raise SysException(e)

        if conf['user'] == '':
            conn_string = 'mongodb://' + \
                            conf['host'] + ':' + \
                            conf['port'] + '/'
        else:
            conn_string = 'mongodb://' + \
                            conf['user'] + ':' + \
                            password + '@' + \
                            conf['host'] + ':' + \
                            conf['port'] + '/'
        try:
            self.mdb = pymongo.MongoClient(conn_string, connect=False)
        except Exception as e:
            self.logger.warning(f'创建 pymongo 连接失败， 失败的原因是： {e}')
            raise SysException(e)

    def get_db(self, db_name):
        try:
            db = self.mdb[db_name]
        except:
            try:
                db = self.mdb.get_database(db_name)
            except Exception as e:
                self.logger.warning(f'创建 {db_name} 数据库连接失败， 失败的原因是 {e}')
                raise SysException(e)

        return db

    def get_coll(self, coll_name, db_name):
        db = None

        try:
            db = self.get_db(db_name)
        except Exception as e:
            self.logger.warning(f'创建 {db_name}.{coll_name} 集合连接失败， 失败的原因是 {e}')
            SysException(e)

        try:
            db.create_collection(coll_name)
        except CollectionInvalid as e:
            # 这里不再 logger 因为 已经存在的时候会打印 没必要 ...
            # self.logger.info(str(e))
            pass
        except Exception as e:
            self.logger.warning(f'创建 {db_name}.{coll_name} 集合连接失败， 失败的原因是 {e}')
            raise SysException(e)

        coll = db[coll_name]

        return coll

    def write_log_pos(self, last_pos, cur_pos):
        coll = self.get_coll('pos_log', "datacenter")
        try:
            doc = list(coll.find())
        except Exception as e:
            self.logger.warning(f'写入 log_pos 失败，失败的原因是 {e}')
            raise SysException(e)

        if doc:  # write for the first time
            try:
                self.logger.info("开始更新日志结构： ")
                # coll.replace_one(last_pos, cur_pos)
                coll.update(last_pos, cur_pos, upsert=True)

            except Exception as e:
                self.logger.warning(f'更新日志结构失败，原因 {e}')
                raise SysException(e)
        else:  # rewrite
            try:
                coll.insert_one(cur_pos)
            except Exception as e:
                self.logger.warning(f'首次写入 log_pos 失败，原因 {e}')
                raise SysException(e)

    def get_log_pos(self):
        coll = self.get_coll('pos_log', "datacenter")
        try:
            last_log = coll.find_one()  # pos_log 表格里始终只有一条数据
        except Exception as e:
            self.logger.warning(f"获取 log_pos 信息失败，失败原因 {e}")
            raise SysException(e)

        return last_log if last_log else dict()

    def insert(self, doc, schema, collection):
        coll = self.get_coll(collection, schema)

        try:
            coll.insert_one(doc)
        except Exception as e:
            self.logger.warning(f"insert 失败： {schema}.{collection}-->{doc}")
            self.logger.warning(f"失败原因： {e}")
            raise SysException(e)

    def update(self, doc, schema, collection, primary_key):
        coll = self.get_coll(collection, schema)

        try:
            coll.replace_one(primary_key, doc)
        except Exception as e:
            self.logger.warning(f'update 失败: {schema}.{collection}-->{doc}')
            self.logger.warning(f"失败原因： {e}")
            raise SysException(e)

    def delete(self, schema, collection, doc=None, primary_key=None):
        coll = self.get_coll(collection, schema)

        if primary_key is None:
            try:
                coll.delete_one(doc)
            except Exception as e:
                self.logger.warning(f'delete 失败：{schema}.{collection}-->{doc}')
                self.logger.warning(f"失败原因： {e}")
                raise SysException(e)
        else:
            try:
                self.logger.debug('try to delete doc with key: ' + str(primary_key))
                result = coll.delete_one(primary_key)
                self.logger.debug('delete result: ' + str(result.deleted_count))
            except Exception as e:
                self.logger.warning(f'无 primary_key delete 失败：{schema}.{collection}-->{doc}')
                self.logger.warning(f"失败原因： {e}")
                raise SysException(e)

    def drop_db(self, db_name):
        if db_name in self.mdb.database_names():
            try:
                self.mdb.drop_database(db_name)
            except Exception as e:
                self.logger.warning(f"drop db 失败，原因: {e}")
                raise SysException(e)

    def gen_count(self, coll):
        try:
            count = coll.find().count()
        except Exception as e:
            self.logger.warning(f'gen count 失败，原因是： {e}')
            raise SysException(e)
        return count

    def calibration_last_location(self, last_pos, table_name_list):
        for table_name in table_name_list:
            coll = self.get_coll(table_name, "datacenter")
            count = self.gen_count(coll)
            if count != last_pos.get(table_name):
                last_pos.update({table_name: count})
        return last_pos
