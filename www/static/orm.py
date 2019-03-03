# coding:utf-8

__author__ = 'songyuan'
import asyncio, logging
import aiomysql

def log(sql, *args):
    return logging.info('sql:{}'.format(sql))

async def create_pool(pool, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', '3306'),  # dictionary取值时，若设置默认值，则可用dic.get('','default')
        user=kw.get('user'),
        pwd=kw.get('password'),
        db=kw.get('database'),
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxseize', 10),
        loop=pool
    )

#数据库结果集为集合形式，程序处理数据单位为按行逐记录，cursor可在数据库结果集中移动，逐行返回记录
# select * from arg_table (where arg_col1 = arg_val1..) (sort by arg_col2)
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or())  # args为字典形式传递
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
    logging.info('select {} rows.'.format(len(rs)))
    return rs


# insert into arg_table set arg_col1=arg_val1, arg_col2 = arg_val2
# OR insert into arg_table(arg_col1, arg_col2...) Values(arg_val1, arg_val2...)
# update arg_table set arg_col1=arg_val1, arg_col2=arg_val2 (where)
# delete from arg_table (where)
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await  conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback
            raise
    return

# 将参数列表转化为用,拼接的参数字符串
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)

# 字段的基类,字段的属性有字段名、字段类型、是否主键（是否自增长？）, 默认值
class Field(object):
    def __init__(self, name, type, primary_key, default): #default??
        self.name = name
        self.type = type
        self.primary_key = primary_key
        self.default = default

    #????
    def __str__(self):
        return '<{},{}:{}>'.format(self.__class.__name__, self.type, self.name )

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, 'varchar(100)', primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=None, default='0.0'):
        super(self).__init__(name, 'float', primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=None, default='0'):
        super(self).__init__(name, 'bigint', primary_key, default)

class textField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

#?????
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model::{} (table:{})'.format(name, tableName))
        mappings = dict()
        fields = []