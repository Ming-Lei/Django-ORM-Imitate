# coding: utf-8
import MySQLdb
# py2 mysql-python  py3 mysqlclient


# 数据库调用
class Database():
    autocommit = True
    conn = {}
    db_config = {}

    @classmethod
    def connect(cls, **databases):
        for db_label, db_config in databases.items():
            cls.conn[db_label] = MySQLdb.connect(host=db_config.get('host', 'localhost'),
                                                 port=int(db_config.get('port', 3306)),
                                                 user=db_config.get('user', 'root'),
                                                 passwd=db_config.get('password', ''),
                                                 db=db_config.get('database', 'test'),
                                                 charset=db_config.get('charset', 'utf8'))
            cls.conn[db_label].autocommit(cls.autocommit)
        cls.db_config.update(databases)

    @classmethod
    def get_conn(cls, db_label):
        if not cls.conn[db_label] or not cls.conn[db_label].open:
            cls.connect(**cls.db_config)
        try:
            cls.conn[db_label].ping()
        except MySQLdb.OperationalError:
            cls.connect(**cls.db_config)
        return cls.conn[db_label]

    @classmethod
    def execute(cls, db_label, *args):
        cursor = cls.get_conn(db_label).cursor()
        cursor.execute(*args)
        return cursor

    def __del__(self):
        for _, conn in self.conn:
            if conn and conn.open:
                conn.close()


def execute_raw_sql(db_label, sql, params=None):
    return Database.execute(db_label, sql, params) if params else Database.execute(db_label, sql)


# 可重复的迭代器
class SqlIterator():
    def __init__(self, data, model):
        self.data = data
        self.model = model
        self.ind = 0
        self.fields_list = list(self.model.fields.keys())

    def __iter__(self):
        return self
    
    def next(self):
        return self.__next__()

    def __next__(self):
        if self.ind == len(self.data):
            raise StopIteration
        else:
            value = self.data[self.ind]
            self.ind += 1
            # 返回实例化的model对象
            inst = self.model(**dict(zip(self.fields_list, value)))
            return inst


class QuerySet():
    def __init__(self, model):
        self.model = model
        self.fields_list = list(self.model.fields.keys())

        self.filter_dict = {}
        self.exclude_dict = {}
        self.order_fields = []
        self.limit_dict = {}

        self.select_result = None
        self.select_count = None

    # filter函数，返回一个新的QuerySet对象
    def filter(self, **kwargs):
        return self.new(filter_dict=kwargs)

    # exclude函数，返回一个新的QuerySet对象
    def exclude(self, **kwargs):
        return self.new(exclude_dict=kwargs)
    
    # first
    def first(self):
        if self.select_result is None:
            # 没有查询结果 使用fetchone获取第一个
            sql, params = self.sql_expr()
            value = Database.execute(self.model.db_label, sql, params).fetchone()
        else:
            # 有查询结果 直接返回第一个
            value = self.select_result[0] if self.select_result else None
        if value:
            # 返回实例化的model对象
            inst = self.model(**dict(zip(self.fields_list, value)))
            return inst
        return None

    # count
    def count(self):
        if self.select_count is None:
            if self.limit_dict:
                # 有数量限制，取得查询结果的长度
                self.select()
                self.select_count = len(self.select_result)
            else:
                # 无数量限制，使用count查询
                sql, params = self.sql_expr(count=True)
                (self.select_count,) = Database.execute(self.model.db_label, sql, params).fetchone()
        return self.select_count
    
    # update
    def update(self, **kwargs):
        if kwargs:
            sql, params = self.sql_expr(update_dict=kwargs)
            Database.execute(self.model.db_label, sql, params)

    # order_by函数，返回一个新的QuerySet对象
    def order_by(self, *args):
        return self.new(order_fields=args)
    
    # exists
    def exists(self):
        return bool(self.count())

    # sql查询基础函数
    def select(self):
        if self.select_result is None:
            sql, params = self.sql_expr()
            self.select_result = Database.execute(self.model.db_label, sql, params).fetchall()

    # 根据当前筛选条件构建sql、params
    def sql_expr(self, count=False, update_dict=None):
        params = []
        where_expr = ''

        if self.filter_dict or self.exclude_dict:
            where_expr += ' where '

        if self.filter_dict:
            params.extend(self.filter_dict.values())
            equations = [key + ' = %s' for key in self.filter_dict.keys()]
            where_expr += '(' + ' and '.join(equations) + ')'

        if self.exclude_dict:
            params.extend(self.exclude_dict.values())
            equations = [key + ' = %s' for key in self.exclude_dict.keys()]
            where_expr += ' and not (' + ' and '.join(equations) + ')'
        
        if self.order_fields:
            where_expr += ' order by '
            order_list = []
            for field in self.order_fields:
                if field[0] == '-':
                    field_name = field[1:]
                    order_list.append(field_name + ' desc ')
                else:
                    order_list.append(field)
            where_expr += ' , '.join(order_list)

        if update_dict and self.limit_dict:
            # todo 报错 无法更新
            pass

        # limit加count不生效
        if not count:
            limit = self.limit_dict.get('limit')
            if limit is not None:
                where_expr += ' limit %s '
                params.append(limit)
            offset = self.limit_dict.get('offset')
            if offset is not None:
                where_expr += ' offset %s '
                params.append(offset)

        if count:
            sql = 'select count(*) from %s %s;' % (self.model.db_table, where_expr)
        elif update_dict:
            _keys = []
            _params = []
            for key, val in update_dict.items():
                if key not in self.fields_list:
                    continue
                _keys.append(key)
                _params.append(val)
            params = _params + params
            sql = 'update %s set %s %s;' % (
                self.model.db_table, ', '.join([key + ' = %s' for key in _keys]), where_expr)
        else:
            sql = 'select %s from %s %s;' % (', '.join(self.fields_list), self.model.db_table, where_expr)
        return sql, params

    # 根据传入的筛选条件，返回新的QuerySet对象
    def new(self, filter_dict={}, exclude_dict={}, limit_dict={}, order_fields=None):
        new_query = QuerySet(self.model)

        new_query.filter_dict.update(self.filter_dict)
        if filter_dict:
            new_query.filter_dict.update(filter_dict)

        new_query.exclude_dict.update(self.exclude_dict)
        if exclude_dict:
            new_query.exclude_dict.update(exclude_dict)

        new_query.limit_dict.update(self.limit_dict)
        if limit_dict:
            new_query.limit_dict.update(limit_dict)
        
        new_query.order_fields = self.order_fields[:]
        if order_fields:
            new_query.order_fields = order_fields

        return new_query

    # 自定义切片及索引取值
    def __getitem__(self, index):
        if isinstance(index, slice):
            # 根据当前偏移量计算新的偏移量
            start = index.start or 0
            stop = index.stop
            self_offset = self.limit_dict.get('offset', 0)
            self_limit = self.limit_dict.get('limit')

            limit = None
            sffset = self_offset + start
            if stop is not None:
                limit = stop - start

                if self_limit and sffset > self_offset + self_limit:
                    sffset = self_offset
                    limit = 0
                elif self_limit and sffset + limit > self_offset + self_limit:
                    limit = self_offset + self_limit - sffset

            limit_dict = {}
            limit_dict['offset'] = sffset
            if limit:
                limit_dict['limit'] = limit
            # 返回新的QuerySet对象
            return self.new(limit_dict=limit_dict)
        elif isinstance(index, int):
            # 取得对应索引值
            self.select()
            value = self.select_result[index]
            # 返回实例化的model对象
            inst = self.model(**dict(zip(self.fields_list, value)))
            return inst
        else:
            return None

    # 返回自定义迭代器
    def __iter__(self):
        self.select()
        return SqlIterator(self.select_result, self.model)
    
    def __nonzero__(self):
        return bool(self.count())

    def __bool__(self):
        return bool(self.count())

    def __repr__(self):
        return '<QuerySet Obj>'


class Field():
    def __nonzero__(self):
        return False

    def __bool__(self):
        return False


class MetaModel(type):
    db_table = None
    fields = {}

    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        fields = {}
        for key, val in cls.__dict__.items():
            if isinstance(val, Field):
                fields[key] = val
        cls.fields = fields
        cls.attrs = attrs
        cls.objects = QuerySet(cls)


def with_metaclass(meta, *bases):
    # 兼容2和3的元类  见 py2 future.utils.with_metaclass
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return metaclass('temporary_class', None, {})


class Model(with_metaclass(MetaModel, dict)):

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

    def __nonzero__(self):
        return bool(self.__dict__)

    def __bool__(self):
        return bool(self.__dict__)

    def __eq__(self, obj):
        return self.__class__ == obj.__class__ and self.__dict__ == obj.__dict__

    def __hash__(self):
        kv_list = sorted(self.__dict__.items(), key=lambda x: x[0])
        return hash(','.join(['"%s":"%s"' % x for x in kv_list]) + str(self.__class__))

    def save(self):
        insert = 'insert ignore into %s(%s) values (%s);' % (
            self.db_table, ', '.join(self.__dict__.keys()), ', '.join(['%s'] * len(self.__dict__)))
        return Database.execute(self.db_label, insert, self.__dict__.values())
