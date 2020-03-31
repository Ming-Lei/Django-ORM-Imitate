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
    def execute(cls, db_label, *args, **kwargs):
        db_conn = cls.get_conn(db_label)
        cursor = db_conn.cursor()
        cursor.execute(*args)
        delete = kwargs.pop('delete', False)
        if delete:
            db_conn.commit()
        return cursor

    def __del__(self):
        for _, conn in self.conn:
            if conn and conn.open:
                conn.close()


def execute_raw_sql(db_label, sql, params=None):
    return Database.execute(db_label, sql, params) if params else Database.execute(db_label, sql)


class Error(Exception):
    pass


class QuerySet():
    def __init__(self, model):
        self.model = model
        self.fields_list = list(self.model.fields.keys())

        self.filter_dict = {}
        self.exclude_dict = {}
        self.order_fields = []
        self.limit_dict = {}

        self.select_result = None
    
    # all函数，返回一个新的QuerySet对象（无筛选条件）
    def all(self):
        return QuerySet(self.model)

    # filter函数，返回一个新的QuerySet对象
    def filter(self, **kwargs):
        return self.new(filter_dict=kwargs)

    # exclude函数，返回一个新的QuerySet对象
    def exclude(self, **kwargs):
        return self.new(exclude_dict=kwargs)
    
    # first
    def first(self):
        return self.get_index(0)

    # count
    def count(self):
        if self.select_result is not None:
            return len(self.select_result)

        # limit查询特殊处理
        limit = self.limit_dict.get('limit', 0)
        offset = self.limit_dict.get('offset', 0)
        if limit or offset:
            # 构建无limit_dict的query
            count_query = self.new(limit_dict={'limit': None, 'offset': None})
            all_count = count_query.count()
            # 根据实际数量及偏移量计算count
            if offset > all_count:
                select_count = 0
            elif offset + limit > all_count:
                select_count = all_count - offset
            else:
                select_count = limit
        else:
            # 无数量限制，使用count查询
            sql, params = self.sql_expr(method='count')
            (select_count,) = Database.execute(self.model.db_label, sql, params).fetchone()
        return select_count
    
    # update
    def update(self, **kwargs):
        if kwargs:
            sql, params = self.sql_expr(method='update', update_dict=kwargs)
            Database.execute(self.model.db_label, sql, params)

    # order_by函数，返回一个新的QuerySet对象
    def order_by(self, *args):
        return self.new(order_fields=args)
    
    # exists
    def exists(self):
        return bool(self.count())
    
    # delete
    def delete(self):
        sql, params = self.sql_expr(method='delete')
        Database.execute(self.model.db_label, sql, params, delete=True)
    
    # values
    def values(self, *args):
        # 字段检查
        err_fields = set(args) - set(self.fields_list)
        if err_fields:
            raise Error('Cannot resolve keyword %s into field.' % list(err_fields)[0])
        
        if not args:
            args = self.fields_list
        self.select()
        return ({y: getattr(x, y) for y in args} for x in self)

    # values_list
    def values_list(self, *args, **kwargs):
        # 字段检查
        err_fields = set(args) - set(self.fields_list)
        if err_fields:
            raise Error('Cannot resolve keyword %s into field.' % list(err_fields)[0])

        flat = kwargs.pop('flat', False)
        # flat 只能返回一个字段列表
        if flat and len(args) > 1:
            raise Error('flat is not valid when values_list is called with more than one field.')
        
        self.select()
        # 返回指定一个字段对应的迭代器
        if flat and len(args) == 1:
            values_field = args[0]
            return (getattr(x, values_field) for x in self)
        # 没有传入指定字段，返回全部
        if not args:
            args = self.fields_list
        return ([getattr(x, y) for y in args] for x in self)

    # query 查询语句
    @property
    def query(self):
        sql, params = self.sql_expr()
        return sql % tuple(params)

    # sql查询基础函数
    def select(self):
        if self.select_result is None:
            sql, params = self.sql_expr()
            self.select_result = Database.execute(self.model.db_label, sql, params).fetchall()

    # 根据当前筛选条件构建sql、params
    def sql_expr(self, method='select', update_dict=None):
        params = []
        where_expr = ''

        if self.filter_dict or self.exclude_dict:
            where_expr += ' where '

        if self.filter_dict:
            # todo 处理双下划线
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
            # 不支持切片更新
            raise Error('Cannot update a query once a slice has been taken.')        

        # limit
        limit = self.limit_dict.get('limit')
        if limit is not None:
            where_expr += ' limit %s '
            params.append(limit)
        offset = self.limit_dict.get('offset')
        if offset is not None:
            where_expr += ' offset %s '
            params.append(offset)

        # 构建不同操作的sql语句
        if method == 'count':
            sql = 'select count(*) from %s %s;' % (self.model.db_table, where_expr)
        elif method == 'update' and update_dict:
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
        elif method == 'delete':
            sql = 'delete from %s %s;' % (self.model.db_table, where_expr)
        else:
            sql = 'select %s from %s %s;' % (', '.join(self.fields_list), self.model.db_table, where_expr)
        return sql, params

    # 索引值查询
    def get_index(self, index):
        if self.select_result is None:
            index_query = self[index:index+1]
            index_query.select()
            index_value = index_query.select_result[0]
        else:
            index_value = self.select_result[index]
        return self.model(**dict(zip(self.fields_list, index_value)))

    # 根据传入的筛选条件，返回新的QuerySet对象
    def new(self, filter_dict=None, exclude_dict=None, limit_dict=None, order_fields=None):
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
            if index < 0:
                raise Error('Negative indexing is not supported.')
            # 取得对应索引值
            return self.get_index(index)
        else:
            return None

    # 返回自定义迭代器
    def __iter__(self):
        self.select()
        for value in self.select_result:
            inst = self.model(**dict(zip(self.fields_list, value)))
            yield inst
    
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
