# coding: utf-8
import MySQLdb
# py2 mysql-python  py3 mysqlclient


class Field():
    def __init__(self, **kw):
        self.primary_key = kw.get('primary_key', False)


class Q():
    def __init__(self, *args, **kwargs):
        self.children = list(args) + list(kwargs.items())
        self.connector = 'AND'
        self.negated = False

    # 添加Q对象
    def add(self, data, conn):
        if not isinstance(data, Q):
            raise TypeError(data)
        if self.connector == conn:
            if not data.negated and (data.connector == conn or len(data) == 1):
                self.children.extend(data.children)
            else:
                self.children.append(data)
        else:
            obj = Q()
            obj.connector = conn
            obj.children = self.children[:]
            self.children = [obj, data]

    def _combine(self, other, conn):
        if not isinstance(other, Q):
            raise TypeError(other)
        obj = Q()
        obj.connector = conn
        obj.add(self, conn)
        obj.add(other, conn)
        return obj

    # 重载 |
    def __or__(self, other):
        return self._combine(other, 'OR')

    # 重载 &
    def __and__(self, other):
        return self._combine(other, 'AND')

    # 重载 ~
    def __invert__(self):
        obj = Q()
        obj.add(self, 'AND')
        obj.negated = not self.negated
        return obj

    # 构建sql查询语句
    def _sql_expr(self):
        sql_list = []
        params = []
        for child in self.children:
            if not isinstance(child, Q):
                temp_sql, temp_params = self.magic_query(child)
                sql_list.append(temp_sql)
                params.extend(temp_params)
            else:
                temp_sql, temp_params = child._sql_expr()
                if temp_sql and temp_params:
                    raw_sql = child.connector.join(temp_sql)
                    if child.negated:
                        raw_sql = ' not ( ' + raw_sql + ' ) '
                    elif child.connector != self.connector:
                        raw_sql = ' ( ' + raw_sql + ' ) '
                    sql_list.append(raw_sql)
                    params.extend(temp_params)
        return sql_list, params

    # 取得对应sql及参数
    def sql_expr(self):
        sql_list, params = self._sql_expr()
        return self.connector.join(sql_list), params

    # 处理双下划线特殊查询
    def magic_query(self, child_query):
        correspond_dict = {
            '': ' = %s ',
            'gt': ' > %s ',
            'gte': ' >= %s ',
            'lt': ' < %s ',
            'lte': ' <= %s ',
            'contains': ' like %%%s%% ',
            'startswith': ' like %s%% ',
            'endswith': ' like %%%s ',
        }

        raw_sql = ''
        params = []
        query_str, value = child_query
        if '__' in query_str:
            field, magic = query_str.split('__')
        else:
            field = query_str
            magic = ''
        temp_sql = correspond_dict.get(magic)
        if temp_sql:
            raw_sql = ' ' + field + temp_sql
            params = [value]
        elif magic == 'isnull':
            if value:
                raw_sql = ' ' + field + ' is null '
            else:
                raw_sql = ' ' + field + ' is not null '
        elif magic == 'range':
            raw_sql = ' ' + field + ' between %s and %s '
            params = value
        elif magic == 'in':
            if isinstance(value, (ValuesListQuerySet, ValuesQuerySet)):
                subquery = value.query.clone()
                if len(subquery.select) != 1:
                    raise TypeError('Cannot use a multi-field %s as a filter value.'
                                    % value.__class__.__name__)
                sub_sql, sub_params = subquery.sql_expr()
                raw_sql = ' ' + field + ' in ( ' + sub_sql[:-1] + ' ) '
                params = sub_params
            elif isinstance(value, QuerySet):
                primary_key = value.model.__primary_key__
                if not primary_key:
                    raise TypeError('Primary key not defined in class: %s' % value.model.__class__.__name__)
                subquery = value.query.clone()
                subquery.select = [primary_key]
                sub_sql, sub_params = subquery.sql_expr()
                raw_sql = ' ' + field + ' in ( ' + sub_sql[:-1] + ' ) '
                params = sub_params
            else:
                if len(value) == 0:
                    raw_sql = ' False '
                    params = []
                else:
                    raw_sql = ' ' + field + ' in %s '
                    params = [tuple(value)]

        return raw_sql, params

    def __len__(self):
        return len(self.children)

    def __nonzero__(self):
        return bool(self.children)

    def __bool__(self):
        return bool(self.children)

    def __repr__(self):
        if self.negated:
            return '(NOT (%s: %s))' % (self.connector, ', '.join([str(c) for c
                                                                  in self.children]))
        return '(%s: %s)' % (self.connector, ', '.join([str(c) for c in
                                                        self.children]))


class Query():
    def __init__(self, model):
        self.model = model
        self.fields_list = self.model.field_list

        self.flat = False
        self.filter_Q = Q()
        self.exclude_Q = Q()
        self.limit_dict = {}
        self.order_fields = []
        self.select = self.fields_list

    def __str__(self):
        sql, params = self.sql_expr()
        return sql % params

    # 根据当前筛选条件构建sql、params
    def sql_expr(self, method='select', update_dict=None):
        params = []
        where_expr = ''

        if self.filter_Q or self.exclude_Q:
            where_expr += ' where '

        if self.filter_Q:
            temp_sql, temp_params = self.filter_Q.sql_expr()
            where_expr += '(' + temp_sql + ')'
            params.extend(temp_params)

        if self.exclude_Q:
            temp_sql, temp_params = self.exclude_Q.sql_expr()
            if params:
                where_expr += ' and '
            where_expr += ' not (' + temp_sql + ')'
            params.extend(temp_params)

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
            raise TypeError('Cannot update a query once a slice has been taken.')

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
            sql = 'select count(*) from %s %s;' % (self.model.__db_table__, where_expr)
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
                self.model.__db_table__, ', '.join([key + ' = %s' for key in _keys]), where_expr)
        elif method == 'delete':
            sql = 'delete from %s %s;' % (self.model.__db_table__, where_expr)
        else:
            sql = 'select %s from %s %s;' % (', '.join(self.select), self.model.__db_table__, where_expr)
        return sql, tuple(params)

    # clone
    def clone(self):
        obj = Query(self.model)
        obj.filter_Q = self.filter_Q
        obj.exclude_Q = self.exclude_Q
        obj.order_fields = self.order_fields[:]
        obj.limit_dict.update(self.limit_dict)
        obj.select = self.select[:]
        return obj


class QuerySet(object):
    def __init__(self, model, query=None):
        self.model = model
        self.select_result = None
        self.query = query or Query(model)
        self.fields_list = self.model.field_list

    # all函数，返回一个新的QuerySet对象（无筛选条件）
    def all(self):
        return self._clone()

    # filter函数，返回一个新的QuerySet对象
    def filter(self, *args, **kwargs):
        return self._filter_or_exclude(False, *args, **kwargs)

    # exclude函数，返回一个新的QuerySet对象
    def exclude(self, *args, **kwargs):
        return self._filter_or_exclude(True, *args, **kwargs)

    # first
    def first(self):
        try:
            return self.get_index(0)
        except IndexError:
            return None

    # count
    def count(self):
        if self.select_result is not None:
            return len(self.select_result)

        # limit查询特殊处理
        limit = self.query.limit_dict.get('limit', 0)
        offset = self.query.limit_dict.get('offset', 0)
        if limit or offset:
            # 构建无limit_dict的query
            count_query = self._clone()
            count_query.query.limit_dict = {}
            all_count = count_query.count()
            # 根据实际数量及偏移量计算count
            if offset > all_count:
                select_count = 0
            elif limit == 0 or offset + limit > all_count:
                select_count = all_count - offset
            else:
                select_count = limit
        else:
            # 无数量限制，使用count查询
            sql, params = self.query.sql_expr(method='count')
            (select_count,) = Database.execute(self.model.__db_label__, sql, params).fetchone()
        return select_count

    # update
    def update(self, **kwargs):
        if kwargs:
            _, kwargs = self.pk_replace(**kwargs)
            sql, params = self.query.sql_expr(method='update', update_dict=kwargs)
            Database.execute(self.model.__db_label__, sql, params)

    # order_by函数，返回一个新的QuerySet对象
    def order_by(self, *args):
        obj = self._clone()
        args, _ = self.pk_replace(*args)
        obj.query.order_fields = args
        return obj

    # create
    def create(self, **kwargs):
        obj = self.model(**kwargs)
        obj.save()
        return obj

    # exists
    def exists(self):
        return bool(self.count())

    # delete
    def delete(self):
        sql, params = self.query.sql_expr(method='delete')
        Database.execute(self.model.__db_label__, sql, params)

    # values
    def values(self, *args):
        fields_list = self.field_check(args)
        return self._clone(ValuesQuerySet, fields_list)

    # values_list
    def values_list(self, *args, **kwargs):
        # 字段检查
        fields_list = self.field_check(args)
        flat = kwargs.pop('flat', False)
        # flat 只能返回一个字段列表
        if flat and len(args) > 1:
            raise TypeError('flat is not valid when values_list is called with more than one field.')

        return self._clone(ValuesListQuerySet, fields_list, flat)

    # 字段检查
    def field_check(self, fields_list):
        err_fields = set(fields_list) - set(self.fields_list)
        primary_key = self.model.__primary_key__
        if 'pk' in err_fields:
            if not primary_key:
                raise TypeError('Primary key not defined in class: %s' % self.model.__class__.__name__)
            err_fields = err_fields - {'pk'}
        if err_fields:
            raise TypeError('Cannot resolve keyword %s into field.' % list(err_fields)[0])

        fields_list, _ = self.pk_replace(*fields_list)
        # 没有传入指定字段，返回全部
        if not fields_list:
            fields_list = self.fields_list
        return fields_list

    def pk_replace(self, *args, **kwargs):
        primary_key = self.model.__primary_key__
        if not primary_key:
            return args, kwargs

        if 'pk' in args:
            pk_index = args.index('pk')
            args = args[:pk_index] + (primary_key,) + args[pk_index + 1:]
        if '-pk' in args:
            pk_index = args.index('-pk')
            args = args[:pk_index] + ('-' + primary_key,) + args[pk_index + 1:]
        if 'pk' in kwargs:
            kwargs[primary_key] = kwargs['pk']
            del kwargs['pk']
        return args, kwargs

    # sql查询基础函数
    def select(self):
        if self.select_result is None:
            sql, params = self.query.sql_expr()
            self.select_result = Database.execute(self.model.__db_label__, sql, params).fetchall()

    def base_index(self, index):
        if self.select_result is None:
            index_query = self[index:index + 1]
            index_query.select()
            index_value = index_query.select_result[0]
        else:
            index_value = self.select_result[index]
        return index_value

    # 索引值查询
    def get_index(self, index):
        index_value = self.base_index(index)
        return self.model(**dict(zip(self.fields_list, index_value)))

    def _clone(self, klass=None, select=None, flat=False):
        if klass is None:
            klass = self.__class__
        query = self.query.clone()
        if select:
            query.select = select[:]
        if flat:
            query.flat = flat
        obj = klass(model=self.model, query=query)
        return obj

    # 根据传入的筛选条件，返回新的QuerySet对象
    def _filter_or_exclude(self, negate, *args, **kwargs):
        clone = self._clone()
        new_q = self._add_q(Q(*args, **kwargs))
        if negate:
            clone.query.exclude_Q.add(new_q, 'AND')
        else:
            clone.query.filter_Q.add(new_q, 'AND')
        return clone

    def _add_q(self, q_object):
        connector = q_object.connector
        new_q = Q()
        new_q.connector = connector
        for child in q_object.children:
            if isinstance(child, Q):
                temp_q = self._add_q(child)
                new_q.add(temp_q, connector)
            else:
                new_child = self.build_filter(child)
                new_q.add(Q(new_child), connector)
        return new_q

    def build_filter(self, filter_expr):
        arg, value = filter_expr
        lookup_splitted = arg.split('__')
        if lookup_splitted[0] == 'pk':
            lookup_splitted[0] = self.model.__primary_key__
        return '__'.join(lookup_splitted), value

    # 自定义切片及索引取值
    def __getitem__(self, index):
        if isinstance(index, slice):
            obj = self._clone()
            # 根据当前偏移量计算新的偏移量
            start = index.start or 0
            stop = index.stop
            self_offset = obj.query.limit_dict.get('offset', 0)
            self_limit = obj.query.limit_dict.get('limit')

            limit = None
            offset = self_offset + start
            if stop is not None:
                limit = stop - start

                if self_limit and offset > self_offset + self_limit:
                    offset = self_offset
                    limit = 0
                elif self_limit and offset + limit > self_offset + self_limit:
                    limit = self_offset + self_limit - offset

            obj.query.limit_dict['offset'] = offset
            if limit:
                obj.query.limit_dict['limit'] = limit
            # 返回新的QuerySet对象
            return obj
        elif isinstance(index, int):
            if index < 0:
                raise TypeError('Negative indexing is not supported.')
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


class ValuesQuerySet(QuerySet):

    def __iter__(self):
        self.select()
        for value in self.select_result:
            inst = {field: value[index] for index, field in enumerate(self.query.select)}
            yield inst

    def get_index(self, index):
        index_value = self.base_index(index)
        return {field: index_value[f_index] for f_index, field in enumerate(self.query.select)}

    def __repr__(self):
        return '<ValuesQuerySet Obj>'


class ValuesListQuerySet(QuerySet):
    def __init__(self, *args, **kwargs):
        super(ValuesListQuerySet, self).__init__(*args, **kwargs)
        self.flat = self.query.flat
        self.select_field = self.query.select
        if self.flat and len(self.select_field) != 1:
            raise TypeError('flat is not valid when values_list is called with more than one field.')

    def __iter__(self):
        self.select()
        for value in self.select_result:
            if self.flat:
                yield value[0]
            else:
                yield value

    def get_index(self, index):
        index_value = self.base_index(index)
        if self.flat:
            return index_value[0]
        else:
            return index_value

    def __repr__(self):
        return '<ValuesListQuerySet Obj>'


class Manager():
    def __init__(self, model):
        self.model = model

    def get_queryset(self):
        return QuerySet(self.model)

    def all(self):
        return self.get_queryset()

    def count(self):
        return self.get_queryset().count()

    def filter(self, *args, **kwargs):
        return self.get_queryset().filter(*args, **kwargs)

    def exclude(self, *args, **kwargs):
        return self.get_queryset().exclude(*args, **kwargs)

    def first(self):
        return self.get_queryset().first()

    def exists(self):
        return self.get_queryset().exists()

    def create(self, **kwargs):
        return self.get_queryset().create(**kwargs)

    def order_by(self, *args):
        return self.get_queryset().order_by(*args)

    def values(self, *args):
        return self.get_queryset().values(*args)

    def values_list(self, *args):
        return self.get_queryset().values_list(*args)


class MetaModel(type):
    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        if name == 'Model':
            return

        __db_table__ = attrs.get('__db_table__')
        if not __db_table__:
            raise TypeError('__db_table__ is not defined in %s ' % name)

        field_list = []
        primary_key = None
        for key, val in cls.__dict__.items():
            if isinstance(val, Field):
                if val.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    primary_key = key
                field_list.append(key)
                setattr(cls, key, None)
        cls.field_list = field_list
        cls.attrs = attrs
        cls.objects = Manager(cls)
        cls.__primary_key__ = primary_key


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
            if k in self.field_list:
                setattr(self, k, v)
            elif k == 'pk' and self.__primary_key__:
                self._set_pk_val(v)
            else:
                raise TypeError("'%s' is an invalid keyword argument for this function" % k)

    def _get_pk_val(self):
        primary_key = self.__primary_key__
        if not primary_key:
            return None
        return getattr(self, primary_key)

    def _set_pk_val(self, value):
        primary_key = self.__primary_key__
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % self.__class__.__name__)
        return setattr(self, primary_key, value)

    pk = property(_get_pk_val, _set_pk_val)

    def __repr__(self):
        return '<%s obj>' % self.__class__.__name__

    def __nonzero__(self):
        return bool(self.__dict__)

    def __bool__(self):
        return bool(self.__dict__)

    def __eq__(self, obj):
        return self.__class__ == obj.__class__ and self.__dict__ == obj.__dict__

    def __hash__(self):
        kv_list = sorted(self.__dict__.items(), key=lambda x: x[0])
        return hash(','.join(['"%s":"%s"' % x for x in kv_list]) + str(self.__class__))

    def _insert(self):
        insert = 'insert into %s(%s) values (%s);' % (
            self.__db_table__, ', '.join(self.__dict__.keys()), ', '.join(['%s'] * len(self.__dict__)))
        cursor = Database.execute(self.__db_label__, insert, self.__dict__.values())
        if self.__primary_key__:
            last_rowid = cursor.lastrowid
            self._set_pk_val(last_rowid)

    def save(self):
        if not self.__primary_key__ or not self.pk:
            self._insert()
        else:
            cls = self.__class__
            filtered = cls.objects.filter(pk=self.pk)
            if filtered.exists():
                temp_dict = dict({}, **self.__dict__)
                del temp_dict[self.__primary_key__]
                filtered.update(**temp_dict)
            else:
                self._insert()


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
        db_conn = cls.get_conn(db_label)
        cursor = db_conn.cursor()
        cursor.execute(*args)
        return cursor

    def __del__(self):
        for _, conn in self.conn:
            if conn and conn.open:
                conn.close()


def execute_raw_sql(db_label, sql, params=None):
    return Database.execute(db_label, sql, params) if params else Database.execute(db_label, sql)
