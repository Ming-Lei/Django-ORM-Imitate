# coding: utf-8
from functools import partial
import pymysql


class Aggregate:
    func = '%s'

    def __init__(self, field):
        self.field = field

    def sql_expr(self, field_func):
        return self.func % field_func(self.field)


class Avg(Aggregate):
    func = 'avg(%s)'


class Count(Aggregate):
    func = 'count(%s)'

    def __init__(self, field, distinct=False):
        super(Count, self).__init__(field=field)
        if distinct:
            self.func = 'count(distinct %s)'


class Max(Aggregate):
    func = 'max(%s)'


class Min(Aggregate):
    func = 'min(%s)'


class Sum(Aggregate):
    func = 'sum(%s)'


class Field:
    def __init__(self, **kw):
        self.primary_key = kw.get('primary_key', False)
        self.db_column = kw.get('db_column', None)
        self.name = kw.get('name', None)


class ForeignKey(Field):
    def __init__(self, **kw):
        super(ForeignKey, self).__init__(**kw)
        self.to_model = kw.get('to')
        self.to_field = kw.get('to_field', 'pk')


class Combinable:

    @staticmethod
    def _combine(lhs, connector, rhs):
        return CombinedExpression(lhs, connector, rhs)

    def __add__(self, other):
        return self._combine(self, ' + ', other)

    def __sub__(self, other):
        return self._combine(self, ' - ', other)

    def __mul__(self, other):
        return self._combine(self, ' * ', other)

    def __truediv__(self, other):
        return self._combine(self, ' / ', other)

    def __mod__(self, other):
        return self._combine(self, ' %% ', other)

    def __pow__(self, other):
        return self._combine(self, ' ~ ', other)

    def __radd__(self, other):
        return self._combine(other, ' + ', self)

    def __rsub__(self, other):
        return self._combine(other, ' - ', self)

    def __rmul__(self, other):
        return self._combine(other, ' * ', self)

    def __rtruediv__(self, other):
        return self._combine(other, ' / ', self)

    def __rmod__(self, other):
        return self._combine(other, ' %% ', self)

    def __rpow__(self, other):
        return self._combine(other, ' ^ ', self)


class F(Combinable):
    def __init__(self, name):
        self.name = name


class CombinedExpression(Combinable):
    def __init__(self, lhs, connector, rhs):
        self.connector = connector
        self.lhs = lhs
        self.rhs = rhs


class Q:
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

    def __len__(self):
        return len(self.children)

    def __bool__(self):
        return bool(self.children)

    def __repr__(self):
        template = '(NOT (%s: %s))' if self.negated else '(%s: %s)'
        return template % (self.connector, ', '.join(str(c) for c in self.children))


class WhereNode:
    def __init__(self, model):
        self.model = model
        self.filter_Q = Q()

    def as_sql(self):
        params = []
        where_expr = ''

        if self.filter_Q:
            join_sql, temp_sql, temp_params = self.sql_expr(self.filter_Q)
            where_expr += join_sql + ' where ' + temp_sql
            params.extend(temp_params)

        return where_expr, params

    # 构建sql查询语句
    def _sql_expr(self, q_query):
        params = []
        sql_list = []
        join_set = set()
        for child in q_query.children:
            if not isinstance(child, Q):
                temp_set, temp_sql, temp_params = self.magic_query(child)
                join_set.update(temp_set)
                sql_list.append(temp_sql)
                params.extend(temp_params)
            else:
                temp_set, temp_sql, temp_params = self._sql_expr(child)
                if temp_sql:
                    join_set.update(temp_set)
                    if child.negated:
                        temp_sql = [' not ' + x for x in temp_sql]
                    raw_sql = child.connector.join(temp_sql)
                    if child.connector != q_query.connector:
                        raw_sql = ' ( ' + raw_sql + ' ) '
                    sql_list.append(raw_sql)
                    params.extend(temp_params)
        return join_set, sql_list, params

    # 取得对应sql及参数
    def sql_expr(self, q_query):
        join_set, sql_list, params = self._sql_expr(q_query)
        return ' '.join(join_set), q_query.connector.join(sql_list), params

    def f_expr(self, value):
        if isinstance(value, F):
            fields_list, _ = ModelCheck(self.model).field_wash([value.name])
            return self.model.field_info(fields_list[0]), []
        params = []
        raw_sql_list = []
        for temp_f in [value.lhs, value.rhs]:
            if isinstance(temp_f, (F, CombinedExpression)):
                temp_sql, temp_params = self.f_expr(temp_f)
                if hasattr(temp_f, 'connector') and temp_f.connector != value.connector:
                    temp_sql = '(' + temp_sql + ')'
            else:
                temp_sql, temp_params = '%s', [temp_f]
            raw_sql_list.append(temp_sql)
            params.extend(temp_params)
        raw_sql = value.connector.join(raw_sql_list)
        return raw_sql, params

    # 处理双下划线特殊查询
    def magic_query(self, child_query):
        correspond_dict = {
            '': ' = %s ',
            'gt': ' > %s ',
            'gte': ' >= %s ',
            'lt': ' < %s ',
            'lte': ' <= %s ',
            'contains': ' like CONCAT("%%", %s, "%%") ',
            'startswith': ' like CONCAT(%s, "%%") ',
            'endswith': ' like CONCAT("%%", %s) ',
        }
        raw_sql = ''
        params = []
        join_set = set()
        temp_model = self.model
        query_str, value = child_query
        # 双下划线特殊查询
        if '__' in query_str:
            foreign_field = None
            magic_list = query_str.split('__')
            # 判断是否为外键查询
            if len(magic_list) == 2:
                field, magic = magic_list
                if magic not in correspond_dict and magic not in ['isnull', 'range', 'in']:
                    foreign_field, field, magic = field, magic, ''
            elif len(magic_list) == 3:
                foreign_field, field, magic = magic_list
            else:
                raise TypeError('Unsupported magic query %s' % query_str)
            # 外键查询处理
            if foreign_field:
                if foreign_field.endswith('_id'):
                    foreign_field = foreign_field[:-3]
                foreign_key = self.model.attrs[foreign_field]
                temp_model = foreign_key.to_model
                to_field = foreign_key.to_field
                join_sql = 'inner join %s on %s = %s' % (temp_model.table_info(), temp_model.field_info(to_field),
                                                          self.model.field_info(foreign_field))
                join_set.add(join_sql)
        else:
            field = query_str
            magic = ''
        field = temp_model.field_info(field)
        temp_sql = correspond_dict.get(magic)
        if temp_sql:
            raw_sql = ' ' + field + temp_sql
            params = [value]
            if isinstance(value, (F, CombinedExpression)):
                temp_raw_sql, params = self.f_expr(value)
                raw_sql = raw_sql.replace('%s', temp_raw_sql)
        elif magic == 'isnull':
            if value:
                raw_sql = ' ' + field + ' is null '
            else:
                raw_sql = ' ' + field + ' is not null '
        elif magic == 'range':
            raw_sql = ' ' + field + ' between %s and %s '
            params = value
        elif magic == 'in':
            self_host = self.model.db_info('host')
            if isinstance(value, (ValuesListQuerySet, ValuesQuerySet, QuerySet)):
                subquery = value.query.clone()
                # 禁止跨库in查询
                sub_host = subquery.model.db_info('host')
                if self_host != sub_host:
                    raise TypeError(
                        '%s and %s are not in the same database ' % (temp_model.__name__, value.model.__name__))
                # QuerySet 使用主键
                if type(value) == QuerySet:
                    primary_key = value.model.__primary_key__
                    if not primary_key:
                        raise TypeError('Primary key not defined in class: %s' % value.model.__name__)
                    subquery.select = [primary_key]
                # in查询只允许单字段
                elif len(subquery.select) != 1:
                    raise TypeError('Cannot use a multi-field %s as a filter value.' % value.model.__name__)
                sub_sql, params = subquery.sql_expr()
                raw_sql = ' ' + field + ' in ( ' + sub_sql[:-1] + ' ) '
            else:
                if len(value) == 0:
                    raw_sql = ' False '
                    params = []
                else:
                    raw_sql = ' ' + field + ' in %s '
                    params = [tuple(value)]

        return join_set, raw_sql, params

    def _add_q(self, q_object):
        self.filter_Q.add(q_object, 'AND')

    def clone(self):
        clone = WhereNode(self.model)
        clone.filter_Q.add(self.filter_Q, 'AND')
        return clone

    def __bool__(self):
        return bool(self.filter_Q)


class Query:
    def __init__(self, model):
        self.model = model
        self.fields_list = self.model.field_list

        self.flat = False
        self.group_by = []
        self.annotates = {}
        self.limit_dict = {}
        self.distinct = False
        self.order_fields = []
        self.where = WhereNode(model)
        self.select = self.fields_list

    def __str__(self):
        sql, params = self.sql_expr()
        return sql % params

    # 根据当前筛选条件构建sql、params
    def sql_expr(self, method='select', update_dict=None):

        limit = self.limit_dict.get('limit')
        offset = self.limit_dict.get('offset')
        if update_dict and offset:
            # update 不支持 offset
            raise TypeError('Cannot update a query once a slice has been taken.')

        if self.group_by and method in ['delete', 'update']:
            # group_by 不支持 update、delete
            raise TypeError('Cannot execute with group by query.')

        params = []
        where_expr = ''
        field_info = self.model.field_info

        if self.where:
            where_sql, where_params = self.where.as_sql()
            where_expr += where_sql
            params.extend(where_params)

        if self.order_fields:
            where_expr += ' order by '
            order_list = []
            for field in self.order_fields:
                if field[0] == '-':
                    field_name = field[1:]
                    order_list.append(field_info(field_name) + ' desc ')
                else:
                    order_list.append(field_info(field))
            where_expr += ' , '.join(order_list)

        if self.group_by:
            where_expr += ' group by ' + ', '.join(field_info(x) for x in self.group_by)

        # limit offset
        if limit is None and offset is not None:
            limit = 18446744073709551615
        if limit is not None:
            where_expr += ' limit %s '
            params.append(limit)
        if offset is not None:
            where_expr += ' offset %s '
            params.append(offset)

        table_info = self.model.table_info()
        # 构建不同操作的sql语句
        if method == 'update' and update_dict:
            _keys = []
            _params = []
            for key, val in update_dict.items():
                if key not in self.fields_list:
                    continue
                temp_key = ' = %s'
                temp_params = [val]
                if isinstance(val, (F, CombinedExpression)):
                    f_sql, f_params = self.where.f_expr(val)
                    temp_key = ' = ' + f_sql
                    temp_params = f_params
                _keys.append(field_info(key) + temp_key)
                _params.extend(temp_params)
            params = _params + params
            sql = 'update %s set %s %s;' % (table_info, ', '.join(_keys), where_expr)
        elif method == 'delete':
            sql = 'delete from %s %s;' % (table_info, where_expr)
        else:
            field_list = [field_info(x) for x in self.select]
            # 聚合查询
            for k, v in self.annotates.items():
                field_list.append('%s as %s' % (v.sql_expr(field_info), k))
            select_field = ', '.join(field_list)
            subquery = 'select %s %s from %s %s' % (
                'distinct' if self.distinct else '', select_field, table_info, where_expr)
            if method == 'count' and (self.distinct or limit):
                sql = 'select count(*) from (%s) subquery;' % subquery
            elif method == 'count':
                sql = 'select count(*) from %s %s;' % (table_info, where_expr)
            else:
                sql = subquery + ';'
        return sql, tuple(params)

    # clone
    def clone(self):
        obj = Query(self.model)
        obj.flat = self.flat
        obj.select = self.select[:]
        obj.distinct = self.distinct
        obj.where = self.where.clone()
        obj.group_by = self.group_by[:]
        obj.annotates.update(self.annotates)
        obj.limit_dict.update(self.limit_dict)
        obj.order_fields = self.order_fields[:]
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
        sql, params = self.query.sql_expr(method='count')
        (select_count,) = Database.execute(self.model.__db_label__, sql, params).fetchone()
        return select_count

    # update
    def update(self, **kwargs):
        if kwargs:
            _, kwargs = ModelCheck(self.model).field_wash(fields_list=[], fields_dict=kwargs)
            sql, params = self.query.sql_expr(method='update', update_dict=kwargs)
            Database.execute(self.model.__db_label__, sql, params)

    # order_by函数，返回一个新的QuerySet对象
    def order_by(self, *args):
        obj = self._clone()
        args, _ = ModelCheck(self.model).field_wash(args)
        obj.query.order_fields = args
        return obj

    # create
    def create(self, **kwargs):
        obj = self.model(**kwargs)
        obj.save()
        return obj

    # exists
    def exists(self):
        return bool(self.first())

    # delete
    def delete(self):
        sql, params = self.query.sql_expr(method='delete')
        Database.execute(self.model.__db_label__, sql, params)

    # values
    def values(self, *args):
        fields_list, _ = ModelCheck(self.model).field_wash(args)
        return self._clone(ValuesQuerySet, fields_list)

    # values_list
    def values_list(self, *args, **kwargs):
        # 字段检查
        fields_list, _ = ModelCheck(self.model).field_wash(args)
        flat = kwargs.pop('flat', False)
        # flat 只能返回一个字段列表
        if flat and len(args) > 1:
            raise TypeError('flat is not valid when values_list is called with more than one field.')

        return self._clone(ValuesListQuerySet, fields_list, flat)

    # group_by
    def group_by(self, *args):
        fields_list, _ = ModelCheck(self.model).field_wash(args)
        clone = self._clone()
        clone.query.group_by += fields_list
        return clone

    # annotate
    def annotate(self, **kwargs):
        ModelCheck(self.model).field_wash([x.field for x in kwargs.values()])
        self.query.annotates.update(kwargs)
        return self._clone(ValuesQuerySet)

    # distinct
    def distinct(self, *field_names):
        if self.__class__ == QuerySet and field_names:
            clone = self._clone(ValuesQuerySet)
        else:
            clone = self._clone()
        if field_names:
            field_names, _ = ModelCheck(self.model).field_wash(field_names)
            clone.query.select = list(field_names)
        clone.query.distinct = True
        return clone

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
            query.select = list(select[:])
        if flat:
            query.flat = flat
        obj = klass(model=self.model, query=query)
        return obj

    # 根据传入的筛选条件，返回新的QuerySet对象
    def _filter_or_exclude(self, negate, *args, **kwargs):
        clone = self._clone()
        temp_q = Q(*args, **kwargs)
        temp_q.negated = negate
        new_q = self._make_q(temp_q)
        clone.query.where._add_q(new_q)
        return clone

    def _make_q(self, q_object):
        connector = q_object.connector
        new_q = Q()
        new_q.connector = connector
        new_q.negated = q_object.negated
        for child in q_object.children:
            if isinstance(child, Q):
                temp_q = self._make_q(child)
                new_q.add(temp_q, connector)
            else:
                new_q.add(Q(child), connector)
        return new_q

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

    def __bool__(self):
        return self.exists()

    def __repr__(self):
        return '<QuerySet Obj>'


class ValuesQuerySet(QuerySet):
    def __init__(self, *args, **kwargs):
        super(ValuesQuerySet, self).__init__(*args, **kwargs)
        self.select_field = self.query.select + list(self.query.annotates.keys())

    def __iter__(self):
        self.select()
        for value in self.select_result:
            inst = {field: value[index] for index, field in enumerate(self.select_field)}
            yield inst

    def get_index(self, index):
        index_value = self.base_index(index)
        return {field: index_value[f_index] for f_index, field in enumerate(self.select_field)}

    def __repr__(self):
        return '<ValuesQuerySet Obj>'


class ValuesListQuerySet(QuerySet):
    def __init__(self, *args, **kwargs):
        super(ValuesListQuerySet, self).__init__(*args, **kwargs)
        self.flat = self.query.flat
        self.select_field = self.query.select[:]
        if self.flat and len(self.select_field) != 1:
            raise TypeError('flat is not valid when values_list is called with more than one field.')
        self.select_field += list(self.query.annotates.keys())

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


class ModelCheck:
    def __init__(self, model):
        self.model = model

    def field_wash(self, fields_list, fields_dict=None):
        temp_list = []
        for field in fields_list:
            minus = ''
            if field[0] == '-':
                minus, field = '-', field[1:]
            field_obj = self.model.attrs.get(field, None)
            if not field_obj:
                raise TypeError('Cannot resolve keyword %s into field.' % field)
            temp_list.append(minus + field_obj.name)

        temp_dict = {}
        if fields_dict:
            for key, value in fields_dict.items():
                field_obj = self.model.attrs.get(key, None)
                if not field_obj:
                    raise TypeError('Cannot resolve keyword %s into field.' % key)
                temp_dict[field_obj.name] = value
        return temp_list, temp_dict


class Manager:
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

    def values_list(self, *args, **kwargs):
        return self.get_queryset().values_list(*args, **kwargs)

    def bulk_create(self, objs, ignore_conflicts=False):
        if_foreign = lambda x: isinstance(self.model.attrs[x], ForeignKey)
        fields = [x + '_id' if if_foreign(x) else x for x in self.model.field_list]
        items = [[getattr(obj, field, None) for field in fields] for obj in objs]
        obj_value = ', '.join(['%s'] * len(fields))
        insert = 'insert %s into %s(%s) values(%s);' % ('ignore' if ignore_conflicts else '', self.model.table_info(),
                                                        ', '.join(self.model.field_info(x) for x in fields), obj_value)
        Database.executemany(self.model.__db_label__, insert, items)


class MetaModel(type):
    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        if name == 'Model':
            return

        meta_attrs = attrs.get('Meta')
        if meta_attrs and getattr(meta_attrs, 'abstract', False):
            return
        cls.__db_table__ = getattr(meta_attrs, 'db_table', name)
        cls.__db_label__ = getattr(meta_attrs, 'db_label', 'default')

        field_list = []
        primary_key = None
        attr_dict = {}
        mro_list = cls.mro()[:-2]
        for base in mro_list[::-1]:
            attr_dict.update(base.__dict__)
        for key, val in attr_dict.items():
            if isinstance(val, ForeignKey):
                val.name = key
                attrs[key] = val
                attrs[key + '_id'] = Field(name=key)
                setattr(cls, key + '_id', Field(name=key))
                _get_foreign = partial(cls._get_foreign, field=key)
                _set_foreign = partial(cls._set_foreign, field=key)
                setattr(cls, key, property(_get_foreign, _set_foreign))
                field_list.append(key)
            elif isinstance(val, Field):
                val.name = key
                if val.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    primary_key = key
                    attrs['pk'] = val
                attrs[key] = val
                setattr(cls, key, val)
                field_list.append(key)
        cls.field_list = field_list
        cls.attrs = attrs
        cls.objects = Manager(cls)
        cls.__primary_key__ = primary_key


class Model(metaclass=MetaModel):

    def __init__(self, **kw):
        [setattr(self, k, None) for k in self.field_list]
        for k, v in kw.items():
            if k in self.field_list:
                setattr(self, k, v)
            elif k == 'pk' and self.__primary_key__:
                self.pk = v
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
        setattr(self, primary_key, value)

    pk = property(_get_pk_val, _set_pk_val)

    def _get_foreign(self, field):
        self_value = getattr(self, field + '_id', None)
        if self_value:
            foreign_key = self.attrs[field]
            to_model = foreign_key.to_model
            to_field = foreign_key.to_field
            return to_model.objects.filter(**{to_field: self_value}).first()

    def _set_foreign(self, val, field):
        if isinstance(val, Model):
            to_field = self.attrs[field].to_field
            val = getattr(val, to_field, None)
        setattr(self, field + '_id', val)

    def __repr__(self):
        return '<%s obj>' % self.__class__.__name__

    def __bool__(self):
        return bool(self.__dict__)

    def __eq__(self, obj):
        return self.__class__ == obj.__class__ and self.__dict__ == obj.__dict__

    def __hash__(self):
        return str(self.__dict__.__hash__) + str(self.__class__)

    def _insert(self):
        insert = 'insert into %s(%s) values (%s);' % (
            self.table_info(), ', '.join(self.field_info(x) for x in self.__dict__.keys()),
            ', '.join(['%s'] * len(self.__dict__)))
        cursor = Database.execute(self.__db_label__, insert, tuple(self.__dict__.values()))
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

    @classmethod
    def field_info(cls, field):
        model_field = cls.attrs.get(field, None)
        if not model_field:
            raise TypeError('Cannot resolve keyword %s into field.' % field)
        db_column = model_field.db_column or model_field.name
        return '`%s`.`%s`' % (cls.__db_table__, db_column)

    @classmethod
    def table_info(cls):
        self_info = cls.db_info('database')
        return '`%s`.`%s`' % (self_info, cls.__db_table__) if self_info else '`%s`' % cls.__db_table__

    @classmethod
    def db_info(cls, key):
        self_config = Database.db_config.get(cls.__db_label__, {})
        return self_config.get(key)


# 数据库调用
class Database:
    conn = {}
    db_config = {}

    @classmethod
    def connect(cls, **databases):
        for db_label, db_config in databases.items():
            cls.conn[db_label] = pymysql.connect(host=db_config.get('host', 'localhost'),
                                                 port=int(db_config.get('port', 3306)),
                                                 user=db_config.get('user', 'root'),
                                                 passwd=db_config.get('password', ''),
                                                 db=db_config.get('database', 'test'),
                                                 charset=db_config.get('charset', 'utf8'),
                                                 autocommit=True)
        cls.db_config.update(databases)

    @classmethod
    def get_conn(cls, db_label):
        if not cls.conn[db_label] or not cls.conn[db_label].open:
            cls.connect(**cls.db_config)
        try:
            cls.conn[db_label].ping()
        except pymysql.OperationalError:
            cls.connect(**cls.db_config)
        return cls.conn[db_label]

    @classmethod
    def execute(cls, db_label, *args):
        db_conn = cls.get_conn(db_label)
        cursor = db_conn.cursor()
        cursor.execute(*args)
        return cursor

    @classmethod
    def executemany(cls, db_label, *args):
        db_conn = cls.get_conn(db_label)
        cursor = db_conn.cursor()
        cursor.executemany(*args)
        return cursor

    def __del__(self):
        for _, conn in self.conn:
            if conn and conn.open:
                conn.close()


def execute_raw_sql(db_label, sql, params=None):
    return Database.execute(db_label, sql, params)
