# coding: utf-8

# https://pypi.org/project/pymysql-pool/
import pymysqlpool


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
        self.join_as = {}
        self.model = model
        self.filter_Q = Q()

    def as_sql(self):
        params = []
        where_expr = ''

        if self.filter_Q:
            temp_sql, temp_params = self.sql_expr(self.filter_Q)
            where_expr += temp_sql
            params.extend(temp_params)

        return where_expr, params

    # 构建sql查询语句
    def _sql_expr(self, q_query):
        sql_list = []
        params = []
        for child in q_query.children:
            if not isinstance(child, Q):
                temp_sql, temp_params = self.magic_query(child)
                sql_list.append(temp_sql)
                params.extend(temp_params)
            else:
                temp_sql, temp_params = self._sql_expr(child)
                if temp_sql:
                    if child.negated:
                        temp_sql = [' not ' + x for x in temp_sql]
                    raw_sql = child.connector.join(temp_sql)
                    if child.connector != q_query.connector:
                        raw_sql = ' ( ' + raw_sql + ' ) '
                    sql_list.append(raw_sql)
                    params.extend(temp_params)
        return sql_list, params

    # 取得对应sql及参数
    def sql_expr(self, q_query):
        sql_list, params = self._sql_expr(q_query)
        return q_query.connector.join(sql_list), params

    def f_expr(self, value):
        if isinstance(value, F):
            field_name = ModelCheck(self.model, self.join_as).field_info(value.name)
            return field_name, []
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
        temp_model = self.model
        query_str, value = child_query
        if '__' in query_str:
            field = magic = ''
            temp_field, *magic_list = query_str.split('__')
            if len(magic_list) == 1 and (temp_field in temp_model.field_list or
                                         (temp_field == 'pk' and temp_model.__primary_key__)):
                field, magic = temp_field, magic_list[0]
            elif temp_field in self.join_as:
                temp_model = self.join_as[temp_field]['join_model']
                if len(magic_list) == 1:
                    field, magic = magic_list[0], ''
                elif len(magic_list) == 2:
                    field, magic = magic_list[0], magic_list[1]
            if not field:
                raise TypeError('Cannot resolve keyword %s into field.' % temp_field)
        else:
            field, magic = query_str, ''
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

        return raw_sql, params

    def _add_q(self, q_object):
        self.filter_Q.add(q_object, 'AND')

    def clone(self):
        clone = WhereNode(self.model)
        clone.join_as.update(self.join_as)
        clone.filter_Q.add(self.filter_Q, 'AND')
        return clone

    def __bool__(self):
        return bool(self.filter_Q)


class Query:
    def __init__(self, model):
        self.model = model
        self.fields_list = self.model.field_list

        self.select = []
        self.flat = False
        self.join_as = {}
        self.group_by = []
        self.annotates = {}
        self.limit_dict = {}
        self.distinct = False
        self.order_fields = []
        self.where = WhereNode(model)

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

        check_obj = ModelCheck(self.model, self.join_as)
        field_info = check_obj.field_info
        table_info = self.model.table_info()

        # join
        join_sql = ''
        join_field = []
        for table_as, join_info in self.join_as.items():
            join_model, join_on = join_info['join_model'], join_info['join_on']
            join_field.extend(field_info(table_as + '__' + x) for x in join_model.field_list)
            temp_join = ' join ' + join_model.table_info() + ' on '
            on_list = []
            for k, v in join_on:
                on_list.append(field_info(k) + ' = ' + field_info(v))
            temp_join += ' and '.join(on_list)
            join_sql += temp_join

        params = []
        where_expr = join_sql
        if self.where:
            where_sql, where_params = self.where.as_sql()
            where_expr += ' where ' + where_sql
            params.extend(where_params)

        if self.group_by:
            where_expr += ' group by ' + ', '.join(field_info(x) for x in self.group_by)

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

        # limit offset
        if limit is None and offset is not None:
            limit = 18446744073709551615
        if limit is not None:
            where_expr += ' limit %s '
            params.append(limit)
        if offset is not None:
            where_expr += ' offset %s '
            params.append(offset)

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

            if self.select:
                field_list = [field_info(x) for x in self.select]
            else:
                field_list = [field_info(x) for x in self.fields_list]
                field_list.extend(join_field)

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
        obj.join_as.update(self.join_as)
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
        args, _ = ModelCheck(self.model, self.query.join_as).field_wash(args)
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
        fields_list, _ = ModelCheck(self.model, self.query.join_as).field_wash(args)
        return self._clone(ValuesQuerySet, fields_list or None)

    # values_list
    def values_list(self, *args, **kwargs):
        # 字段检查
        fields_list, _ = ModelCheck(self.model, self.query.join_as).field_wash(args)
        flat = kwargs.pop('flat', False)
        # flat 只能返回一个字段列表
        if flat and len(args) > 1:
            raise TypeError('flat is not valid when values_list is called with more than one field.')

        return self._clone(ValuesListQuerySet, fields_list or None, flat)

    # group_by
    def group_by(self, *args):
        fields_list, _ = ModelCheck(self.model, self.query.join_as).field_wash(args)
        clone = self._clone()
        clone.query.group_by += fields_list
        return clone

    # annotate
    def annotate(self, **kwargs):
        ModelCheck(self.model, self.query.join_as).field_wash([x.field for x in kwargs.values()])
        self.query.annotates.update(kwargs)
        return self._clone(ValuesQuerySet, self.query.group_by)

    # distinct
    def distinct(self, *field_names):
        if self.__class__ == QuerySet and field_names:
            clone = self._clone(ValuesQuerySet)
        else:
            clone = self._clone()
        if field_names:
            field_names, _ = ModelCheck(self.model, self.query.join_as).field_wash(field_names)
            clone.query.select = list(field_names)
        clone.query.distinct = True
        return clone

    # join on
    def join(self, on, table_as, **kwargs):
        clone = self._clone()
        if table_as in self.model.field_list:
            raise TypeError("'%s' is an field for model '%s'" % (table_as, self.model.__name__))
        elif table_as in clone.query.join_as:
            raise TypeError("alias '%s' is already exists" % table_as)

        clone.query.join_as[table_as] = {'join_model': on, 'join_on': []}
        key_list, _ = ModelCheck(self.model, clone.query.join_as).field_wash(kwargs.keys())
        values_list, _ = ModelCheck(on, clone.query.join_as).field_wash(kwargs.values())
        clone.query.join_as[table_as] = clone.query.where.join_as[table_as] = \
            {'join_model': on, 'join_on': tuple(zip(key_list, values_list))}
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
        return self.data_to_obj(index_value)

    def _clone(self, klass=None, select=None, flat=False):
        if klass is None:
            klass = self.__class__
        query = self.query.clone()
        if select is not None:
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

    def data_to_obj(self, value):
        start_index = len(self.fields_list)
        inst = self.model(**dict(zip(self.fields_list, value[:start_index])))
        for table_as, join_info in self.query.join_as.items():
            join_model = join_info['join_model']
            temp_len = len(join_model.field_list)
            temp_obj = join_model(**dict(zip(join_model.field_list, value[start_index:start_index + temp_len])))
            setattr(inst, table_as, temp_obj)
            start_index += temp_len
        return inst

    # 返回自定义迭代器
    def __iter__(self):
        self.select()
        for value in self.select_result:
            inst = self.data_to_obj(value)
            yield inst

    def __bool__(self):
        return self.exists()

    def __repr__(self):
        return '<QuerySet Obj>'


class ValuesQuerySet(QuerySet):
    def __init__(self, *args, **kwargs):
        super(ValuesQuerySet, self).__init__(*args, **kwargs)
        self.select_field = self.query.select + list(self.query.annotates.keys())
        if not self.select_field:
            self.select_field.extend(x for x in self.model.field_list)
            for table_as, join_info in self.query.join_as.items():
                self.select_field.extend(table_as + '__' + x for x in join_info['join_model'].field_list)

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
    def __init__(self, model, join_as=None):
        self.model = model
        self.join_as = join_as or {}

    def field_wash(self, fields_list, fields_dict=None):
        temp_list = []
        for field in fields_list:
            minus = ''
            if field[0] == '-':
                minus, field = '-', field[1:]
            if '__' in field:
                temp_as, field = field.split('__')
                join_as, temp_model = temp_as + '__', self.join_as[temp_as]['join_model']
            else:
                join_as, temp_model = '', self.model
            field_obj = temp_model.attrs.get(field, None)
            if not field_obj:
                raise TypeError('Cannot resolve keyword %s into field.' % field)
            temp_list.append(minus + join_as + field_obj.name)

        temp_dict = {}
        if fields_dict:
            for key, value in fields_dict.items():
                if '__' in key:
                    temp_as, key = key.split('__')
                    join_as, temp_model = temp_as + '__', self.join_as[temp_as]['join_model']
                else:
                    join_as, temp_model = '', self.model
                field_obj = temp_model.attrs.get(key, None)
                if not field_obj:
                    raise TypeError('Cannot resolve keyword %s into field.' % key)
                temp_dict[join_as + field_obj.name] = value
        return temp_list, temp_dict

    def field_info(self, field_name):
        if '__' not in field_name:
            return self.model.field_info(field_name)
        else:
            temp_as, field = field_name.split('__')
            if temp_as in self.join_as:
                return self.join_as[temp_as]['join_model'].field_info(field)
            raise TypeError('Cannot resolve keyword %s into field.' % field_name)


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

    def join(self, *args, **kwargs):
        return self.get_queryset().join(*args, **kwargs)

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
        fields = self.model.field_list
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
            if isinstance(val, Field):
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
            cls.conn[db_label] = pymysqlpool.ConnectionPool(size=db_config.get('pool_min', 1),
                                                            maxsize=db_config.get('pool_max', 1),
                                                            pre_create_num=db_config.get('pool_min', 1),
                                                            name=db_config.get('database', 'test'),
                                                            host=db_config.get('host', 'localhost'),
                                                            port=int(db_config.get('port', 3306)),
                                                            user=db_config.get('user', 'root'),
                                                            password=db_config.get('password', ''),
                                                            database=db_config.get('database', 'test'),
                                                            charset=db_config.get('charset', 'utf8'),
                                                            autocommit=True)
        cls.db_config.update(**databases)

    @classmethod
    def execute(cls, db_label, *args):
        db_conn = cls.conn[db_label].get_connection()
        with db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(*args)
                return cursor

    @classmethod
    def executemany(cls, db_label, *args):
        db_conn = cls.conn[db_label].get_connection()
        with db_conn:
            with db_conn.cursor() as cursor:
                cursor.executemany(*args)
                return cursor


def execute_raw_sql(db_label, sql, params=None):
    return Database.execute(db_label, sql, params)
