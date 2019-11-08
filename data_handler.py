# coding: utf-8
import MySQLdb


class Field():

    def __nonzero__(self):
        return False

    def __bool__(self):
        return False


class Expr():
    def __init__(self, model, kwargs):
        self.model = model
        # How to deal with a non-dict parameter?
        self.params = kwargs.values()
        equations = [key + ' = %s' for key in kwargs.keys()]
        self.where_expr = 'where ' + ' and '.join(equations) if len(equations) > 0 else ''

    def update(self, **kwargs):
        _keys = []
        _params = []
        for key, val in kwargs.items():
            if val is None or key not in self.model.fields:
                continue
            _keys.append(key)
            _params.append(val)
        _params.extend(self.params)
        sql = 'update %s set %s %s;' % (
            self.model.db_table, ', '.join([key + ' = %s' for key in _keys]), self.where_expr)
        return Database.execute(self.model.db_label, sql, _params)

    def limit(self, rows, offset=None):
        self.where_expr += ' limit %s%s' % (
            '%s, ' % offset if offset is not None else '', rows)
        return self

    def order_by(self, field, order='asc'):
        self.where_expr += ' order by %s %s ' % (field, order)
        return self

    def select(self):
        fields_list = list(self.model.fields.keys())
        sql = 'select %s from %s %s;' % (', '.join(fields_list), self.model.db_table, self.where_expr)
        for row in Database.execute(self.model.db_label, sql, self.params).fetchall():
            inst = self.model()
            for idx, f in enumerate(row):
                setattr(inst, fields_list[idx], f)
            yield inst

    def first(self):
        fields_list = list(self.model.fields.keys())
        sql = 'select %s from %s %s;' % (', '.join(fields_list), self.model.db_table, self.where_expr)
        row = Database.execute(self.model.db_label, sql, self.params).fetchone()
        if row:
            inst = self.model()
            for idx, f in enumerate(row):
                setattr(inst, fields_list[idx], f)
            return inst
        return None

    def count(self):
        sql = 'select count(*) from %s %s;' % (self.model.db_table, self.where_expr)
        (row_cnt,) = Database.execute(self.model.db_label, sql, self.params).fetchone()
        return row_cnt

    def raw_sql(self, sql):
        self.where_expr += sql
        return self


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

    @classmethod
    def where(cls, **kwargs):
        return Expr(cls, kwargs)


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
