# coding: utf-8

import MySQLdb


class Field(object):
    pass


class Expr(object):
    def __init__(self, model, kwargs):
        self.model = model
        # How to deal with a non-dict parameter?
        self.params = kwargs.values()
        equations = [key + ' = %s' for key in kwargs.keys()]
        self.where_expr = 'where ' + ' and '.join(equations) if len(equations) > 0 else ''

    def update(self, **kwargs):
        _keys = []
        _params = []
        for key, val in kwargs.iteritems():
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
        sql = 'select %s from %s %s;' % (', '.join(self.model.fields.keys()), self.model.db_table, self.where_expr)
        for row in Database.execute(self.model.db_label, sql, self.params).fetchall():
            inst = self.model()
            for idx, f in enumerate(row):
                setattr(inst, self.model.fields.keys()[idx], f)
            yield inst

    def first(self):
        sql = 'select %s from %s %s;' % (', '.join(self.model.fields.keys()), self.model.db_table, self.where_expr)
        row = Database.execute(self.model.db_label, sql, self.params).fetchone()
        if row:
            inst = self.model()
            for idx, f in enumerate(row):
                setattr(inst, self.model.fields.keys()[idx], f)
            return inst
        return None

    def count(self):
        sql = 'select count(*) from %s %s;' % (self.model.db_table, self.where_expr)
        (row_cnt,) = Database.execute(self.model.db_label, sql, self.params).fetchone()
        return row_cnt


class MetaModel(type):
    db_table = None
    fields = {}

    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        fields = {}
        for key, val in cls.__dict__.iteritems():
            if isinstance(val, Field):
                fields[key] = val
        cls.fields = fields
        cls.attrs = attrs


class Model(object):
    __metaclass__ = MetaModel

    def save(self):
        insert = 'insert ignore into %s(%s) values (%s);' % (
            self.db_table, ', '.join(self.__dict__.keys()), ', '.join(['%s'] * len(self.__dict__)))
        return Database.execute(self.db_label, insert, self.__dict__.values())

    @classmethod
    def where(cls, **kwargs):
        return Expr(cls, kwargs)


class Database(object):
    autocommit = True
    conn = {}
    db_config = {}

    @classmethod
    def connect(cls, **databases):
        for db_label, db_config in databases.items():
            cls.conn[db_label] = MySQLdb.connect(host=db_config.get('host', 'localhost'), port=int(db_config.get('port', 3306)),
                                                 user=db_config.get('user', 'root'), passwd=db_config.get('password', ''),
                                                 db=db_config.get('database', 'test'), charset=db_config.get('charset', 'utf8'))
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
