Django ORM Imitate
========

基于[QuickORM](https://github.com/2shou/QuickORM)开发 模仿实现Django ORM的基本操作  

----------------

```python
from data_handler import Database

db_config = {
    'default': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'test'
    }
}
Database.connect(**db_config)
```

Define a model
--------------

```python
from data_handler import Model, Field

class TestModel(Model):
  db_table = 'test'
  db_label = 'default'
  a = Field()
  b = Field()
```

Insert
------

```python
test = TestModel()
test.a = 1
test.b = 'john'
test.save()

test = TestModel()
test.a = 'marry'
test.b = 2
test.save()

test = TestModel()
test.a = 'john'
test.b = 3
test.save()
```

Query
-----

```python
from data_handler import Q
filter_result = TestModel.objects.filter(Q(a='john') | Q(a='marry'), b__gte=1).exclude(b__in=[3, 4])
print(filter_result.query)

# select
for r in filter_result[:5]:
    print(type(r))
    print(r.a)
    print(r.b)
```

```python
# first
r = filter_result.first()
if r:
    print(type(r))
    print(r.a)
    print(r.b)

first = filter_result[0]
print(first == r)
```

Count
-----

```python
print(filter_result.count())
```

Update
------

```python
filter_result.update(b=1)
```

Execute raw SQL
---------------

```python
from data_handler import execute_raw_sql

results = execute_raw_sql('default', 'select b, count(*) from test where b = %s group by b;', (1,))
for val, cnt in results:
  print(val, cnt)
```
