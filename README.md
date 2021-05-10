Django ORM Imitate(async)
========

模仿实现Django ORM的基本操作(异步)

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


async def db_connect():
    await Database.connect(**db_config)
```

Define a model
--------------

```python
from data_handler import Model, Field


class TestModel(Model):
    __db_table__ = 'test'
    __db_label__ = 'default'
    id = Field(primary_key=True)  # primary_key is optional
    a = Field()
    b = Field()
```

Insert
------

```python
async def create():
    test = TestModel()
    test.a = 'john'
    test.b = 1
    await test.save()
    print(test.id)

    test = TestModel(a='marry', b=2)
    await test.save()
    print(test.pk)

    test = await TestModel.objects.create(a='marry', b=3)
    print(test.pk)
```

Query
-----

```python
async def select():
    filter_result = TestModel.objects.filter(Q(a='john') | Q(a='marry'), pk__gt=1).exclude(b__in=[3, 4])
    print(filter_result.query)
    print(await filter_result.count())

    # select
    for r in await filter_result[:5]:
        print(type(r))
        print(r.a)
        print(r.b)

    # first
    r = await filter_result.first()
    if r:
        print(type(r))
        print(r.a)
        print(r.b)

    first = await filter_result[0]
    print(first == r)
```

Update
------

```python
async def update():
    first.a = 'update'
    await first.save()
    await filter_result.update(b=1)
```

Execute raw SQL
---------------

```python
from data_handler import execute_raw_sql


async def execute():
    results = await execute_raw_sql('default', 'select b, count(*) from test where b = %s group by b;', (1,))
    async for val, cnt in results:
        print(val, cnt)
```
