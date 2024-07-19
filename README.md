Django ORM Imitate
========

基于 [QuickORM](https://github.com/2shou/QuickORM) 开发 模仿实现Django ORM的基本操作

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
    id = Field(primary_key=True)  # primary_key is optional
    a = Field()
    b = Field(db_column='bb')

    class Meta:
        db_table = 'test'  # If not filled, the db_table is class name 
        db_label = 'default'  # If not filled, the db_label is default  


class TestForeignModel(Model):
    id = Field(primary_key=True)
    a = Field()
    c = Field()

    class Meta:
        db_table = 'test_foreign'
        db_label = 'default'

# use abstract class
# class TestModelBasic(Model):
#     id = Field(primary_key=True)
#     a = Field()
# 
#     class Meta:
#         abstract = True
# 
# class TestForeignModel(TestModelBasic):
#     c = Field()
# 
#     class Meta:
#         db_table = 'test_foreign'
#         db_label = 'default'

```

Insert
------

```python
test = TestModel()
test.a = 'Rick'
test.b = 1
test.save()
print(test.id)

test = TestModel(a='Morty', b=2)
test.save()
print(test.pk)

test = TestModel.objects.create(a='Jerry', b=3)

# bulk create
temp_list = [
    ['Beth', 4],
    ['Summer', 5],
    ['Rick', 6],
    ['Morty', 7],
    ['Jerry', 8],
    ['Beth', 9],
    ['Summer', 10],
]

objs_list = []
for (temp_a, temp_c) in temp_list:
    obj = TestForeignModel(a=temp_a, c=temp_c)
    objs_list.append(obj)

TestForeignModel.objects.bulk_create(objs_list)
```

Query
-----

```python
from data_handler import Q

filter_result = TestModel.objects.filter(Q(a='Rick') | Q(a='Morty'), pk__gte=1).exclude(b__in=[3, 4])
print(filter_result.query)

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
from data_handler import F

first.a = 'Rick Sanchez'
first.save()
filter_result.update(b=F('b') + 11)
```

Group by
------

```python
from data_handler import Sum, Max, Count

group_value = filter_result.group_by('a').annotate(count_a=Count('a'), sum_b=Sum('b'), max_id=Max('id'))
print(group_value.query)
for obj in group_value:
    print(obj['a'], obj['count_a'], obj['sum_b'], obj['max_id'])
```

Join 
------

```python

join_filter = TestModel.objects.join(on=TestForeignModel, table_as='tfm', a='a').filter(b__gte=2, tfm__c__lte=10, 
                                                                                        pk__lte=F('tfm__id'))[:10]
print(join_filter.query)
for obj in join_filter:
    tfm = obj.tfm
    print(obj.id, obj.a, obj.b, tfm.id, tfm.a, tfm.c)
```

Execute raw SQL
---------------

```python
from data_handler import execute_raw_sql

results = execute_raw_sql('default', 'select bb, count(*) from test where bb = %s group by bb;', (1,))
for val, cnt in results:
    print(val, cnt)

```
