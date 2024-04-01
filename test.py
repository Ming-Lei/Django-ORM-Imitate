from data_handler import Database, Model, Field, execute_raw_sql, Q, Sum, F, Max, ForeignKey

# connect database
db_config = {
    'default': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'test',
        'pool_min': 1,
        'pool_max': 5
    }
}
Database.connect(**db_config)


class TestModelBasic(Model):
    id = Field(primary_key=True)
    a = Field()

    class Meta:
        abstract = True


class TestModel(TestModelBasic):
    b = Field(db_column='bb')

    class Meta:
        db_table = 'test'
        db_label = 'default'


class TestForeignModel(TestModelBasic):
    c = ForeignKey(to=TestModel, to_field="pk")

    class Meta:
        db_table = 'test_foreign'
        db_label = 'default'


# create instance
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
for (temp_a, temp_b) in temp_list:
    obj = TestModel(a=temp_a, b=temp_b)
    objs_list.append(obj)

TestModel.objects.bulk_create(objs_list)

# select
filter_result = TestModel.objects.filter(Q(a='Rick') | Q(a='Morty'), pk__gt=1).exclude(b__in=[3, 4])
print(filter_result.query)
print(filter_result.count())

for r in filter_result[:5]:
    print(type(r))
    print(r.a)
    print(r.b)

# first
r = filter_result.first()
if r:
    print(type(r))
    print(r.a)
    print(r.b)

first = filter_result[0]
print(first == r)

# update
first.a = 'Rick Sanchez'
first.save()
filter_result.update(b=F('b') + 11)

# group by
group_value = filter_result.group_by('a').annotate(sum_b=Sum('b'), max_id=Max('id')).values('a')
print(group_value.query)
for obj in group_value:
    print(obj['a'], obj['sum_b'], obj['max_id'])

# execute raw sql
results = execute_raw_sql('default', 'select bb, count(*) from test where bb = %s group by bb;', (1,))
for val, cnt in results:
    print(val, cnt)

# foreign key

# create
obj = TestForeignModel(a='Rick', c=first)
obj.save()
print(obj.pk)

obj = TestForeignModel(a='Rick', c=2)
obj.save()
print(obj.pk)

# bulk create
objs_list = []
for temp_a in ['Morty', 'Jerry', 'Beth', 'Summer']:
    for x in range(1, 3):
        obj = TestForeignModel(a=temp_a, c=x)
        objs_list.append(obj)

TestForeignModel.objects.bulk_create(objs_list)

# filter
foreign_values = TestForeignModel.objects.filter(c__a='Rick', c__b__lte=5)[:2]
print(foreign_values.query)
for obj in foreign_values:
    obj_c = obj.c
    print(type(obj_c))
    print(obj_c.pk, obj_c.a, obj_c.b)
