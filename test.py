from data_handler import Database, Model, Field, execute_raw_sql, Q

# connect database
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


class TestModelBasic(Model):
    id = Field(primary_key=True)
    a = Field()

    class Meta:
        abstract = True


class TestModel(TestModelBasic):
    b = Field()

    class Meta:
        db_table = 'test'
        db_label = 'default'


# create instance
test = TestModel()
test.a = 'john'
test.b = 1
test.save()
print(test.id)

test = TestModel(a='marry', b=2)
test.save()
print(test.pk)

test = TestModel.objects.create(a='marry', b=3)

filter_result = TestModel.objects.filter(Q(a='john') | Q(a='marry'), pk__gt=1).exclude(b__in=[3, 4])
print(filter_result.query)
print(filter_result.count())

# select
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
first.a = 'update'
first.save()
filter_result.update(b=1)

# execute raw sql
results = execute_raw_sql('default', 'select b, count(*) from test where b = %s group by b;', (1,))
for val, cnt in results:
    print(val, cnt)
