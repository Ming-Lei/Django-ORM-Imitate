from data_handler import Database, Model, Field, execute_raw_sql

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


# define model
class TestModel(Model):
    db_table = 'test'  # point table name
    db_label = 'default'
    a = Field()
    b = Field()

# create instance
test = TestModel()
test.a = 'john'
test.b = 1
test.save()

test = TestModel()
test.a = 'marry'
test.b = 2
test.save()

test = TestModel()
test.a = 'john'
test.b = 3
test.save()

filter_result = TestModel.objects.filter(a='john', b__gte=1).exclude(b__in=[3, 4])
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
filter_result.update(b=1)

# execute raw sql
results = execute_raw_sql('default', 'select b, count(*) from test where b = %s group by b;', (1,))
for val, cnt in results:
    print(val, cnt)
