from data_handler import Database, Model, Field, execute_raw_sql, Q

db_config = {
    'default': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'password',
        'database': 'test'
    }
}


# define model
class TestModel(Model):
    __db_table__ = 'test'
    __db_label__ = 'default'
    id = Field(primary_key=True)  # primary_key is optional
    a = Field()
    b = Field()


# create
async def create():
    # create instance
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


# select
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
    return first, filter_result


# update
async def update(first, filter_result):
    first.a = 'update'
    await first.save()
    await filter_result.update(b=1)


# execute raw sql
async def execute():
    results = await execute_raw_sql('default', 'select b, count(*) from test where b = %s group by b;', (1,))
    async for val, cnt in results:
        print(val, cnt)


# execute raw sql
async def test():
    await Database.connect(**db_config)
    await create()
    first, filter_result = await select()
    await update(first, filter_result)
    await execute()


if __name__ == '__main__':
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
