import psycopg2 as pg
from mETL.config import read_params

from mETL.write.write_model import WriteModel
from mETL.database import Database
from mETL.column_data_type import IntegerColumn, TextColumn


def level_zero():
    params = read_params(filename='database.ini', section='localhost')
    conn = pg.connect(host=params['host'], database=params['database'], user=params['user'])
    cur = conn.cursor()
    cur.execute("create table test_table(num_col INT, text_col TEXT)")
    cur.execute("insert into test_table (num_col, text_col) values (0, 'hello world')")
    conn.commit()
    cur.close()
    conn.close()


def level_one():
    class TestTable(WriteModel):
        def __init__(self):
            metl_queue = Queue('mETL.fifo')
            WriteModel.__init__(self, database=Database(queue=metl_queue, database='mETL'), table_name='test_table')

        num_col = IntegerColumn()
        text_col = TextColumn()

    test_table = TestTable()
    params = read_params(filename='database.ini', section='localhost')
    conn = pg.connect(host=params['host'], database=params['database'], user=params['user'])
    cur = conn.cursor()
    cur.execute(test_table.create_sql())
    cur.execute(test_table.insert_sql(num_col=0, text_col='hello world'))
    conn.commit()
    cur.close()
    conn.close()


def level_two():
    class TestTable(WriteModel):
        def __init__(self):
            metl_queue = Queue('mETL.fifo')
            WriteModel.__init__(self, database=Database(queue=metl_queue, database='metl'), table_name='test_table')

        num_col = IntegerColumn()
        text_col = TextColumn()

    test_table = TestTable(database=Database(queue_name='mETL.fifo', database='metl'), table_name='test_table')
    test_table.create()
    test_table.insert(num_col=0, text_col='hello world')


def level_three():
    class Colors(WriteModel):
        id = IntegerColumn()
        name = TextColumn()

    class Users(WriteModel):
        id = IntegerColumn()
        name = TextColumn()
        favorite_color_id = IntegerColumn()

    color_table = Colors(database=Database(queue_name='mETL.fifo', database='metl'), table_name='colors')
    user_table = Users(database=Database(queue_name='mETL.fifo', database='metl'), table_name='users')
    color_table.create()
    user_table.create()
    color_table.insert(id=0, name='red')
    color_table.insert(id=1, name='orange')
    color_table.insert(id=2, name='yellow')
    color_table.insert(id=3, name='green')
    color_table.insert(id=4, name='blue')
    color_table.insert(id=5, name='purple')
    user_table.insert(id=0, name='Rachel', favorite_color_id=4)
    user_table.insert(id=1, name='Matt', favorite_color_id=4)
    user_table.insert(id=2, name='Josh', favorite_color_id=3)


def level_four():
    class Colors(WriteModel):
        id = IntegerColumn()
        name = TextColumn()

    class Users(WriteModel):
        id = IntegerColumn()
        name = TextColumn()
        favorite_color_id = IntegerColumn()

    color_table = Colors(database=Database(queue_name='mETL.fifo', database='metl'), table_name='colors')
    user_table = Users(database=Database(queue_name='mETL.fifo', database='metl'), table_name='users')
    #color_table.delete(id=4)
    color_table.insert(id=4, name='blurple')

if __name__ == '__main__':
    level_four()
