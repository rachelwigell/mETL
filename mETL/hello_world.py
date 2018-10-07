import psycopg2 as pg
from config import read_params

from write.write_model import WriteModel
from write.write_database import WriteDatabase
from column_data_type import IntegerColumn, TextColumn
from write.queue import Queue


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
            WriteModel.__init__(self, database=WriteDatabase(queue=metl_queue, database='mETL'), table_name='test_table')

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
            WriteModel.__init__(self, database=WriteDatabase(queue=metl_queue, database='metl'), table_name='test_table')

        num_col = IntegerColumn()
        text_col = TextColumn()

    test_table = TestTable()
    test_table.create()
    test_table.insert(num_col=0, text_col='hello world')


if __name__ == '__main__':
    level_two()
