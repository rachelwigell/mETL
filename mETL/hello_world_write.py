import psycopg2 as pg
from config import read_params

from write.write_model import WriteModel
from database import Database
from column_data_type import IntegerColumn, TextColumn


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
        num_col = IntegerColumn()
        text_col = TextColumn()

    test_table = TestTable(database=Database(queue_name='mETL.fifo', database='metl'), table_name='test_table')
    test_table.create()
    test_table.insert(num_col=0, text_col='hello world')


if __name__ == '__main__':
    level_two()
