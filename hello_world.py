import psycopg2 as pg
from config import conn_params

from write.model import Model
from write.column_data_type import IntegerColumn, TextColumn

params = conn_params()


def level_zero():
    conn = pg.connect(host=params['host'], database=params['database'], user=params['user'])
    cur = conn.cursor()
    cur.execute("create table test_table(num_col INT, text_col TEXT)")
    cur.execute("insert into test_table (num_col, text_col) values (0, 'hello world')")
    conn.commit()
    cur.close()
    conn.close()


def level_one():
    class TestTable(Model):
        def __init__(self):
            Model.__init__(self, name='test_table')

        num_col = IntegerColumn()
        text_col = TextColumn()

    test_table = TestTable()
    conn = pg.connect(host=params['host'], database=params['database'], user=params['user'])
    cur = conn.cursor()
    cur.execute(test_table.create_sql())
    cur.execute(test_table.insert_sql(num_col=0, text_col='hello world'))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    level_one()
