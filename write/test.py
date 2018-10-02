import psycopg2 as pg
from config import conn_params

conn_params = conn_params()
conn = pg.connect(host=conn_params['host'], database=conn_params['database'], user=conn_params['user'])
cur = conn.cursor()
cur.execute("insert into test_table (num_col, text_col) values (0, 'hello world')")
conn.commit()
cur.close()
conn.close()