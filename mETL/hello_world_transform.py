from etl.raw_model import RawModel
from etl.transform_database import TransformDatabase
from column_data_type import IntegerColumn, TextColumn


def level_zero():
    class TestDatabase(TransformDatabase):

        class TestTable(RawModel):
            num_col = IntegerColumn()
            text_col = TextColumn()

        test_table = TestTable(table_name='test_table')

    test_database = TestDatabase(queue_name='mETL.fifo', database='metl')
    test_database.create_all_tables()
    message = test_database.queue.read_from_queue()
    test_database.process_queue_message(message)


if __name__ == '__main__':
    level_zero()
