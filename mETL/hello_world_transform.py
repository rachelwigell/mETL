from etl.raw_model import RawModel
from etl.transform_database import TransformDatabase
from column_data_type import IntegerColumn, TextColumn
from etl.transform import Transform


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


def level_one():
    class ColorsDatabase(TransformDatabase):

        class Colors(RawModel):
            id = IntegerColumn()
            name = TextColumn()

        class Users(RawModel):
            id = IntegerColumn()
            name = TextColumn()
            favorite_color_id = IntegerColumn()

        color_table = Colors(table_name='colors')
        user_table = Users(table_name='users')

        class FavoriteColors(Transform):
            def transform(self):
                return '''
                    select users.id as user_id, users.name, favorite_color_id, colors.name as favorite_color_name
                    from colors
                    join users
                    on colors.id = users.favorite_color_id
                '''

        favorite_colors = FavoriteColors(table_name='favorite_colors', source_tables=[color_table, user_table])

    colors_database = ColorsDatabase(queue_name='mETL.fifo', database='metl')
    colors_database.create_all_tables()

    message = colors_database.queue.read_from_queue()
    colors_database.process_queue_message(message)

if __name__ == '__main__':
    level_one()
