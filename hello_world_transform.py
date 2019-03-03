from mETL.etl.raw_model import RawModel
from mETL.etl.transform_database import TransformDatabase
from mETL.column_data_type import IntegerColumn, TextColumn
from mETL.etl.transform import Transform
from mETL.etl.query import Query


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


def level_two():
    class ColorsDatabase(TransformDatabase):
        class Colors(RawModel):
            id = IntegerColumn()
            name = TextColumn()

        class Users(RawModel):
            id = IntegerColumn()
            name = TextColumn()
            favorite_color_id = IntegerColumn()

        colors = Colors(table_name='colors')
        users = Users(table_name='users')

        favorite_colors = Transform(table_name='favorite_colors', source_tables=[colors, users])
        favorite_colors.transform = (Query()
                                     .select(user_id=users.id, name=users.name,
                                             favorite_color_id=colors.id, favorite_color_name=colors.name)
                                     .from_table(users)
                                     .join(colors, how='inner', left_on=users.favorite_color_id, right_on=colors.id)
                                     .build)

    colors_database = ColorsDatabase(queue_name='mETL.fifo', database='metl')
    colors_database.create_all_tables()

    message = colors_database.queue.read_from_queue()
    #message = {'favorite_color_id': {'StringValue': '3', 'DataType': 'String'}, 'id': {'StringValue': '3', 'DataType': 'String'}, 'name': {'StringValue': 'Kevin', 'DataType': 'String'}, 'operation': {'StringValue': 'insert', 'DataType': 'String'}, 'schema': {'StringValue': 'public', 'DataType': 'String'}, 'table': {'StringValue': 'users', 'DataType': 'String'}}
    colors_database.process_queue_message(message)


if __name__ == '__main__':
    level_two()
