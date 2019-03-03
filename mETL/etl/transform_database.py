from psycopg2 import ProgrammingError
from ..database import Database
from ..model import Model
from .raw_model import RawModel
from .transform import Transform


class TransformDatabase(Database):

    def __init__(self, queue_name, host='localhost', port=5432, database='postgres', user='postgres'):
        Database.__init__(self, queue_name, host=host, port=port, database=database, user=user)

    def create(self, model, connection=None, cursor=None):
        """
        Execute the creation SQL for the given model against the database
        """
        connection, cursor = self.execute(model.create_sql(), connection=connection, cursor=cursor)
        return connection, cursor

    def insert(self, model, connection=None, cursor=None, **kwargs):
        """
        Execute the insertion SQL for the given model against the database
        """

        connection, cursor = self.execute(model.insert_sql(**kwargs), connection=connection, cursor=cursor)
        return connection, cursor

    def insert_or_create(self, model, operation, table, connection=None, cursor=None, **args):
        """
        Attempt to do an insert. If the table doesn't exist, create it.
        """
        try:
            connection, cursor = self.execute(model.process_transaction(operation, table, **args),
                                              connection=connection, cursor=cursor)
        except ProgrammingError as e:
            if 'does not exist' in str(e):
                connection, cursor = self.create(model)
            else:
                raise e
        return connection, cursor

    def create_all_tables(self):
        """
        Executes the creation SQL for all the models defined on this database
        """

        for model in self.__class__.__dict__:
            if not model.startswith('__'):
                obj = getattr(self, model)
                if isinstance(obj, RawModel):
                    connection, cursor = self.create(model=obj)
                    self._commit_and_close(connection, cursor)

    def __get_table_by_name(self, table_name, schema_name):
        """
        Returns the model object for a table in this database with the given name
        :param table_name: name of the table to find
        :param schema_name: name of the schema that the table's in
        :return: model object for the corresponding table
        """

        for model in self.__class__.__dict__:
            if not model.startswith('__'):
                obj = getattr(self, model)
                if isinstance(obj, Model):
                    if obj.table_name == table_name and obj.schema_name == schema_name:
                        return obj
        raise ValueError('No table by name {schema}.{table} found in database {db}'.format(schema=schema_name,
                                                                                           table=table_name,
                                                                                           db=self.database))

    def __get_table_by_source_name(self, source_table_name, source_schema_name):
        """
        Returns the model object for a table in this database with the given source name
        :param source_table_name: name of the source table
        :param source_schema_name: name of the schema that the source table's in
        :return: model object for the corresponding table copy
        """

        for name in self.__class__.__dict__:
            if not name.startswith('__'):
                obj = getattr(self, name)
                if isinstance(obj, RawModel):
                    if obj.source_table_name == source_table_name and obj.source_schema_name == source_schema_name:
                        return obj
        raise ValueError('No table by with source name {schema}.{table} found in database {db}'.format(
            schema=source_schema_name,
            table=source_table_name,
            db=self.database))

    def __get_transforms_to_update(self, table_obj):
        """
        Returns all the transforms that are dependent on the given table
        :param table_obj: the table being modified
        :return: a list of table objects representing dependent transforms
        """
        tables = []

        for name in self.__class__.__dict__:
            if not name.startswith('__'):
                obj = getattr(self, name)
                if isinstance(obj, Transform):
                    if table_obj in obj.source_tables:
                        tables.append(obj)
        return tables

    def process_queue_message(self, message):
        attributes = message.message_attributes

        # update the copy
        copy_table = self.__get_table_by_source_name(source_schema_name=attributes['schema']['StringValue'],
                                                     source_table_name=attributes['table']['StringValue'])
        operation = attributes['operation']['StringValue']
        func = getattr(self, operation)
        args = {x: {'value': attributes[x]['StringValue'], 'type': attributes[x]['DataType']} for x in attributes
                if x not in ('operation', 'schema', 'table')}
        conn, cur = func(copy_table, **args)

        # update affected transforms
        transforms = self.__get_transforms_to_update(table_obj=copy_table)
        table = attributes['table']['StringValue']
        for transform in transforms:
            # conn, cur = self.create(transform, conn, cur)
            conn, cur = self.insert_or_create(transform, operation, table, conn, cur, **args)

        message.delete()

        self._commit_and_close(conn, cur)
