from database import Database
from model import Model


class TransformDatabase(Database):

    def __init__(self, queue_name, host='localhost', port=5432, database='postgres', user='postgres'):
        Database.__init__(self, queue_name, host=host, port=port, database=database, user=user)

    def create(self, model):
        """
        Execute the creation SQL for the given model against the database
        """

        conn, cur = self.execute(model.create_sql())
        self.commit_and_close(conn, cur)

    def insert(self, model, **kwargs):
        """
        Execute the insertion SQL for the given model against the database
        """

        conn, cur = self.execute(model.insert_sql(**kwargs))
        self.commit_and_close(conn, cur)

    def create_all_tables(self):
        """
        Executes the creation SQL for all the models defined on this database
        """

        for model in self.__class__.__dict__:
            if not model.startswith('__'):
                obj = getattr(self, model)
                if isinstance(obj, Model):
                    self.create(model=obj)

    def get_table_by_name(self, table_name, schema_name):
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

    def get_table_by_source_name(self, source_table_name, source_schema_name):
        """
        Returns the model object for a table in this database with the given source name
        :param source_table_name: name of the source table
        :param source_schema_name: name of the schema that the source table's in
        :return: model object for the corresponding table copy
        """

        for model in self.__class__.__dict__:
            if not model.startswith('__'):
                obj = getattr(self, model)
                if isinstance(obj, Model):
                    if obj.source_table_name == source_table_name and obj.source_schema_name == source_schema_name:
                        return obj

    def process_queue_message(self, message):
        print message
        copy_table = self.get_table_by_source_name(source_schema_name=message['schema']['StringValue'],
                                                   source_table_name=message['table']['StringValue'])
        operation = message['operation']['StringValue']
        func = getattr(self, operation)
        args = {x: message[x]['StringValue'] for x in message if x not in ('operation', 'schema', 'table')}
        func(copy_table, **args)
