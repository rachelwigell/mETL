from six import iteritems


class Model(object):
    """
    Superclass for defining the schema of a table
    """

    def __init__(self, table_name, schema_name):
        self.table_name = table_name
        self.schema_name = schema_name

    def create_sql(self):
        """
        Iterates over the columns defined on this table and produces a PostgreSQL string
        for creating the table
        :return: a PostgreSQL create statement
        """

        create_string = '''
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}(
        '''.format(schema_name=self.schema_name, table_name=self.table_name)

        col_array = []
        for column in self.__class__.__dict__:
            if not column.startswith('__'):
                column_obj = getattr(self, column)
                col_array.append('{name} {data_type}'.format(name=column, data_type=column_obj.postgres_name))

        create_string += ', '.join(col_array)
        create_string += ')'
        return create_string

    def insert_sql(self, **kwargs):
        """
        Iterates over the given column name/value pairs and creates a PostgreSQL insert string
        :param kwargs: name of the keyword should be the column name, value of the keyword should be the value to write
        :return: a PostgreSQL insert statement
        """

        insert_string = '''
            INSERT INTO {schema_name}.{table_name}(
        '''.format(schema_name=self.schema_name, table_name=self.table_name)

        col_array = []
        val_array = []
        for key, hash_map in iteritems(kwargs):
            value = hash_map['value'] if type(hash_map) == dict else hash_map
            col_array.append(key)
            value = "'{value}'".format(value=str(value))
            val_array.append(value)

        insert_string += ', '.join(col_array)
        insert_string += ') VALUES ('
        insert_string += ', '.join(val_array)
        insert_string += ')'
        return insert_string

    def delete_sql(self, **kwargs):
        delete_string = '''
            DELETE FROM {schema_name}.{table_name} WHERE 
        '''.format(schema_name=self.schema_name, table_name=self.table_name)

        clause_array = []
        for key, hash_map in iteritems(kwargs):
            value = hash_map['value'] if type(hash_map) == dict else hash_map
            clause_array.append('{column} = {value}'.format(column=key, value=value))

        delete_string += ' AND '.join(clause_array)
        return delete_string
