from model import Model


class RawModel(Model):
    """
    Superclass for defining the schema of a table
    """

    def __init__(self, table_name, schema_name='raw', source_table_name=None, source_schema_name='public'):
        Model.__init__(self, table_name=table_name, schema_name=schema_name)
        self.source_schema_name = source_schema_name
        if not source_table_name:
            self.source_table_name = table_name
        else:
            self.source_table_name = source_table_name


