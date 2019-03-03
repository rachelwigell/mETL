from model import Model
from column_data_type import ColumnDataType


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

        for name in self.__class__.__dict__:
            if not name.startswith('__'):
                obj = getattr(self, name)
                if isinstance(obj, ColumnDataType):
                    obj.table = self
                    obj.column_name = name


