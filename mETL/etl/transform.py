from model import Model


class Transform(Model):
    def __init__(self, table_name, schema_name='transformed', source_tables=None):
        Model.__init__(self, table_name=table_name, schema_name=schema_name)
        if source_tables is None:
            self.source_tables = []
        else:
            self.source_tables = source_tables

    def transform(self):
        # all subclasses must define
        pass

    def recalculate_sql(self):
        recalculate_string = 'with '

        subq_array = []
        for table in self.source_tables:
            subq = '{name} as (select * from {schema}.{name})'.format(name=table.table_name,
                                                                      schema=table.schema_name)
            subq_array.append(subq)

        recalculate_string += ', '.join(subq_array)
        recalculate_string += ' '
        recalculate_string += self.transform()
        return recalculate_string

    def create_sql(self):
        return 'create table {schema}.{table} as ({recalculate_sql})'.format(schema=self.schema_name,
                                                                             table=self.table_name,
                                                                             recalculate_sql=self.recalculate_sql())

    def insert_sql(self, **kwargs):
        pass
