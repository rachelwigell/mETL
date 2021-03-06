from ..model import Model


class Transform(Model):
    def __init__(self, table_name, schema_name='transformed', source_tables=None):
        Model.__init__(self, table_name=table_name, schema_name=schema_name)
        if source_tables is None:
            self.source_tables = []
        else:
            self.source_tables = source_tables

    def transform(self, **kwargs):
        # all subclasses must define
        raise ValueError('transform must be defined for all instances of Transform!')

    def create_sql(self):
        return '''
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS ({transform})
        '''.format(schema=self.schema_name, table=self.table_name, transform=self.transform()['rebuild_sql'])

    def insert_sql(self, table, **args):
        return '''
            INSERT INTO {schema}.{table} {transform}
        '''.format(schema=self.schema_name, table=self.table_name,
                   transform=self.transform(insert_table=table, data=args)['insert_sql'])

    def delete_sql(self, table, **args):
        return '''
            DELETE FROM {schema}.{table} {where_clause}
        '''.format(schema=self.schema_name, table=self.table_name,
                   where_clause=self.transform(delete_table=table, data=args)['delete_sql'])
