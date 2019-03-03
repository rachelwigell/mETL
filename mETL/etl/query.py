class Query(object):

    def __init__(self):
        self.query = ''

    def build(self):
        return self.query

    def select(self, **kwargs):
        clauses = []
        for name, value in kwargs.iteritems():
            clauses.append('{schema}.{table}.{column} as {column_name}'
                           .format(schema=value.table.schema_name, table=value.table.table_name,
                                   column=value.column_name, column_name=name))
        self.query = 'select ' + ', '.join(clauses)
        return self

    def from_table(self, table):
        self.query += ' from {schema}.{table} '.format(schema=table.schema_name,
                                                      table=table.table_name)
        return self

    def join(self, table, how='left', on=None, left_on=None, right_on=None):
        self.query += '{join_type} join {schema}.{table} '.format(join_type=how, schema=table.schema_name,
                                                                  table=table.table_name)
        if on:
            self.query += ('using ({schema}.{table}.{column}) '
                           .format(schema=on.table.schema_name, table=on.table.table_name, column=on.column_name))
        elif left_on and right_on:
            self.query += ('on {left_schema}.{left_table}.{left_column} = {right_schema}.{right_table}.{right_column} '
                           .format(left_schema=left_on.table.schema_name, left_table=left_on.table.table_name,
                                   left_column=left_on.column_name, right_schema=right_on.table.schema_name,
                                   right_table=right_on.table.table_name, right_column=right_on.column_name))
        return self

    # @TODO - handling transforms that involve aggregation!
    def group_by(self, **args):
        return self

    def functions(self):
        return self
