from six import iteritems


class Query(object):

    def __init__(self):
        self.recalculate_query = ''
        self.insert_select = ''
        self.insert_subqueries = ''
        self.insert_joins = ''
        self.insert_alternate_table_definitions = {}
        self.insert_query = ''

    def build(self, **kwargs):
        if 'insert_table' in kwargs.keys():
            self.insert_query = ' '.join([self.insert_subqueries, self.insert_select, self.insert_joins])
            table_definitions = {}
            for parameter, definition in iteritems(self.insert_alternate_table_definitions):
                if parameter != kwargs['insert_table'] + '_table_definition':
                    table_definitions[parameter] = definition
                else:
                    select_clauses = []
                    for key, hash_map in iteritems(kwargs['data']):
                        value = hash_map['value']
                        data_type = 'numeric' if hash_map['type'] == 'Number' else 'text'
                        select_clauses.append("'{value}'::{data_type} AS {key}".format(value=value, key=key,
                                                                                       data_type=data_type))
                    table_definitions[parameter] = 'SELECT ' + ', '.join(select_clauses)
            self.insert_query = self.insert_query.format(**table_definitions)
        return {'rebuild_sql': self.recalculate_query,
                'insert_sql': self.insert_query}

    def select(self, **kwargs):
        recalculate_clauses = []
        insert_clauses = []
        for name, value in iteritems(kwargs):
            recalculate_clauses.append('{schema}.{table}.{column} AS {column_name}'
                                       .format(schema=value.table.schema_name, table=value.table.table_name,
                                               column=value.column_name, column_name=name))
            insert_clauses.append('{table}.{column} AS {column_name}'
                                  .format(table=value.table.table_name, column=value.column_name, column_name=name))
        self.recalculate_query = 'SELECT ' + ', '.join(recalculate_clauses)
        self.insert_select = 'SELECT ' + ', '.join(insert_clauses)
        return self

    def from_table(self, table):
        self.recalculate_query += ' FROM {schema}.{table} '.format(schema=table.schema_name, table=table.table_name)
        self.insert_subqueries = ('WITH {table} AS ({table_definition})'
                                  .format(table=table.table_name,
                                          table_definition='{' + table.table_name + '_table_definition}'))
        self.insert_joins += 'FROM {table} '.format(table=table.table_name)
        self.insert_alternate_table_definitions[table.table_name + '_table_definition']\
            = 'SELECT * FROM {schema}.{table}'.format(schema=table.schema_name, table=table.table_name)
        return self

    def join(self, table, how='left', on=None, left_on=None, right_on=None):
        self.recalculate_query += '{join_type} JOIN {schema}.{table} '.format(join_type=how, schema=table.schema_name,
                                                                              table=table.table_name)
        self.insert_subqueries += (', {table} AS ({table_definition})'
                                   .format(table=table.table_name,
                                           table_definition='{' + table.table_name + '_table_definition}'))
        self.insert_alternate_table_definitions[table.table_name + '_table_definition'] \
            = 'SELECT * FROM {schema}.{table}'.format(schema=table.schema_name, table=table.table_name)
        self.insert_joins += '{join_type} JOIN {table} '.format(join_type=how, table=table.table_name)
        if on:
            self.recalculate_query += ('USING ({schema}.{table}.{column}) '
                                       .format(schema=on.table.schema_name, table=on.table.table_name,
                                               column=on.column_name))
            self.insert_joins += ('USING ({table}.{column}) '
                                  .format(table=on.table.table_name, column=on.column_name))
        elif left_on and right_on:
            self.recalculate_query += '''
                ON {left_schema}.{left_table}.{left_column} = {right_schema}.{right_table}.{right_column} 
            '''.format(left_schema=left_on.table.schema_name, left_table=left_on.table.table_name,
                       left_column=left_on.column_name, right_schema=right_on.table.schema_name,
                       right_table=right_on.table.table_name, right_column=right_on.column_name)
            self.insert_joins += '''
                ON {left_table}.{left_column} = {right_table}.{right_column} 
            '''.format(left_table=left_on.table.table_name, left_column=left_on.column_name,
                       right_table=right_on.table.table_name, right_column=right_on.column_name)
        else:
            raise ValueError("Missing required arguments: either 'on' or both 'left_on' and 'right_on' must be defined")

        return self

    # @TODO - handling transforms that involve aggregation!
    def group_by(self, **args):
        return self

    def functions(self):
        return self
