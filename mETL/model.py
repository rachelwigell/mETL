class Model(object):
    """
    Superclass for defining the schema of a table
    """

    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
