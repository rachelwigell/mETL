class ColumnDataType(object):
    """
    Superclass for defining a data type of a column
    """

    def __init__(self, postgres_name):
        self.postgres_name = postgres_name


class IntegerColumn(ColumnDataType):
    def __init__(self):
        ColumnDataType.__init__(self, postgres_name='INT')


class TextColumn(ColumnDataType):
    def __init__(self):
        ColumnDataType.__init__(self, postgres_name='TEXT')