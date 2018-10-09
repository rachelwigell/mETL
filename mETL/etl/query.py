class Query(object):

    def __init__(self):
        pass

    def select(self, **kwargs):
        return Query()

    def from_table(self, table):
        return Query()

    def join(self, how='left', on=None, left_on=None, right_on=None):
        return Query()

    def group_by(self, **args):
        return Query()

    def functions(self):
        def array_agg(arg):
            return Query()
