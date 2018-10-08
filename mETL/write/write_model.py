from model import Model


class WriteModel(Model):
    """
    Superclass for defining the schema of a table
    """

    def __init__(self, database, table_name, schema_name='public'):
        Model.__init__(self, table_name=table_name, schema_name=schema_name)
        self.database = database

    def create(self):
        """
        Execute the table creation SQL against the given database
        """

        conn, cur = self.database.execute(self.create_sql())
        self.database.commit_and_close(conn, cur)

    def insert(self, **kwargs):
        """
        Execute the insertion SQL against the given database
        Don't commit the db operation until the queue has been written to successfully to ensure
        the queue and database stay in sync
        """

        conn, cur = self.database.execute(self.insert_sql(**kwargs))
        self.database.queue.write_to_queue(operation='insert', schema=self.schema_name, table=self.table_name, **kwargs)
        self.database.commit_and_close(conn, cur)
