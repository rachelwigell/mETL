import psycopg2 as pg


class Database(object):
    """
    Define connection parameters to a database
    """

    def __init__(self, host='localhost', port=5432, database='postgres', user='postgres'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user

    def connect(self):
        """
        Connects to the database using the given parameters
        :return: a psycopg connection object
        """

        return pg.connect(host=self.host, port=self.port, database=self.database, user=self.user)

    def execute(self, sql_string):
        """
        Opens a connection to this database, executes the given SQL string, then closes the connection
        """

        conn = self.connect()
        cur = conn.cursor()
        cur.execute(sql_string)
        conn.commit()
        cur.close()
        conn.close()

