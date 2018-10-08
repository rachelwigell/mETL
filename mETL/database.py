import psycopg2 as pg
from queue import Queue


class Database(object):
    """
    Define connection parameters to a database
    as well as an AWS SQS queue to send logs to write and read from
    """

    def __init__(self, queue_name, host='localhost', port=5432, database='postgres', user='postgres'):
        self.queue = Queue(queue_name)
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
        :return: The psycopg connection and cursor objects
        """

        conn = self.connect()
        cur = conn.cursor()
        cur.execute(sql_string)

        return conn, cur

    @staticmethod
    def commit_and_close(conn, cur):
        conn.commit()
        cur.close()



