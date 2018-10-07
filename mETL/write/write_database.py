from database import Database


class WriteDatabase(Database):
    """
    Define connection parameters to a database
    as well as an AWS SQS queue to send logs to whenever writes occur
    """

    def __init__(self, queue, host='localhost', port=5432, database='postgres', user='postgres'):
        self.queue = queue
        Database.__init__(self, host=host, port=port, database=database, user=user)