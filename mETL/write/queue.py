import boto3
from config import read_params


class Queue(object):
    def __init__(self, queue_name, aws_config_filename='aws_config.ini'):
        self.queue_name = queue_name

        aws_params = self.read_config_file(aws_config_filename)

        session = boto3.session.Session(aws_access_key_id=aws_params['aws_access_key_id'],
                                        aws_secret_access_key=aws_params['aws_secret_access_key'],
                                        region_name=aws_params['region_name'])
        sqs = session.resource('sqs')
        self.sqs_queue = sqs.get_queue_by_name(QueueName=queue_name)

    def read_config_file(self, filename):
        aws_params = read_params(filename, section='mETL')
        return aws_params