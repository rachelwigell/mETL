import boto3
from config import read_params
import random


class Queue(object):
    def __init__(self, queue_name, aws_config_filename='aws_config.ini'):
        self.queue_name = queue_name

        aws_params = self.read_config_file(aws_config_filename)

        session = boto3.session.Session(aws_access_key_id=aws_params['aws_access_key_id'],
                                        aws_secret_access_key=aws_params['aws_secret_access_key'],
                                        region_name=aws_params['region_name'])
        sqs = session.resource('sqs')
        self.sqs_queue = sqs.get_queue_by_name(QueueName=queue_name)

    @staticmethod
    def read_config_file(filename):
        aws_params = read_params(filename, section='mETL')
        return aws_params

    def write_to_queue(self, **kwargs):
        data = {}
        for key, value in kwargs.iteritems():
            data[key] = {'StringValue': str(value), 'DataType': 'String'}

        self.sqs_queue.send_message(
            MessageAttributes=data,
            MessageGroupId='mETL',
            MessageBody='mETL insert data',
            MessageDeduplicationId=str(random.getrandbits(128))
        )

    def read_from_queue(self):
        messages = self.sqs_queue.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=['.*'])
        if len(messages) != 0:
            message = messages[0]
            return message
        else:
            raise ValueError('No messages returned from queue {queue}'.format(queue=self.queue_name))
