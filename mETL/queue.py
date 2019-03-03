import boto3
import random
from six import iteritems
from .config import read_params


class Queue(object):
    def __init__(self, queue_name, aws_config_filename='aws_config.ini'):
        self.queue_name = queue_name

        aws_params = self.__read_config_file(aws_config_filename)

        session = boto3.session.Session(aws_access_key_id=aws_params['aws_access_key_id'],
                                        aws_secret_access_key=aws_params['aws_secret_access_key'],
                                        region_name=aws_params['region_name'])
        sqs = session.resource('sqs')
        self.sqs_queue = sqs.get_queue_by_name(QueueName=queue_name)

    @staticmethod
    def __read_config_file(filename):
        aws_params = read_params(filename, section='mETL')
        return aws_params

    def write_to_queue(self, **kwargs):
        data = {}
        for key, value in iteritems(kwargs):
            if type(value) == str:
                data_type = 'String'
            elif type(value) == int:
                data_type = 'Number'
            else:
                raise ValueError('Data types besides str and int not currently supported')
            data[key] = {'StringValue': str(value), 'DataType': data_type}

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
