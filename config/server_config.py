import boto3
import lakefs_client
from lakefs_client.client import LakeFSClient

# lakefs_url = 'http://lakefs-service:8000'
lakefs_url = 'http://www.opendataology.com:30910'


class LakefsConnectEntity:
    s3 = None
    client = None

    def __init__(self):
        """
       初始化
       :return:
       """
        self.s3 = boto3.client('s3',
                               endpoint_url=lakefs_url,
                               aws_access_key_id='AKIAJAJLUWEUICR7742Q',
                               aws_secret_access_key='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')

        configuration = lakefs_client.Configuration()
        configuration.username = 'AKIAJAJLUWEUICR7742Q'
        configuration.password = 'K2gnuqxF5xc4N9WHNqwBTe3y4RZt0g3lRmk4jY4W'
        configuration.host = lakefs_url
        self.client = LakeFSClient(configuration)
