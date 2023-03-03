"""
Connector and methods accessing S3
"""

import os
import logging
from io import StringIO
import boto3
import pandas as pd

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        Listing all files with a prefix on the S3 bucket
        
        :param prefix: prefix in the object names stored on the S3 bucket which should be filtered
        
        returns:
            files: list of all file names containing the prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix = prefix)]
        return files

    def read_csv_to_df(self, key: str, decoding: str = 'utf-8', delimiter: str = ','):
        """
        Reading the csv files from the s3 bucket using the key

        :param key: key of the file that is to be read
        :param encoding: encoding of the data in csv, default used - 'utf-8'
        :param delimiter: separator of the csv file, default - ','
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key = key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data, delimiter = delimiter)
        return dataframe

    def write_df_to_s3(self):
        pass
