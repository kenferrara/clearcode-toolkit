"""
    s3.py

    Shared functions used by the firmware pipeline to
    interact with Amazon S3.
"""

import logging
from typing import BinaryIO, IO, Union

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# change log level for boto logs
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)

s3_client = None
s3_resource = None


def __get_client():
    """ Returns an S3 client. Creates one if it doesn't exist yet """
    global s3_client
    if s3_client is None:
        s3_client = boto3.client('s3')
    return s3_client


def __get_resource():
    """ Returns an S3 resource. Creates one if it doesn't exist yet """
    global s3_resource
    if s3_resource is None:
        s3_resource = boto3.resource('s3')
    return s3_resource


def upload_data_to_s3(bucket, key, data) -> object:
    """ Uploads the given data to the bucket:key - returns an instance of s3.Object """
    logger.debug(f'Uploading object to S3 at s3://{bucket}/{key}')
    bucket = __get_resource().Bucket(bucket)
    s3_object = bucket.put_object(Key=key, Body=data)
    if s3_object is None:
        raise Exception(f'Empty object result after put_object for s3://{bucket}/{key}')
    return s3_object


def upload_file_to_s3(local_file_path: Union[str, BinaryIO, IO[bytes]], bucket, key, extra_args=None):
    """ Uploads the given local file to the bucket:key """
    if isinstance(local_file_path, str):
        logger.debug(f'Uploading file from {local_file_path} to S3 at s3://{bucket}/{key}')
        func_name = 'upload_file'
    else:
        logger.debug(f'Uploading file object to S3 at s3://{bucket}/{key}')
        func_name = 'upload_fileobj'

    getattr(__get_client(), func_name)(local_file_path, bucket, key, ExtraArgs=extra_args or {})


def download_file_from_s3(bucket, key, local_file_path, client=None):
    """ Downloads the given file from S3, saving it to a local path """
    logger.debug(f'Downloading file from S3 at s3://{bucket}/{key} to {local_file_path}')
    # TODO: Figure out a cleaner way around the 'custom client' support.
    #       Ideally we wouldn't need the `client` parameter, but when used
    #       en masse it is much more efficient to use a thread-local client
    #       instance, as is done by the sbom_processor rollup plugin.
    s3_client = client or __get_client()
    s3_client.download_file(bucket, key, local_file_path)


def open_object_from_s3(bucket, key) -> BinaryIO:
    """ Opens the given file from S3 without saving it locally.
        Returns a StreamingBody (see: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html#botocore.response.StreamingBody)
        which _does not_ implement readline/readlines, but rather iter_lines.  One convenient way to consume this
        is with codecs.getreader('utf-8') if the content is text-based.
    """
    logger.debug(f'Opening file from S3 at s3://{bucket}/{key}')
    s3_obj = __get_client().get_object(Bucket=bucket, Key=key)
    return s3_obj['Body']


def retrieve_data_from_s3(bucket, key):
    """ Downloads the given file from S3 without saving it locally.
        Returns the bytes from the body of the S3 object.
    """
    return open_object_from_s3(bucket, key).read()


def retrieve_data_chunk_from_s3(bucket, key, offset, max_bytes):
    """ Downloads a chunk of data from the given bucket/key.
        Returns the relevant bytes from the body of S3 object.
        The content will contain up to max_bytes or to the end
        of the object's content, whichever comes first.
    """
    logger.debug(f'Retrieving up to {max_bytes} bytes from S3 at s3://{bucket}/{key}, starting from offset {offset}')
    s3_obj = __get_client().get_object(Bucket=bucket, Key=key, Range=f'bytes={offset}-{offset + max_bytes - 1}')
    return s3_obj['Body'].read()


def s3_key_exists(bucket, key):
    """ Does the minimal amount of work possible to see if
        the given key exists in the given bucket
    """
    # This is super noisy. Re-enable if necesary for debugging.
    # logger.debug(f'Checking if s3://{bucket}/{key} exists')
    try:
        __get_resource().Object(bucket, key).load()
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Return false if the file doesn't exist
            return False
        else:
            # Something else has gone wrong.
            raise e
    else:
        return True


def get_s3_file_size(bucket, key):
    s3_obj = __get_client().get_object(Bucket=bucket, Key=key)
    return s3_obj['ContentLength']
