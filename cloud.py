# examples taken from: https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-object_wrapper.py.html
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import os
import random
import uuid

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

from basic_defs import cloud_storage, NAS
from file_system import FS

import os
import sys
from array import array
import hashlib
import boto3

hash_it = lambda value: hashlib.sha256(str(value).encode()).hexdigest()

class AWS_S3(cloud_storage):
    def __init__(self):
        aws_data = FS.json_read("./settings/passwords.dont-sync/aws.json")
        self.access_key_id     = aws_data["access_key_id"]
        self.access_secret_key = aws_data["access_secret_key"]
        self.bucket_name       = aws_data["bucket_name"]
        
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_secret_key,
        )
        self.session = boto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_secret_key
        )
        self.bucket = self.s3.Bucket(aws_data["bucket_name"])
    
    def _get(self, *args, **kwargs):
        return self.s3.Object(self.bucket_name, *args, **kwargs)
        
    def list_blocks(self):
        for each in self._list_objects():
            print(each.key)

    def read_block(self, offset):
        try:
            self._get_object(key=offset)
        except botocore.exceptions.ClientError as e:
            return None

    def write_block(self, block, offset):
        self._put_object()

    def delete_block(self, offset):
        raise NotImplementedError
    
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from boto3
    #     boto3.session.Session:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    #     boto3.resources:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    #     boto3.s3.Bucket:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    #     boto3.s3.Object:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object

    def _put_object(self, key, data):
        """
        Upload data to a bucket and identify it with the specified object key.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket to receive the data.
        :param key: The key of the object in the bucket.
        :param data: The data to upload. This can either be bytes or a string. When this
                    argument is a string, it is interpreted as a file name, which is
                    opened in read bytes mode.
        """
        bucket = self.bucket
        put_data = data
        # # convert strings to bytes
        # if type(data) == str:
        #     put_data = bytes(data, 'utf-8')

        try:
            obj = bucket.Object(key)
            obj.put(Body=put_data)
            obj.wait_until_exists()
            logger.info("Put object '%s' to bucket '%s'.", key, bucket.name)
        except ClientError:
            logger.exception("Couldn't put object '%s' to bucket '%s'.", key, bucket.name)
            raise
        finally:
            if getattr(put_data, 'close', None):
                put_data.close()


    def _get_object(self, key):
        """
        Gets an object from a bucket.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket that contains the object.
        :param key: The key of the object to retrieve.
        :return: The object data in bytes.
        """
        bucket = self.bucket
        try:
            body = bucket.Object(key).get()['Body'].read()
            logger.info("Got object '%s' from bucket '%s'.", key, bucket.name)
        except ClientError:
            logger.exception(("Couldn't get object '%s' from bucket '%s'.", key, bucket.name))
            raise
        else:
            return body


    def _list_objects(self, prefix=None):
        """
        Lists the objects in a bucket, optionally filtered by a prefix.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket to query.
        :param prefix: When specified, only objects that start with this prefix are listed.
        :return: The list of objects.
        """
        bucket = self.bucket
        try:
            if not prefix:
                objects = list(bucket.objects.all())
            else:
                objects = list(bucket.objects.filter(Prefix=prefix))
            logger.info("Got objects %s from bucket '%s'",
                        [o.key for o in objects], bucket.name)
        except ClientError:
            logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
            raise
        else:
            return objects

    def _delete_object(self, object_key):
        """
        Removes an object from a bucket.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket that contains the object.
        :param object_key: The key of the object to delete.
        """
        bucket = self.bucket
        try:
            obj = bucket.Object(object_key)
            obj.delete()
            obj.wait_until_not_exists()
            logger.info("Deleted object '%s' from bucket '%s'.", object_key, bucket.name)
        except ClientError:
            logger.exception("Couldn't delete object '%s' from bucket '%s'.",
                            object_key, bucket.name)
            raise

    def _delete_objects(self, object_keys):
        """
        Removes a list of objects from a bucket.
        This operation is done as a batch in a single request.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket that contains the objects.
        :param object_keys: The list of keys that identify the objects to remove.
        :return: The response that contains data about which objects were deleted
                and any that could not be deleted.
        """
        bucket = self.bucket
        try:
            response = bucket._delete_objects(Delete={
                'Objects': [ { 'Key': key } for key in object_keys ]
            })
            if 'Deleted' in response:
                logger.info(
                    "Deleted objects '%s' from bucket '%s'.",
                    [del_obj['Key'] for del_obj in response['Deleted']],
                    bucket.name
                )
            if 'Errors' in response:
                logger.warning(
                    "Sadly, could not delete objects '%s' from bucket '%s'.",
                    [ str(del_obj['Key'])+": "+str(del_obj['Code']) for del_obj in response['Errors']],
                    bucket.name
                )
        except ClientError:
            logger.exception()
            raise
        else:
            return response


class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        auth_info = FS.json_read("./settings/passwords.dont-sync/azure.json")
        self.key            = auth_info["key"]
        self.conn_str       = auth_info["conn_str"]
        self.account_name   = auth_info["account_name"]
        self.container_name = auth_info["container_name"]

    def list_blocks(self):
        raise NotImplementedError

    def read_block(self, offset):
        raise NotImplementedError

    def write_block(self, block, offset):
        raise NotImplementedError

    def delete_block(self, offset):
        raise NotImplementedError
    
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        self.credential_file = "./settings/passwords.dont-sync/gcp-credential.json"
        self.bucket_name = "csce678-s21-p1-326001802"

    def list_blocks(self):
        raise NotImplementedError

    def read_block(self, offset):
        raise NotImplementedError

    def write_block(self, block, offset):
        raise NotImplementedError

    def delete_block(self, offset):
        raise NotImplementedError
    
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from google.cloud.storage
    #    storage.client.Client:
    #        https://googleapis.dev/python/storage/latest/client.html
    #    storage.bucket.Bucket:
    #        https://googleapis.dev/python/storage/latest/buckets.html
    #    storage.blob.Blob:
    #        https://googleapis.dev/python/storage/latest/blobs.html

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]
    
    def open(self, filename):
        # this seems too simple ... but as far as I can tell it meets the requirements
        # (intentionally use builtin hash function)
        return hash(filename)
    
    
    def read(self, fd, len, offset):
        starting_point = offset
        how_many_bytes_to_read = len
        del len # len is a built in
        blocks = self._get_block_ranges(fd, start_index=starting_point, end_index=(starting_point+how_many_bytes_to_read))
        
        
        pass
    
    def write(self, fd, data, offset):
        pass
    
    def close(self, fd):
        pass
    
    def delete(self, filename):
        pass
    
    def get_storage_sizes(self):
        pass
    
    # helpers
    def _which_providers(self, hash_address):
        which_combo = hash(hash_address) % 3
        use_aws   = False
        use_azure = False
        use_gcs   = False
        if which_combo == 0:
            use_aws   = True
            use_azure = True
            # use_gcs   = False
        elif which_combo == 1:
            # use_aws   = False
            use_azure = True
            use_gcs   = True
        else:
            use_aws   = True
            # use_azure = False
            use_gcs   = True
        return (use_aws, use_azure, use_gcs)
    
    def _get_uuid(self, fd, block_index):
        # I know this looks weird, but it should prevent collisions
        # just doing a hash_it(filename+str(block_index)) would cause tonz of collisions
        hash_for_number = hash_it(block_index)
        hash_for_file = hash_it(fd)
        uuid_hopefully = hash_it(hash_for_file + hash_for_number)
        return uuid_hopefully
    
    def _get_address_range(self, start_block_index=0, start_offset=0, end_block_index=0, end_offset=0):
        start_byte_index = (self.block_size * start_block_index) + start_offset
        actual_start_block_index = self.block_size * (start_byte_index / self.block_size)
        actual_start_offset =  start_byte_index - actual_start_block_index
        
        end_byte_index = (self.block_size * end_block_index) + end_offset
        actual_end_block_index = self.block_size * (end_byte_index / self.block_size) 
        actual_end_offset =  end_byte_index - actual_end_block_index
        # goes at least until the end of the block
        if actual_end_block_index >= 1+actual_start_block_index:
            local_end = self.block_size
        elif actual_end_block_index == actual_start_block_index:
            local_end = actual_end_offset
        else:
            raise Exception('Something went wrong in _get_full_address_range(), end is before start\nfd='+str(fd)+'\n    start_block_index='+str(start_block_index)+'\n    start_offset='+str(start_offset)+'\n    end_block_index='+str(end_block_index)+'\n    end_offset='+str(end_offset))
        
        return actual_start_block_index, actual_start_offset, local_end
            
    def _get_segmentation(self, start_index, end_index):
        # no blocks
        if start_index == end_index:
            return []
        
        segments = []
        missing_number_of_bytes = end_index - start_index
        while missing_number_of_bytes > 0:
            block_index, local_start, local_end = self._get_address_range(start_offset=start_index, end_offset=end_index)
            bytes_gathered = local_end - local_start
            missing_number_of_bytes -= bytes_gathered
            segments.append((block_index, local_start, local_end))
        
        return segments
        
    def _get_block_ranges(self, fd, start_index, end_index):
        segments = self._get_segmentation(start_index, end_index)
        blocks = []
        for each in segments:
            (use_aws, use_azure, use_gcs) = self._which_providers(uuid)
            uuid = self._get_uuid(fd, block_index)
            segments.append(((use_aws, use_azure, use_gcs), uuid, local_start, local_end))
        
        return blocks