# examples taken from: https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-object_wrapper.py.html
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import os
import random
import traceback

from botocore.exceptions import ClientError
import botocore

logger = logging.getLogger(__name__)

from basic_defs import cloud_storage, NAS
from file_system import FS

import os
import sys
from array import array
import hashlib
import boto3

hash_it = lambda value: hashlib.sha256(str(value).encode()).hexdigest()
length = len

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
        output = []
        for each in self._list_objects():
            try:
                output.append(int(each.key))
            except Exception as error:
                output.append(str(each.key))
        return output

    def read_block(self, offset):
        key = str(offset)
        try:
            return bytearray(self._get_object(key=key))
        except botocore.exceptions.ClientError as e:
            return None

    def write_block(self, block, offset):
        data = str(block)
        key = str(offset)
        try:
            self._delete_object(key=key)
        except Exception as error:
            pass
        return self._put_object(key=key, data=data)

    def delete_block(self, offset):
        key = str(offset)
        return self._delete_object(key=key)
    
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
    
    # 
    # helpers modified from AWS documentation (below)
    # 

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

        try:
            obj = bucket.Object(key)
            obj.put(Body=put_data)
            obj.wait_until_exists()
            # logger.info("Put object '%s' to bucket '%s'.", key, bucket.name)
        except ClientError:
            # logger.exception("Couldn't put object '%s' to bucket '%s'.", key, bucket.name)
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
            # logger.info("Got object '%s' from bucket '%s'.", key, bucket.name)
        except ClientError:
            # logger.exception(("Couldn't get object '%s' from bucket '%s'.", key, bucket.name))
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
            # logger.info("Got objects %s from bucket '%s'", [o.key for o in objects], bucket.name)
        except ClientError:
            # logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
            raise
        else:
            return objects

    def _delete_object(self, key):
        """
        Removes an object from a bucket.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket that contains the object.
        :param key: The key of the object to delete.
        """
        bucket = self.bucket
        try:
            obj = bucket.Object(key)
            obj.delete()
            obj.wait_until_not_exists()
            # logger.info("Deleted object '%s' from bucket '%s'.", key, bucket.name)
        except ClientError:
            # logger.exception("Couldn't delete object '%s' from bucket '%s'.", key, bucket.name)
            raise

    def _delete_objects(self, keys):
        """
        Removes a list of objects from a bucket.
        This operation is done as a batch in a single request.

        Usage is shown in usage_demo at the end of this module.

        :param bucket: The bucket that contains the objects.
        :param keys: The list of keys that identify the objects to remove.
        :return: The response that contains data about which objects were deleted
                and any that could not be deleted.
        """
        bucket = self.bucket
        try:
            response = bucket._delete_objects(Delete={
                'Objects': [ { 'Key': key } for key in keys ]
            })
            if 'Deleted' in response:
                # logger.info(
                #     "Deleted objects '%s' from bucket '%s'.",
                #     [del_obj['Key'] for del_obj in response['Deleted']],
                #     bucket.name
                # )
                pass
            if 'Errors' in response:
                # logger.warning(
                #     "Sadly, could not delete objects '%s' from bucket '%s'.",
                #     [ str(del_obj['Key'])+": "+str(del_obj['Code']) for del_obj in response['Errors']],
                #     bucket.name
                # )
                pass
        except ClientError:
            # logger.exception()
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
        
         # Instantiate a BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_client = self.blob_service_client.get_container_client(container=self.container_name)

    def list_blocks(self):
        output = []
        for each in self.container_client.list_blobs():
            try:
                output.append(int(each.name))
            except Exception as error:
                output.append(str(each.name))
        return output
            
    def read_block(self, offset):
        key = str(offset)
        try:
            blob_client = self.blob_service_client.get_blob_client(self.container_name, key)
            return bytearray(blob_client.download_blob().readall())
        except Exception as error:
            return None

    def write_block(self, block, offset):
        data = str(block)
        key = str(offset)
        try:
            self.delete_block(offset=key)
        except Exception as error:
            pass
        blob_client = self.blob_service_client.get_blob_client(self.container_name, key)
        return blob_client.upload_blob(data, overwrite=True)
        
    def delete_block(self, offset):
        key = str(offset)
        blob_client = self.blob_service_client.get_blob_client(self.container_name, key)
        return blob_client.delete_blob()
    
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python
    
    def _get_blob(self, key):
        return self.blob_service_client.get_blob_client(container=self.container_name, blob=key)
        
        

class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        from google.cloud import storage
        # Google Cloud Storage is authenticated with a **Service Account**
        self.credential_file = "./settings/passwords.dont-sync/gcp-credential.json"
        self.bucket_name = "csce678-s21-p1-326001802"
        self.client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.client.lookup_bucket(self.bucket_name)
            
    def list_blocks(self):
        output = []
        for each in self.bucket.list_blobs():
            try:
                output.append(int(each.name))
            except Exception as error:
                output.append(str(each.name))
        return output

    def read_block(self, offset):
        key = str(offset)
        blob = self.bucket.blob(key)
        try:
            return bytearray(blob.download_as_string())
        except Exception as error:
            return None

    def write_block(self, block, offset):
        data = str(block)
        key = str(offset)
        try:
            self.delete_block(offset=key)
        except Exception as error:
            pass
        blob = self.bucket.blob(key)
        return blob.upload_from_string(data)

    def delete_block(self, offset):
        key = str(offset)
        blob = self.bucket.blob(key)
        return blob.delete()
    
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
        self.block_size = 4096
        from collections import defaultdict
        self.is_open = defaultdict(lambda: False)
    
    def open(self, filename):
        # this seems too simple  but as far as I can tell it meets the requirements
        # (intentionally use builtin hash function)
        fd = hash(filename)
        self.is_open[fd] = True
        return fd
    
    
    def read(self, fd, len, offset):
        """
        # Reading the file descriptor up to the given number of bytes, at the given offset. Once the file is read successfully, the CLI will prints the output directly on the screen as UTF-8 strings.
        """
        if not self.is_open[fd]:
            return "" # I hope this is the right behavior
        try:
            
            starting_point = offset
            how_many_bytes = len
            del len # len is a built in
            block_ranges = self._get_block_ranges(fd, start_index=starting_point, end_index=(starting_point+how_many_bytes))
            FS.json_write(block_ranges, to="block_ranges.dont-sync.json")
            output = ""
            # for each block
            for (use_backend, block_id, local_start, local_end, is_final_block) in block_ranges:
                # increment for next block
                block_addition = None
                for use_service, backend in zip(use_backend, self.backends):
                    if use_service:
                        block_string = backend.read_block(offset=block_id)
                        if block_string is not None:
                            block_string = str(block_string)
                        # make resiliant by only working if the backend works
                        if type(block_string) == str:
                            block_addition = block_string[local_start:local_end]
                            # if success, don't retreive the block from both
                            break
                # fail immediate / log
                if type(block_addition) != str:
                    raise Exception('ERROR: block: '+str((use_backend, block_id, local_start, local_end, is_final_block))+'\n              (use_backend, block_id, local_start, local_end, is_final_block)\n\nhad an issue and wasnt able to get the data from any sources')
                output += block_addition
                
            # convert from fixed length into full unicode
            return self._garbled_ascii_to_utf8(output)
        except Exception as error:
            return ""
    
    def write(self, fd, data, offset):
        """
        Reading the string from the screen and write to the file descriptor at the given offset. The string will be read until the CLI detects a line break followed by a Control-D.
        """
        if not self.is_open[fd]:
            return # I hope this is the right behavior
        starting_point = offset
        # ensure that the data is properly encoded so we can write without issue and measure bytes without issue
        ascii_data     = self._utf8_to_garbled_ascii(data)
        how_many_bytes = len(ascii_data)
        block_ranges   = self._get_block_ranges(fd, start_index=starting_point, end_index=(starting_point+how_many_bytes))
        count = 0
        index = 0
        for (use_backend, block_uuid, local_start, local_end, is_final_block) in block_ranges:
            count += 1
            if len(block_ranges) == count:
                is_final_block = True
            
            amount_of_data = local_end - local_start
            # get the data based on the offset
            data_for_block = ascii_data[index: index + amount_of_data]
            # increment for next block
            index += amount_of_data
            
            # for each of backends that are pseudo-randomly selected
            for use_service, backend in zip(use_backend, self.backends):
                if use_service:
                    # create a filler of 0's if no data exists
                    base_data = str(bytearray(self.block_size))
                    prexisting_string = backend.read_block(offset=block_uuid)
                    if prexisting_string is None:
                        prexisting_string = ""
                    else:
                        prexisting_string = str(prexisting_string)
                    
                    base_data = prexisting_string + base_data
                    # the start is always filled up with some kind of data
                    pre_data = base_data[0:local_start]
                    # the ending data won't be preserved if this is the final block
                    post_data = base_data[local_end:self.block_size] if not is_final_block else ""
                    # if only writing in the middle, make sure to pad the sides
                    data_for_block = pre_data + data_for_block + post_data
                    # len(data_for_block) should be self.block_size for sure if its not the final block
                    
                    # save the whole block
                    backend.write_block(block=data_for_block, offset=block_uuid)
        
    
    def close(self, fd):
        self.is_open[fd] = False
    
    def delete(self, filename):
        fd = self.open(filename)
        file_prefix = self._get_prefix(fd)
        while True:
            for each_backend in self.backends:
                uuids = each_backend.list_blocks()
                for each_block_id in uuids:
                    if type(each_block_id) == str:
                        if each_block_id.startswith(file_prefix):
                            for backend in self.backends:
                                try:
                                    backend.delete_block(each_block_id)
                                except Exception as error:
                                    pass
            
            if len(self.read(fd, 1, 0)) > 0:
                print('self.read(fd, 10, 0) = ', self.read(fd, 1, 0))
                # wait until the job is done becase some of the backends 
                # don't seem to perform the operation synchronously
                import time
                time.sleep(0.5)
                continue
            else:
                break
    
    def _utf8_to_garbled_ascii(self, string):
        # is this very roundabout? yes
        # is it the only way I could find in python2 and be certain about? yes
        # (python2's unicode/ascii/binary has problems)
        import binascii
        string_bytes = bytearray(u""+string, encoding="utf-8")
        hex_string = binascii.hexlify(string_bytes)
        ascii_string = ""
        for index in range(0, len(hex_string), 2): 
            # extract two characters from hex string 
            part = hex_string[index : index + 2] 
            # change it into base 16 and 
            # typecast as the character  
            ascii_string += chr(int(part, 16)) 
        
        # python2 treats ascii the same as a bytes object in python3
        return ascii_string
    
    def _garbled_ascii_to_utf8(self, string):
        # we're hoping it is a properly encoded ascii
        return string.decode('utf8', errors='replace')
    
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
    
    def _get_prefix(self, fd):
        return str(fd)+"-"
    
    def _get_uuid(self, fd, block_index):
        # I know this looks weird, but it should prevent collisions
        # just doing a hash_it(filename+str(block_index)) would cause tonz of collisions
        hash_for_number = hash_it(block_index)
        hash_for_file = hash_it(fd)
        uuid_hopefully = hash_it(hash_for_file + hash_for_number)
        prefix = str(fd)+"-"
        return self._get_prefix(fd)+uuid_hopefully
    
    def _get_address_range(self, start_offset=0, end_offset=0):
        start_block_index = 0
        end_block_index = 0
        
        blocks_past = start_offset / self.block_size
        actual_start_block_index = self.block_size * blocks_past
        local_start = start_offset - actual_start_block_index
        
        blocks_till_end = end_offset / self.block_size
        local_end = min([ self.block_size, end_offset - actual_start_block_index ])
        
        if local_start > local_end:
            print('Something went wrong in _get_address_range(), end is before start\n    start_block_index='+str(start_block_index)+'\n    start_offset='+str(start_offset)+'\n    end_block_index='+str(end_block_index)+'\n    end_offset='+str(end_offset))
            raise Exception('Something went wrong in _get_address_range(), end is before start\n    start_block_index='+str(start_block_index)+'\n    start_offset='+str(start_offset)+'\n    end_block_index='+str(end_block_index)+'\n    end_offset='+str(end_offset))
        
        return actual_start_block_index, local_start, local_end
            
    def _get_segmentation(self, start_index, end_index):
        """
        :returns [(block_index, local_start, local_end)]
        """
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
            start_index = block_index + local_end
        
        return segments
        
    def _get_block_ranges(self, fd, start_index, end_index):
        """
        returns [ ((use_aws, use_azure, use_gcs), uuid, local_start, local_end, is_final_block) ]
        """
        segments = self._get_segmentation(start_index, end_index)
        blocks = []
        for block_index, local_start, local_end in segments:
            block_uuid = self._get_uuid(fd, block_index)
            (use_aws, use_azure, use_gcs) = self._which_providers(block_uuid)
            blocks.append(((use_aws, use_azure, use_gcs), block_uuid, local_start, local_end, False))
        
        # set the "is_final_block" for the last one
        if len(blocks) > 1:
            (use_aws, use_azure, use_gcs), block_uuid, local_start, local_end, _ = blocks[-1]
            blocks[-1] = ((use_aws, use_azure, use_gcs), block_uuid, local_start, local_end, True)
            
        return blocks
    