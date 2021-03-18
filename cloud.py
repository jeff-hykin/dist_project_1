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

    def list_blocks(self):
        for each in self.bucket.objects.all():
            print(each)

    def read_block(self, offset):
        raise NotImplementedError

    def write_block(self, block, offset):
        raise NotImplementedError

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