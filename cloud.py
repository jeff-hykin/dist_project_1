from basic_defs import cloud_storage, NAS
from file_system import FS

import os
import sys
from array import array

class AWS_S3(cloud_storage):
    def __init__(self):
        aws_data = FS.json_read("./settings/passwords.dont-sync/aws.json")
        self.access_key_id     = aws_data["access_key_id"]
        self.access_secret_key = aws_data["access_secret_key"]
        self.bucket_name       = aws_data["bucket_name"]

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
        uses_aws, uses_azure, uses_gcs = _which_providers(file_system)
        # FIXME: I guess I'm supposed figure out the offset of the file
    
    def read(self, fd, len, offset):
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
    def _which_providers(self, filename):
        # hashed twice because the once-hashed value is already being used
        which_combo = hash(str(hash(filename))) % 3
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
    
    def _get_address(self, filename, block_number):
        # I know this looks weird, but it should prevent collisions
        # just doing a hash(filename+str(block_number)) would cause tonz of collisions
        hash_for_number = hash(str(block_number))
        hash_for_file = hash(filename)
        uuid_hopefully = hash(str(hash_for_file) + str(hash_for_number))
        return uuid_hopefully
    
    def _file_blocks(self, filepath):
        binary_array = FS.read(filepath, into="binary_array")
        # split it into blocks
        return [
            binary_array[i:i + self.block_size] for i in xrange(0, len(binary_array), self.block_size)
        ]