from file_system import FS
import boto3

import cloud
reload(cloud); from cloud import AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage, RAID_on_Cloud; self = cloud.AWS_S3()