from file_system import FS
import boto3

def info(a):
    print(a)
    for each in dir(a):
        print(each)        

import cloud;reload(cloud); from cloud import AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage, RAID_on_Cloud; self = cloud.AWS_S3()

reload(cloud); from cloud import AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage, RAID_on_Cloud; self = cloud.Azure_Blob_Storage()

z = Azure_Blob_Storage()