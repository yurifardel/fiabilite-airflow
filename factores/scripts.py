import glob
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def local_to_s3(bucket_name, dir_target, ds, filepath):
    s3 = S3Hook()
    if glob.glob(filepath):
        for f in glob.glob(filepath):
            print('file to move {}'.format(f))
            key = dir_target + ds + '/' + f.split('/')[-1]
            s3.load_file(filename=f, bucket_name=bucket_name, replace=True, key=key)
    else:
        raise ValueError('directory is empty no file to copy')

        
def remove_local_file(filepath):
    files = glob.glob(filepath)
    for f in files:
        os.remove(f)