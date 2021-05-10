import config
from using_boto import s3, data_list
from config import Configuration
import os


directory = config.Configuration.directory
for key in data_list:
    s3.download_file(Bucket=Configuration.BUCKET, Key=key,
                     Filename=os.path.join(
                         directory, key)
                     )
