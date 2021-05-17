import config
from client_and_filekeys import s3, data_list
from config import Configuration
import os


def extraction():
    directory = config.Configuration.directory
    for key in data_list:
        s3.download_file(Bucket=Configuration.BUCKET, Key=key,
                         Filename=os.path.join(
                             directory, key)
                         )
