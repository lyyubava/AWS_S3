import boto3
import config
import json

s3 = boto3.client('s3')
s3.download_file(Bucket=config.Configuration.BUCKET, Key=config.Configuration.files_list_object_key,
                 Filename=config.Configuration.output_filename)
# = bucket, Key = s3_file, Filename = os.path.join(output, s3_file)
data_list = list()
with open('data_list', 'r') as f:
    for line in f:
        line = line.strip()
        data_list.append(line)







