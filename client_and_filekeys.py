import boto3
import config
# creation of s3 cli
s3 = boto3.client('s3')
s3.download_file(Bucket=config.Configuration.BUCKET, Key=config.Configuration.files_list_object_key,
                 Filename=config.Configuration.output_filename)
# creation of list of keys
data_list = list()
# and writing them to the file(output_filename)
with open(config.Configuration.output_filename, 'r') as f:
    for line in f:
        line = line.strip()
        data_list.append(line)







