import subprocess 
import uuid
import os
import shutil
import boto3
import urllib.parse
import json
import configparser

config = configparser.ConfigParser()
config.read('ann_config.ini')

region_name = config.get('default', 'region_name')
sqs_url = config.get('sqs', 'queue_url')
queue = boto3.resource('sqs', region_name=region_name).Queue(sqs_url)

table_name = config.get('dynamodb', 'table_name')
dynamodb = boto3.resource('dynamodb', region_name=region_name)
table = dynamodb.Table(table_name)

s3_inputs_bucket = config.get('s3', 'inputs_bucket')
s3 = boto3.resource('s3', region_name=region_name)


# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
while True:
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)

    for message in messages:
        notification = json.loads(message.body)
        data = json.loads(notification['Message'])
        job_id = data['job_id']
        user_id = data['user_id'] 
        input_file_name = data['input_file_name']
        s3_inputs_bucket = data['s3_inputs_bucket']
        s3_key_input_file = data['s3_key_input_file']
        submit_time = data['submit_time']
        
        job_dir = "jobs/{}".format(job_id)
        os.makedirs(job_dir, exist_ok=True)
        
        s3 = boto3.resource('s3', region_name='us-east-1') 
        local_file_path = os.path.join(job_dir, input_file_name)
        s3.Bucket(s3_inputs_bucket).download_file(s3_key_input_file, local_file_path)

        try:
            subprocess.Popen(["python", "anntools/run.py", local_file_path, job_id, user_id, input_file_name])
            
            message.delete()
        except Exception as e:
            print(f"Failed to submit job {job_id}: {e}")
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
        try:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression="set job_status = :r", 
                ConditionExpression="job_status = :p",
                ExpressionAttributeValues={
                    ':r': "RUNNING",
                    ':p': "PENDING"  
                }
            )
        except Exception as e:
            print(f"Failed to update job status in DynamoDB for job {job_id}: {e}")