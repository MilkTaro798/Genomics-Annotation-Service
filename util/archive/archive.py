# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago

__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
import time
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_config.ini')

# AWS general settings
region_name = config.get('aws', 'AwsRegionName')

# Create AWS clients
sqs = boto3.client('sqs', region_name=region_name)
s3 = boto3.client('s3', region_name=region_name)
glacier = boto3.client('glacier', region_name=region_name)
dynamodb = boto3.resource('dynamodb', region_name=region_name)

# Get SQS queue URL and Glacier vault name from config
queue_url = config.get('sqs', 'ArchiveQueueUrl')
vault_name = config.get('glacier', 'GlacierName')
table_name = config.get('dynamodb', 'TableName')

while True:
    # Long poll for messages from SQS queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Process up to 10 messages at a time
        WaitTimeSeconds=20
    )

    # Process messages if any
    if 'Messages' in response:
        messages = response['Messages']
        for message in messages:
            body = json.loads(message['Body'])
            job_id = body['Subject']
            job_details = json.loads(body['Message'])

            # Get user profile from accounts database
            user_id = job_details['user_id']
            try:
                user_profile = helpers.get_user_profile(id=user_id)
            except Exception as e:
                print(f"Error retrieving user profile for job {job_id}: {str(e)}")
                continue

            # Check if user is a free user
            if user_profile['role'] == 'free_user':
                # Check if job completed more than 5 minutes ago
                complete_time = job_details['complete_time']
                # Upload results file to Glacier
                results_bucket = job_details['s3_results_bucket']
                results_key = job_details['s3_key_result_file']
                # https://docs.aws.amazon.com/amazonglacier/latest/dev/example_glacier_UploadArchive_section.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
                try:
                    archive_id = glacier.upload_archive(
                        vaultName=vault_name,
                        archiveDescription=f'Results file for job {job_id}',
                        body=s3.get_object(Bucket=results_bucket, Key=results_key)['Body'].read()
                    )['archiveId']
                    print(f"Results file for job {job_id} uploaded to Glacier with archive ID: {archive_id}")
                except ClientError as e:
                    print(f"Error uploading results file to Glacier for job {job_id}: {str(e)}")
                    

                # Update DynamoDB item with archive ID
                # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
                table = dynamodb.Table(table_name)
                try:
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression='SET results_file_archive_id = :archive_id',
                        ExpressionAttributeValues={':archive_id': archive_id}
                    )
                except ClientError as e:
                    print(f"Error updating DynamoDB item for job {job_id}: {str(e)}")
                    

                # Delete results file from S3
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
                try:
                    s3.delete_object(Bucket=results_bucket, Key=results_key)
                    print(f"Results file for job {job_id} deleted from S3")
                except ClientError as e:
                    print(f"Error deleting results file from S3 for job {job_id}: {str(e)}")
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                try:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Message for job {job_id} deleted from SQS queue")
                except ClientError as e:
                    print(f"Error deleting message from SQS queue for job {job_id}: {str(e)}")   
            else: # if user is a premium user
                # Delete message from SQS queue
                try:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Message for job {job_id} deleted from SQS queue")
                except ClientError as e:
                    print(f"Error deleting message from SQS queue for job {job_id}: {str(e)}")
                    

### EOF