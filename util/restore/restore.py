# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here

# AWS general settings
region_name = config.get('aws', 'AwsRegionName')

# Create AWS clients
sqs = boto3.client('sqs', region_name=region_name)
sns = boto3.client('sns', region_name=region_name)
s3 = boto3.client('s3', region_name=region_name)
glacier = boto3.client('glacier', region_name=region_name)
dynamodb = boto3.resource('dynamodb', region_name=region_name)

queue_url = config.get('sqs', 'RestoreQueueUrl')
vault_name = config.get('glacier', 'GlacierName')
table_name = config.get('dynamodb', 'TableName')
sns_arn = config.get('sns', 'ThawSns')

def initiate_restore(job_id, archive_id):
    # https://docs.aws.amazon.com/code-library/latest/ug/python_3_glacier_code_examples.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects-retrieval-options.html
    try:
        response = glacier.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Description': f'Restore job for {job_id}',
                'Tier': 'Expedited'
            }
        )
        print(f"Restore job initiated for {job_id} with Expedited tier")
        job_id = response['jobId']
        return job_id
    except ClientError as e:
        if e.response['Error']['Code'] == 'PolicyEnforcedException' or e.response['Error']['Code'] == 'InsufficientCapacityException':
            # Expedited tier unavailable, use Standard tier
            print(f"Expedited tier unavailable for {job_id}, switching to Standard tier")
            response = glacier.initiate_job(
                vaultName=vault_name,
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Description': f'Restore job for {job_id}',
                    'Tier': 'Standard'
                }
            )
            print(f"Restore job initiated for {job_id} with Standard tier")
            job_id = response['jobId']
            return job_id
        else:
            raise e


while True:
    # Long poll for messages from SQS queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Process up to 10 messages at a time
        WaitTimeSeconds=20
    )

    if 'Messages' in response:
        messages = response['Messages']
        for message in messages:
            body = json.loads(message['Body'])
            job_details = json.loads(body['Message'])
            job_id = job_details['job_id']

            # Get the archive ID from the job details
            archive_id = job_details['results_file_archive_id']

            # Initiate the restore job
            restore_job_id = initiate_restore(job_id, archive_id)

            # Update the DynamoDB item with the restore job ID
            # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
            table = dynamodb.Table(table_name)
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET restore_job_id = :restore_job_id',
                ExpressionAttributeValues={':restore_job_id': restore_job_id}
            )

            # Publish a message to the SNS topic for thawing the restored archive
            # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
            job_details['restore_job_id'] = restore_job_id
            
            sns.publish(
                TopicArn=sns_arn,
                Message=json.dumps(job_details),
                Subject=job_id
            )

            # Delete the message from the SQS queue
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )


### EOF