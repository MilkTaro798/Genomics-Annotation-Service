# thaw.py
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
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_config.ini')

# AWS general settings
region_name = config.get('aws', 'AwsRegionName')

# Create AWS clients
sqs = boto3.client('sqs', region_name=region_name)
s3 = boto3.client('s3', region_name=region_name)
glacier = boto3.client('glacier', region_name=region_name)
dynamodb = boto3.resource('dynamodb', region_name=region_name)

# Get SQS queue URL and Glacier vault name from config
queue_url = config.get('sqs', 'ThawQueueUrl')
vault_name = config.get('glacier', 'GlacierName')
table_name = config.get('dynamodb', 'TableName')

while True:
    try:
        # Receive messages from the queue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        # Process messages if any are received
        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                try:
                    # Extract job ID and restore job ID from the message
                    body = json.loads(message['Body'])
                    job_details = json.loads(body['Message'])
                    job_id = job_details['job_id']
                    restore_job_id = job_details['restore_job_id']

                    logging.info(f"Processing job {job_id} with restore job {restore_job_id}")

                    # Check the status of the restore job
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
                    # https://docs.aws.amazon.com/code-library/latest/ug/python_3_glacier_code_examples.html
                    restore_job_response = glacier.describe_job(
                        vaultName=vault_name,
                        jobId=restore_job_id
                    )

                    # If the restore job is completed, download the restored archive
                    # https://docs.aws.amazon.com/code-library/latest/ug/python_3_glacier_code_examples.html
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
                    if restore_job_response['Completed']:
                        archive_id = job_details['results_file_archive_id']
                        archive_response = glacier.get_job_output(
                            vaultName=vault_name,
                            jobId=restore_job_id
                        )

                        # Move the restored archive to the S3 bucket
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
                        s3_key_result_file = job_details['s3_key_result_file']
                        s3_results_bucket = job_details['s3_results_bucket']
                        s3.put_object(
                            Bucket=s3_results_bucket,
                            Key=s3_key_result_file,
                            Body=archive_response['body'].read()
                        )
                        logging.info(f"Moved restored archive to S3 bucket for job {job_id}")

                        # Delete the restored archive from Glacier
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
                        # https://docs.aws.amazon.com/code-library/latest/ug/glacier_example_glacier_Usage_RetrieveDelete_section.html
                        glacier.delete_archive(
                            vaultName=vault_name,
                            archiveId=archive_id
                        )
                        logging.info(f"Deleted restored archive from Glacier for job {job_id}")

                        # Update the DynamoDB item
                        # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
                        table = dynamodb.Table(table_name)
                        table.update_item(
                            Key={'job_id': job_id},
                            UpdateExpression='REMOVE results_file_archive_id, restore_job_id'
                        )
                        logging.info(f"Updated DynamoDB item for job {job_id}")
                        # Delete the message from the queue
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logging.info(f"Deleted message from SQS queue for job {job_id}")

                    else:
                        logging.info(f"Restore job {restore_job_id} for job {job_id} is not yet completed")



                except ClientError as e:
                    logging.error(f"Error processing message for job {job_id}: {str(e)}")
                    continue

                except Exception as e:
                    logging.error(f"Unexpected error processing message for job {job_id}: {str(e)}")
                    continue

    except ClientError as e:
        logging.error(f"Error receiving messages from SQS queue: {str(e)}")
        continue

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        continue

### EOF