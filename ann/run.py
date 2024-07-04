# hw5_run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import os
import boto3
import shutil
import configparser
import json
from botocore.exceptions import ClientError


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('ann_config.ini')

    region_name = config.get('default', 'region_name')
    CNetID = 'sun00545'
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1]
        job_id = sys.argv[2]
        user_id = sys.argv[3]
        input_file_name = sys.argv[4]
        with Timer():
            driver.run(input_file_path, 'vcf')

        # Add code to save results and log files to S3 results bucket
        s3 = boto3.resource('s3', region_name=region_name)
        s3_results_bucket = config.get('s3', 'results_bucket')

        # 1. Upload the results file
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        results_file = os.path.join(os.path.dirname(input_file_path), f"{os.path.splitext(input_file_name)[0]}.annot.vcf")
        s3.Bucket(s3_results_bucket).upload_file(results_file, f"sun00545/{user_id}/{job_id}/{os.path.basename(results_file)}")

        # 2. Upload the log file
        log_file = os.path.join(os.path.dirname(input_file_path), f"{os.path.splitext(input_file_name)[0]}.vcf.count.log")
        s3.Bucket(s3_results_bucket).upload_file(log_file, f"sun00545/{user_id}/{job_id}/{os.path.basename(log_file)}")
        
        dynamodb = boto3.resource('dynamodb', region_name=region_name)
        table_name = config.get('dynamodb', 'table_name') 
        table = dynamodb.Table(table_name)
        complete_time = int(time.time())
        s3_key_result_file = f"{CNetID}/{user_id}/{job_id}/{os.path.splitext(input_file_name)[0]}.annot.vcf"
        s3_key_log_file = f"{CNetID}/{user_id}/{job_id}/{os.path.splitext(input_file_name)[0]}.vcf.count.log"
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
        try:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression="SET job_status = :c, s3_results_bucket = :b, s3_key_result_file = :r, s3_key_log_file = :l, complete_time = :t",
                ConditionExpression="job_status = :s",
                ExpressionAttributeValues={
                    ':c': "COMPLETED",
                    ':b': s3_results_bucket,
                    ':r': s3_key_result_file,
                    ':l': s3_key_log_file,
                    ':t': complete_time,
                    ':s': "RUNNING"
                }
            )
        except Exception as e:
            print(f"Failed to update job status in DynamoDB: {e}")
            raise
        
        # 3. Clean up (delete) local job files
        os.remove(input_file_path)
        os.remove(results_file)
        os.remove(log_file)
        jobs_path =  f"jobs/{job_id}"
        shutil.rmtree(jobs_path)
        # public message 
        # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
        results_sns = config.get('sns', 'results_sns')
        archive_sns = config.get('sns', 'archive_sns')
        sns = boto3.client('sns', region_name=region_name)
        
        status_response = table.get_item(Key = { 'job_id': job_id })
        data = status_response.get('Item', None)
        if data:
            data['submit_time'] = int(data['submit_time'])
            data['complete_time'] = int(data['complete_time'])
            try:
                response = sns.publish(
                    TopicArn=results_sns,
                    Message=json.dumps(data),
                    Subject=job_id
                )
            except ClientError as e:
                print(f"Failed to publish results SNS message for job {job_id}: {e}")
                raise
            try:
                response = sns.publish(
                    TopicArn=archive_sns,
                    Message=json.dumps(data),
                    Subject=job_id
                )
            except ClientError as e:
                print(f"Failed to publish archive SNS message for job {job_id}: {e}")
                raise
    else:
        print("A valid .vcf file must be provided as input to this program.")
### EOF