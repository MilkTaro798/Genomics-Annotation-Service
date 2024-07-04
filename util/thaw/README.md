## Thaw Process (thaw.py):
1. The script reads the configuration from thaw_config.ini and initializes AWS clients for SQS, S3, Glacier, and DynamoDB.
2. It enters an infinite loop to long poll for messages from the SQS queue specified in the configuration, processing up to 10 messages at a time. 
3. For each received message, it extracts the job details, job ID, and restore job ID.
4. It checks the status of the restore job using glacier.describe_job. 
5. If the restore job is completed, it retrieves the restored archive using glacier.get_job_output. 
6. It moves the restored archive to the S3 results bucket using s3.put_object. 
7. It deletes the restored archive from Glacier using glacier.delete_archive. 
8. It updates the DynamoDB item for the job, removing the results_file_archive_id and restore_job_id attributes using table.update_item. 
9. Finally, it deletes the processed message from the SQS queue.