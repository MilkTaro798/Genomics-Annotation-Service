## Archive Process (archive.py):  
1. The script reads the configuration from archive_config.ini and initializes AWS clients for SQS, S3, Glacier, and DynamoDB.
2. It enters an infinite loop to long poll for messages from the SQS queue specified in the configuration. It processes up to 10 messages at a time to improve throughput.
3. For each received message, it extracts the job details and checks if the user is a free user by retrieving the user profile from the accounts database using the get_user_profile helper function.
4. If the user is a free user, it uploads the results file from S3 to Glacier using the glacier.upload_archive method and captures the returned archive ID.
5. It updates the DynamoDB item for the job with the archive ID using the table.update_item method.
6. It deletes the results file from S3 using s3.delete_object.
7. Finally, it deletes the processed message from the SQS queue using sqs.delete_message.