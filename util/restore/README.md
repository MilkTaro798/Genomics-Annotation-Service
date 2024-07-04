## Restore Process (restore.py):
1. The script reads the configuration from restore_config.ini and initializes AWS clients for SQS, SNS, S3, Glacier, and DynamoDB.
2. It defines the initiate_restore function, which initiates a Glacier restore job for a given job ID and archive ID. It first attempts to use the Expedited retrieval tier, and if it fails due to policy enforcement or insufficient capacity, it gracefully degrades to the Standard retrieval tier. 
3. It enters an infinite loop to long poll for messages from the SQS queue specified in the configuration, processing up to 10 messages at a time. 
4. For each received message, it extracts the job details and archive ID.
5. It calls the initiate_restore function to initiate the restore job and captures the restore job ID. 
6. It updates the DynamoDB item for the job with the restore job ID using table.update_item. 
7. It publishes a message to the SNS topic specified in the configuration, including the job details and restore job ID, to trigger the thaw process. 
8. Finally, it deletes the processed message from the SQS queue.