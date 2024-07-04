# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Design Rationale:

* The archive, restore, and thaw processes are decoupled using SQS message queues and SNS topics. This allows for scalability and fault tolerance, as each process can run independently and asynchronously. If any step fails, the message remains in the queue and can be retried later.  
* Long polling is used when receiving messages from SQS queues to reduce the number of empty responses and improve efficiency.
* The archive process checks the user's role to determine if archiving is necessary, avoiding unnecessary Glacier operations for premium users.
* The restore process implements graceful degradation by first attempting Expedited retrieval and falling back to Standard retrieval if Expedited is unavailable. This ensures the restore process can continue even if the faster tier is not available.
* The thaw process checks the status of the restore job before attempting to retrieve the archive, ensuring that it only processes completed restore jobs.
* Error handling and logging are implemented to capture and log any errors that occur during processing, allowing for easier debugging and monitoring.
* DynamoDB is used to store job metadata, and the scripts use the update_item method with specific attributes to update the items. This approach is more efficient than a full table scan and allows for targeted updates of specific attributes.

Overall, the design aims to provide a scalable and fault-tolerant solution for archiving, restoring, and thawing results files. The use of message queues and asynchronous processing allows for high throughput and decoupling of the different stages. The graceful degradation in the restore process ensures that the system can continue to function even if the faster retrieval tier is not available. The targeted updates to DynamoDB items optimize performance by avoiding full table scans.