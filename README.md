# DynamoDB Data Archiving Script

This Python script is designed to archive data from a DynamoDB table to an S3 bucket. It leverages the Boto3 library for AWS integration and the concurrent.futures module for parallel processing. The script follows a multi-step process to efficiently archive data:

## Configuration
Before running the script, you need to configure the following parameters in the code:

- `table_name`: The name of the DynamoDB table from which data will be archived.
- `bucket_name`: The name of the S3 bucket where the archived data will be stored.
- `archive_filename`: The filename for the archived data in the S3 bucket.

## Script Overview

### 1. Data Retrieval
The script begins by scanning the DynamoDB table in parallel using multiple segments. Each segment is processed in a separate thread, and the results are consolidated.

### 2. Data Deletion
After retrieving the data, it is deleted from the DynamoDB table in batches. The script processes these deletions concurrently to optimize performance.

### 3. Retry Unprocessed Items
In case there are unprocessed deletion requests, the script retries them until all items have been deleted.

## How to Run

1. Ensure you have the required AWS credentials configured (e.g., AWS access key and secret key).
2. Modify the configuration parameters at the beginning of the script to match your setup.
3. Execute the script, and it will archive and delete data from the specified DynamoDB table.

## Dependencies

- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html): The Amazon Web Services (AWS) SDK for Python.
- [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html): A built-in Python library for concurrent execution of tasks.

## Note

- Make sure you have the necessary AWS IAM permissions to read from the DynamoDB table and write to the S3 bucket.
- This script is designed for archiving and deleting data from a DynamoDB table. Ensure that you understand the implications and have backups in place before using it in a production environment.

Feel free to reach out if you have any questions or need further assistance.
