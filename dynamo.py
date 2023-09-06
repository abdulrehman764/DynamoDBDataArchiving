import boto3
from concurrent.futures import ThreadPoolExecutor

# Configuration
table_name = 'test_and_test'
bucket_name = 'airflow-assignment-bucket'
archive_filename = 'archived_data.json'

dynamodb = boto3.client('dynamodb')

def process_batch(batch):
    try:
        print("Processing batch...")
        response = dynamodb.batch_write_item(RequestItems={table_name: batch})
        unprocessed_items = response.get('UnprocessedItems', {}).get(table_name, [])
        return unprocessed_items
    except Exception as e:
        print("Error:", e)
        return []

def parallel_scan(segment, exclusive_start_keys=None):
    items = []

    while True:
        scan_params = {
            'TableName': table_name,
            'FilterExpression': '#yr > :customer_id_val',
            'ExpressionAttributeNames': {"#yr": "customer_id"},
            'ExpressionAttributeValues': {":customer_id_val": {"N": "1000"}},
            'Segment': segment,
            'TotalSegments': 5,
        }

        if exclusive_start_keys and exclusive_start_keys[segment]:
            scan_params['ExclusiveStartKey'] = exclusive_start_keys[segment]

        response = dynamodb.scan(**scan_params)
        segment_items = response.get('Items', [])
        last_evaluated_key = response.get('LastEvaluatedKey')

        items.extend(segment_items)

        if last_evaluated_key is None:
            break
        else:
            exclusive_start_keys[segment] = last_evaluated_key

    return items

def parallel_delete(delete_requests, batch_size):
    batches = [delete_requests[i:i + batch_size] for i in range(0, len(delete_requests), batch_size)]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        unprocessed_items = list(executor.map(process_batch, batches))
    
    unprocessed_items = [item for sublist in unprocessed_items for item in sublist]
    return unprocessed_items

def retry_unprocessed(delete_requests, batch_size):
    unprocessed_items = delete_requests
    while unprocessed_items:
        print("Retrying unprocessed items...")
        new_unprocessed_items = []
        batch_to_process = []
        
        for item in unprocessed_items:
            batch_to_process.append(item)
            
            if len(batch_to_process) >= batch_size:
                new_unprocessed_items.extend(process_batch(batch_to_process))
                batch_to_process = []
        
        if batch_to_process:
            new_unprocessed_items.extend(process_batch(batch_to_process))
        
        unprocessed_items = new_unprocessed_items
        print("Remaining unprocessed items:", len(unprocessed_items))


def main():
    total_segments = 5
    exclusive_start_keys = [None] * total_segments
    total_items = []

    with ThreadPoolExecutor(max_workers=total_segments) as executor:
        futures = [executor.submit(parallel_scan, segment, exclusive_start_keys) for segment in range(total_segments)]

        for future in futures:
            items = future.result()
            total_items.extend(items)

    print(f"Total items: {len(total_items)}")
    
    batch_size = 25
    delete_requests = [
        {'DeleteRequest': {'Key': {'PK': {'S': item['PK']['S']}, 'SK': {'N': str(item['SK']['N'])}}}}
        for item in total_items
    ]
    
    unprocessed_items = parallel_delete(delete_requests, batch_size)
    retry_unprocessed(unprocessed_items, batch_size)


main()
