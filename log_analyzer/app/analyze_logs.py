import boto3
import json
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize DynamoDB client with explicit region
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Replace 'your_region' with your actual region
table = dynamodb.Table('baitnet1')  # Replace with your DynamoDB table name

def read_logs_from_s3(bucket_name, prefix):
    s3 = boto3.client('s3', region_name='us-east-1')  # Ensure the region is specified here as well
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to list objects in bucket {bucket_name} with prefix {prefix}: {e}")
        return

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            try:
                if key.endswith('.json'):
                    obj_data = s3.get_object(Bucket=bucket_name, Key=key)
                    log_data = obj_data['Body'].read().decode('utf-8')
                    process_log_data(log_data)
            except boto3.exceptions.Boto3Error as e:
                logger.error(f"Failed to get object {key} from bucket {bucket_name}: {e}")

def process_log_data(log_data):
    logs = log_data.strip().split('\n')
    for log in logs:
        try:
            log_entry = json.loads(log)
            save_log_to_dynamodb(log_entry)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON log entry: {e}")

def save_log_to_dynamodb(log_entry):
    try:
        response = table.put_item(
            Item={
                'id': str(uuid.uuid4()),  # Unique identifier for the log entry
                'ContainerID': log_entry.get('container_id', ''),
                'ContainerName': log_entry.get('container_name', ''),
                'Source': log_entry.get('source', ''),
                'Log': log_entry.get('log', ''),
                'Message': log_entry.get('message', '')
            }
        )
        logger.info(f"Successfully inserted log entry into DynamoDB: {response}")
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to insert log entry into DynamoDB: {e}")

if __name__ == "__main__":
    bucket_name = 'baitfluentd'
    prefix = 'logs/'  # Adjust to your log file path
    read_logs_from_s3(bucket_name, prefix)