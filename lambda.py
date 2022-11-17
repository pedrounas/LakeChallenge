import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('glue')

glue_job_name = "Bogata-V3"


def lambda_handler(event, _):
    records = [x for x in event.get('Records', []) if x.get('eventName') == 'ObjectCreated:Put']
    sorted_events = sorted(records, key=lambda e: e.get('eventTime'))
    latest_event = sorted_events[-1] if sorted_events else {}
    info = latest_event.get('s3', {})
    file_key = info.get('object', {}).get('key')
    _ = client.start_job_run(
        JobName=glue_job_name,
        Arguments={
            '--file_key': file_key})
    return {
        'statusCode': 200
    }
