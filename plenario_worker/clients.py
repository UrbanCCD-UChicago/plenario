import boto3
import botocore.exceptions
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME
from plenario.settings import JOBS_QUEUE


sqs_client = boto3.client(
    'sqs',
    region_name=AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

sqs_resource = boto3.resource(
    'sqs',
    region_name=AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

autoscaling_client = boto3.client(
    'autoscaling',
    region_name=AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

try:
    job_queue = sqs_resource.get_queue_by_name(QueueName=JOBS_QUEUE)
except botocore.exceptions.ClientError:
    job_queue = None
