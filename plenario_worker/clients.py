import boto3
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME
from plenario.settings import JOBS_QUEUE


sqs_client = boto3.client('sqs')
sqs_resource = boto3.resource('sqs')
job_queue_url = sqs_client.get_queue_url(QueueName=JOBS_QUEUE)['QueueUrl']
job_queue = sqs_resource.Queue(job_queue_url)

autoscaling_client = boto3.client(
    'autoscaling',
    region_name=AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
