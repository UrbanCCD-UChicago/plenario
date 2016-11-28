import botocore.exceptions
import boto3
import requests


def get_ec2_instance_id():
    """Retrieve the instance id for the currently running EC2 instance. If
    the host machine is not an EC2 instance or is for some reason unable
    to make requests, return None.

    :returns: (str) id of the current EC2 instance
              (None) if the id could not be found"""

    instance_id_url = "http://169.254.169.254/latest/meta-data/instance-id"
    try:
        return requests.get(instance_id_url, timeout=.5).text
    except requests.ConnectionError:
        print("Could not find EC2 instance id...")
        return None


def get_autoscaling_group():
    """Retrieve the autoscaling group name of the current instance. If
    the host machine is not an EC2 instance, not subject to autoscaling,
    or unable to make requests, return None.

    :returns: (str) id of the current autoscaling group
              (None) if the autoscaling group could not be found"""

    try:
        autoscaling_client = boto3.client("autoscaling", region_name="us-east-1")
        return autoscaling_client.describe_auto_scaling_instances(
            InstanceIds=[get_ec2_instance_id()]
        )["AutoScalingInstances"][0]["AutoScalingGroupName"]
    except botocore.exceptions.ParamValidationError:
        print("Bad params for autoscaling group ...")
    except botocore.exceptions.NoRegionError:
        print("Could not find autoscaling group region ...")
    except botocore.exceptions.ClientError:
        print("Could not create autoscaling client ...")


AUTOSCALING_GROUP = get_autoscaling_group()
INSTANCE_ID = get_ec2_instance_id()
