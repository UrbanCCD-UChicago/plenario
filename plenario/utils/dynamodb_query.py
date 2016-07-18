import boto3
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import ast

from plenario.database import client

# converts dynamodbaccepted Decimal back to float type for output
class _TypeDeserializer(boto3.dynamodb.types.TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value) # DYNAMODB_CONTEXT.create_decimal(value)

deserializer = _TypeDeserializer()

def query(args):
    params = ('network_name', 'nodes',
              'start_datetime', 'end_datetime')

    vals = (args.get(k) for k in params)
    network_name, nodes, start_datetime, end_datetime = vals

    data = []
    for node in nodes:
        response = client.query(
            TableName=network_name,
            KeyConditionExpression="id = :id AND #t BETWEEN :start AND :end",
            ExpressionAttributeValues={
                ":id": {"S": node},
                ":start": {"S": start_datetime},
                ":end": {"S": end_datetime}
            },
            ExpressionAttributeNames={
                "#t": "time",
            }
        )
        for item in response['Items']:
            for i in item.keys():
                item[i] = deserializer.deserialize(item[i])
            data.append(item)

    return data

