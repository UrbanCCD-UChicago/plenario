import boto3
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import ast

from plenario.database import client


# converts floats to Decimal type to avoid dynamodb type errors
class _TypeSerializer(boto3.dynamodb.types.TypeSerializer):
    def serialize(self, value):
        if isinstance(value, float):
            value = Decimal(repr(value))
        dynamodb_type = self._get_dynamodb_type(value)
        serializer = getattr(self, '_serialize_%s' % dynamodb_type.lower())
        return {dynamodb_type: serializer(value)}


# converts dynamodbaccepted Decimal back to float type for output
class _TypeDeserializer(boto3.dynamodb.types.TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value)


serializer = _TypeSerializer()
deserializer = _TypeDeserializer()

ops = {
    "eq": "=",
    "gt": ">",
    "ge": ">=",
    "lt": "<",
    "le": "<=",
}


def query(args):
    params = ('network_name', 'nodes',
              'start_datetime', 'end_datetime',
              'filter')

    vals = (args.get(k) for k in params)
    network_name, nodes, start_datetime, end_datetime, filter = vals

    data = []
    attr_values = {
        ":start": {"S": start_datetime},
        ":end": {"S": end_datetime}
    }

    if filter:
        filter_expression = "results." + filter['col'] + " " + ops[filter['op']] + " :val"
        attr_values[":val"] = serializer.serialize(filter['val'])
    else:
        filter_expression = None

    for node in nodes:
        attr_values[":id"] = {"S": node}
        if filter_expression:
            response = client.query(
                TableName=network_name,
                KeyConditionExpression="id = :id AND #t BETWEEN :start AND :end",
                ExpressionAttributeValues=attr_values,
                FilterExpression=filter_expression,
                ExpressionAttributeNames={
                    "#t": "time"
                }
            )
        else:
            response = client.query(
                TableName=network_name,
                KeyConditionExpression="id = :id AND #t BETWEEN :start AND :end",
                ExpressionAttributeValues=attr_values,
                ExpressionAttributeNames={
                    "#t": "time"
                }
            )

        for item in response['Items']:
            for i in item.keys():
                item[i] = deserializer.deserialize(item[i])
            data.append(item)

    return data
