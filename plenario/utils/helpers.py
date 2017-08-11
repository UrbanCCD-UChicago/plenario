import agate
import csv
import math
from collections import namedtuple

import sqlalchemy
import boto3
from slugify import slugify as _slugify
from sqlalchemy import Column, Table

from plenario.settings import ADMIN_EMAILS, AWS_ACCESS_KEY, AWS_REGION_NAME, AWS_SECRET_KEY, MAIL_USERNAME
from plenario.utils.typeinference import normalize_column_type


def get_size_in_degrees(meters, latitude):
    earth_circumference = 40041000.0  # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference

    degrees_at_equator = meters * degrees_per_meter

    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))

    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator

    return degrees_x, degrees_y


ColumnInfo = namedtuple('ColumnInfo', 'name type_ has_nulls')


def typeinfer(file) -> list:
    """Use agate to a dictionary that describes csv column types."""

    typemap = {
        agate.Boolean: sqlalchemy.Boolean,
        agate.Date: sqlalchemy.Text,
        agate.DateTime: sqlalchemy.DateTime,
        agate.Number: sqlalchemy.Numeric,
        agate.Text: sqlalchemy.Text
    }

    tester = agate.TypeTester(limit=1000)
    table = agate.Table.from_csv(file, column_types=tester)
    table = table.rename(slug_columns=True)
    types = [typemap[type(o)] for o in table.column_types]
    schema = zip(table.column_names, types)
    return [Column(name, dtype) for name, dtype in schema]


def infer_csv_columns(inp):
    return typeinfer(inp)

def iter_column(idx, f):
    """
    :param idx: index of column
    :param f: gzip file object of CSV dataset
    :return: col_type, null_values
             where col_type is inferred type from typeinference.py
             and null_values is whether null values were found and normalized.
    """
    f.seek(0)
    reader = csv.reader(f)

    # Discard the header
    next(reader)

    col = []
    for row in reader:
        if row:
            try:
                col.append(row[idx])
            except IndexError:
                # Bad data. Maybe we can fill with nulls?
                pass
    col_type, null_values = normalize_column_type(col)
    return col_type, null_values


def slugify(text: str, delimiter: str = '_') -> str:
    return _slugify(text, separator=delimiter)


def send_mail(subject, recipient, body):
    # Connect to AWS Simple Email Service
    try:
        ses_client = boto3.client(
            'ses',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION_NAME
        )
    except Exception as e:
        print(e, 'Failed to connect to AWS SES. Email aborted.')
        return

    destination = {
        'ToAddresses': [recipient],
        'BccAddresses': ADMIN_EMAILS
    }

    message = {
        'Subject': {'Data': subject},
        'Body': {
            'Text': {
                'Data': body
            },
            'Html': {
                'Data': str.replace(body, '\r\n', '<br />')
            }
        }
    }

    # Send email from MAIL_USERNAME
    try:
        ses_client.send_email(
            Source=MAIL_USERNAME,
            Destination=destination,
            Message=message
        )
    except Exception as e:
        print(e, 'Failed to send email through AWS SES.')


def reflect(table_name, metadata, engine):
    """Helper function for an oft repeated block of code.

    :param table_name: (str) table name
    :param metadata: (MetaData) SQLAlchemy object found in a declarative base
    :param engine: (Engine) SQLAlchemy object to send queries to the database
    :returns: (Table) SQLAlchemy object
    """
    return Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )
