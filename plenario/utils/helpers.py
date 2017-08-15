import math

import agate
import boto3
import dateutil.parser
import sqlalchemy
from datetime import date
from slugify import slugify as _slugify
from sqlalchemy import Column, Table

from plenario.settings import ADMIN_EMAILS, AWS_ACCESS_KEY, AWS_REGION_NAME, AWS_SECRET_KEY, MAIL_USERNAME


def get_size_in_degrees(meters, latitude):
    earth_circumference = 40041000.0  # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference

    degrees_at_equator = meters * degrees_per_meter

    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))

    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator

    return degrees_x, degrees_y


class ParserInfo(dateutil.parser.parserinfo):
    """Restrict what counts as a valid date for dateutil."""

    AMPM = []
    HMS = []
    MONTHS = []
    PERTAIN = []
    WEEKDAYS = []


class Date(agate.Date):
    """Modified version of agate's date data type that isn't so insanely
    aggresive about parsing dates. Originally, it would infer 'sunday' or
    'tomorrow' as valid dates."""

    def __init__(self, date_format=None, **kwargs):
        super(Date, self).__init__(**kwargs)
        self.date_format = date_format

    def cast(self, value, **kwargs):
        if isinstance(value, date) or value is None:
            return value

        try:
            return dateutil.parser.parse(value, parserinfo=ParserInfo()).date()
        except (TypeError, ValueError):
            raise agate.CastError()


def infer(file):
    """Use agate to generate a list that describes a source csv's column types."""

    tester = agate.TypeTester(types=[
        agate.Boolean(),
        agate.Number(currency_symbols=[]),
        agate.TimeDelta(),
        Date(),
        agate.DateTime(),
        agate.Text()
    ])

    typemap = {
        agate.Boolean: sqlalchemy.Boolean,
        Date: sqlalchemy.Date,
        agate.DateTime: sqlalchemy.DateTime,
        agate.Number: sqlalchemy.Numeric,
        agate.Text: sqlalchemy.Text
    }

    file.seek(0)
    table = agate.Table.from_csv(file, column_types=tester)
    table = table.rename(slug_columns=True)
    types = [typemap[type(o)] for o in table.column_types]
    schema = zip(table.column_names, types)
    return [Column(name, dtype) for name, dtype in schema]


def infer_csv_columns(inp):
    return infer(inp)


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
