import re
from unicodedata import normalize
import string
from csvkit.unicsv import UnicodeCSVReader
from plenario.utils.typeinference import normalize_column_type
import boto3
from plenario.settings import MAIL_USERNAME, ADMIN_EMAILS, \
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME
import math
from collections import namedtuple
from sqlalchemy import Table


def get_size_in_degrees(meters, latitude):
    earth_circumference = 40041000.0  # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference

    degrees_at_equator = meters * degrees_per_meter

    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))

    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator

    return degrees_x, degrees_y

ColumnInfo = namedtuple('ColumnInfo', 'name type_ has_nulls')


def infer_csv_columns(inp):
    """

    :param inp: File handle to a CSV dataset
                that we can throw into a UnicodeCSVReader
    :return: List of `ColumnInfo`s
    """
    reader = UnicodeCSVReader(inp)
    header = next(reader)
    inp.seek(0)
    iter_output = [iter_column(col_idx, inp)
                   for col_idx in range(len(header))]

    return [ColumnInfo(name, type_, has_nulls)
            for name, (type_, has_nulls) in zip(header, iter_output)]


def iter_column(idx, f):
    """

    :param idx: index of column
    :param f: gzip file object of CSV dataset
    :return: col_type, null_values
             where col_type is inferred type from typeinference.py
             and null_values is whether null values were found and normalized.
    """
    f.seek(0)
    reader = UnicodeCSVReader(f)

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


def slugify(text, delim='_'):
    """
    Given text, return lowercase ASCII slug that gets as close as possible to the original.
    Will fail on Asian characters.
    Taken from http://flask.pocoo.org/snippets/5/
    """
    if text:
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
        result = []
        for word in punct_re.split(text.lower()):
            word = normalize('NFKD', word).encode('ascii', 'ignore')
            if word:
                result.append(word)
        return delim.join(result)
    else:
        return text


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
        print(e.message, 'Failed to connect to AWS SES. Email aborted.')
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
                'Data': string.replace(body, '\r\n', '<br />')
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
        print(e.message, "Failed to send email through AWS SES.")

def reflect(table_name, metadata, engine):
    """Helper function for an oft repeated block of code.

    :param table_name: (str) table name
    :param metadata: (MetaData) SQLAlchemy object found in a declarative base
    :param engine: (Engine) SQLAlchemy object to send queries to the database
    :returns: (Table) SQLAlchemy object"""

    return Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )