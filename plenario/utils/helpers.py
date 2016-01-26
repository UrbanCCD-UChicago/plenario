import requests
import re
from unicodedata import normalize
import calendar
import string
from datetime import timedelta
from csvkit.unicsv import UnicodeCSVReader
from plenario.utils.typeinference import normalize_column_type
from flask_mail import Mail, Message
from plenario.settings import MAIL_DISPLAY_NAME, MAIL_USERNAME, ADMIN_EMAIL
from smtplib import SMTPAuthenticationError
import math

mail = Mail()


def get_size_in_degrees(meters, latitude):
    earth_circumference = 40041000.0 # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference

    degrees_at_equator = meters * degrees_per_meter

    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))

    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator

    return degrees_x, degrees_y


def iter_column(idx, f):
    """

    :param idx: index of column
    :param f: gzip file object of CSV dataset
    :return: col_type, null_values
             where col_type is inferred type from typeinference.py
             and null_values is whether null values were found and normalized.

             (It looks like normalize_column_type goes to the trouble
             of mutating a column that nobody ever uses. IDK why.)
    """
    f.seek(0)
    reader = UnicodeCSVReader(f)

    # Discard the header
    reader.next()

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


def slugify(text, delim=u'_'):
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
        return unicode(delim.join(result))
    else:
        return text


def send_mail(subject, recipient, body):
    msg = Message(subject,
                  sender=(MAIL_DISPLAY_NAME, MAIL_USERNAME),
                  recipients=[recipient], bcc=[ADMIN_EMAIL])

    msg.body = body
    msg.html = string.replace(msg.body, '\r\n', '<br />')
    try: 
        mail.send(msg)
    except SMTPAuthenticationError, e:
        print "error sending email"
