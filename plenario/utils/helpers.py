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

mail = Mail()


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

def get_socrata_data_info(host, path, four_by_four):
    errors = []
    status_code = None
    dataset_info = {}
    view_url = '%s/%s/%s' % (host, path, four_by_four)
    source_url = '%s/rows.csv?accessType=DOWNLOAD' % view_url

    try:
        r = requests.get(view_url)
        status_code = r.status_code
    except requests.exceptions.InvalidURL:
        errors.append('Invalid URL')
    except requests.exceptions.ConnectionError:
        errors.append('URL can not be reached')
    try:
        resp = r.json()
    except AttributeError:
        errors.append('No Socrata views endpoint available for this dataset')
        resp = None
    except ValueError:
        errors.append('The Socrata dataset you supplied is not available currently')
        resp = None
    if resp:
        columns = resp.get('columns')

        if columns:
            dataset_info = {
                'name': resp['name'],
                'description': resp.get('description'),
                'attribution': resp.get('attribution'),
                'columns': [],
                'view_url': view_url,
                'source_url': source_url
            }
            try:
                dataset_info['update_freq'] = \
                    resp['metadata']['custom_fields']['Metadata']['Update Frequency']
            except KeyError:
                dataset_info['update_freq'] = None
            for column in columns:
                d = {
                    'human_name': column['name'],
                    'machine_name': column['fieldName'],
                    #'field_name': column['fieldName'], # duplicate definition for code compatibility
                    #'field_name': column['name'], # duplicate definition for code compatibility
                    'field_name': slugify(column['name']), # duplicate definition for code compatibility
                    'data_type': column['dataTypeName'],
                    'description': column.get('description', ''),
                    'width': column['width'],
                    'sample_values': [],
                    'smallest': '',
                    'largest': '',
                }

                if column.get('cachedContents'):
                    cached = column['cachedContents']
                    if cached.get('top'):
                        d['sample_values'] = \
                            [c['item'] for c in cached['top']][:5]
                    if cached.get('smallest'):
                        d['smallest'] = cached['smallest']
                    if cached.get('largest'):
                        d['largest'] = cached['largest']
                    if cached.get('null'):
                        if cached['null'] > 0:
                            d['null_values'] = True
                        else:
                            d['null_values'] = False
                dataset_info['columns'].append(d)
        else:
            errors.append('Views endpoint not structured as expected')
    return dataset_info, errors, status_code


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

def increment_datetime_aggregate(sourcedate, time_agg):
    delta = None
    # if time_agg == 'hour':
    #     delta = timedelta(hours=1)
    if time_agg == 'day':
        delta = timedelta(days=1)
    elif time_agg == 'week':
        delta = timedelta(days=7)
    elif time_agg == 'month':
        _, days_to_add = calendar.monthrange(sourcedate.year, sourcedate.month)
        delta = timedelta(days=days_to_add)
    elif time_agg == 'quarter':
        _, days_to_add_1 = calendar.monthrange(sourcedate.year, sourcedate.month)
        _, days_to_add_2 = calendar.monthrange(sourcedate.year, sourcedate.month+1)
        _, days_to_add_3 = calendar.monthrange(sourcedate.year, sourcedate.month+2)
        delta = timedelta(days=(days_to_add_1 + days_to_add_2 + days_to_add_3))
    elif time_agg == 'year':
        days_to_add = 366 if calendar.isleap(sourcedate.year) else 365
        delta = timedelta(days=days_to_add)

    return sourcedate + delta

def send_mail(subject, recipient, body):
    msg = Message(subject,
              sender=(MAIL_DISPLAY_NAME, MAIL_USERNAME),
              recipients=[recipient], bcc=[ADMIN_EMAIL])

    msg.body = body
    msg.html = string.replace(msg.body,'\r\n','<br />')
    try: 
        mail.send(msg)
    except SMTPAuthenticationError, e:
        print "error sending email"
