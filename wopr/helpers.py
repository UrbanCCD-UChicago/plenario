import requests
import time
from base64 import b64decode
import os
import json
import csv
from operator import itemgetter
from itertools import groupby
from datetime import datetime, timedelta
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from hashlib import sha1
from wopr.database import engine, Base
from wopr.models import crime_table
import gzip

CRIMES = 'https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD'
AWS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET = os.environ['AWS_SECRET_KEY']
DATA_DIR = os.environ['WOPR_DATA_DIR']

class SocrataError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

def get_crimes(fpath=None):
    if not fpath:
        r = requests.get(CRIMES, stream=True)
        fpath = '%s/crime_%s.csv' % (DATA_DIR, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        with gzip.open(os.path.join(fpath), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
    crime_src_table = crime_table('src_chicago_crimes_all', Base.metadata)
    crime_src_table.drop(bind=engine, checkfirst=True)
    crime_src_table.create(bind=engine)
    # Refactor to use COPY FROM
    with gzip.open(fpath, 'rb') as f:
        header = crime_src_table.columns.keys()
        reader = csv.DictReader(f, fieldnames=header)
        reader.next()
        rows = []
        for row in reader:
            r = {}
            for k,v in row.items():
              if row.get(k):
                  r[k] = v
              else:
                  r[k] = None
            rows.append(r)
            if len(rows) % 100000 == 0:
                ins = crime_src_table.insert()
                engine.execute(ins, rows)
                rows = []
    return 'Done!'
    #case_numbers = [c['case_number'] for c in crimes]
    #existing = 0
    #new = 0
    #dates = []
    #for crime in crimes:
    #    try:
    #        crime['location'] = {
    #            'type': 'Point',
    #            'coordinates': (float(crime['longitude']), float(crime['latitude']))
    #        }
    #    except KeyError:
    #        crime['location'] = geocode_it(crime['block'], coll)
    #    crime['updated_on'] = datetime.strptime(crime['updated_on'], '%Y-%m-%dT%H:%M:%S')
    #    crime['date'] = datetime.strptime(crime['date'], '%Y-%m-%dT%H:%M:%S')
    #    if crime['arrest'] == 'true':
    #        crime['arrest'] = True
    #    elif crime['arrest'] == 'false':
    #        crime['arrest'] = False
    #    if crime['domestic'] == 'true':
    #        crime['domestic'] = True
    #    elif crime['domestic'] == 'false':
    #        crime['domestic'] = False
    #    dates.append(crime['date'])
    #    crime_update = {}
    #    for k,v in crime.items():
    #        new_key = '_'.join(k.split()).lower()
    #        crime_update[new_key] = v
    #    try:
    #        iucr = str(int(crime_update['iucr']))
    #    except ValueError:
    #        iucr = crime_update['iucr']
    #    crime_update['iucr'] = iucr
    #    try:
    #        crime_type = iucr_codes.find_one({'iucr': iucr})['type']
    #    except (TypeError, KeyError):
    #        crime_type = None
    #    crime_update['type'] = crime_type
    #    update = coll.update({'case_number': crime['case_number']}, crime_update, upsert=True)
    #    if update['updatedExisting']:
    #        existing += 1
    #    else:
    #        new += 1
    ## skipped, committed = update_crimediffs(case_numbers)
    #unique_dates = list(set([datetime.strftime(d, '%Y%m%d') for d in dates]))
    #weather_updated = get_weather(unique_dates)
    #return 'Updated %s, Created %s %s' % (existing, new, weather_updated)

def get_weather(dates):
    c = pymongo.MongoClient()
    db = c['chicago']
    db.authenticate(MONGO_USER, password=MONGO_PW)
    coll = db['weather']
    for date in dates:
        url = 'http://api.wunderground.com/api/%s/history_%s/q/IL/Chicago.json' % (WEATHER_KEY, date)
        weat = requests.get(url)
        weather = {
            'CELSIUS_MAX': None,
            'CELSIUS_MIN': None,
            'FAHR_MIN': None, 
            'FAHR_MAX': None,
        }
        if weat.status_code == 200:
            summary = weat.json()['history']['dailysummary'][0]
            weather['CELSIUS_MAX'] = summary['maxtempm']
            weather['CELSIUS_MIN'] = summary['mintempm']
            weather['FAHR_MAX'] = summary['maxtempi']
            weather['FAHR_MIN'] = summary['mintempi']
            weather['DATE'] = datetime.strptime(date, '%Y%m%d')
            update = {'$set': weather}
            up = coll.update({'DATE': datetime.strptime(date, '%Y%m%d')}, update, upsert=True)
        else:
            raise WeatherError('Wunderground API responded with %s: %s' % (weat.status_code, weat.content[300:]))
        time.sleep(7)
    return 'Updated weather for %s' % ', '.join(dates)

def get_most_wanted():
    wanted = requests.get(MOST_WANTED, params={'max': 100})
    if wanted.status_code == 200:
        s3conn = S3Connection(AWS_KEY, AWS_SECRET)
        bucket = s3conn.get_bucket('crime.static-eric.com')
        wanted_list = []
        for person in wanted.json():
            warrant = person['warrantNo']
            wanted_list.append(warrant)
            mugs = requests.get(MUGSHOTS, params={'warrantNo': warrant})
            person['mugs'] = []
            if mugs.status_code == 200:
                for mug in mugs.json():
                    image_path = 'images/wanted/%s_%s.jpg' % (warrant, mug['mugshotNo'])
                    k = Key(bucket)
                    k.key = image_path
                    k.set_contents_from_string(b64decode(mug['image']))
                    k.set_acl('public-read')
                    person['mugs'].append({'angle': mug['mugshotNo'], 'image_path': image_path})
            else:
                raise ClearPathError('ClearPath API returned %s when fetching mugshots for %s: %s' % (mugs.status_code, warrant, mugs.content[300:]))
            k = Key(bucket)
            k.key = 'data/wanted/%s.json' % warrant
            k.set_contents_from_string(json.dumps(person, indent=4))
            k.set_acl('public-read')
        k = Key(bucket)
        k.key = 'data/wanted/wanted_list.json'
        k = k.copy(k.bucket.name, k.name, {'Content-Type':'application/json'})
        k.set_acl('public-read')
    else:
        raise ClearPathError('ClearPath API returned %s when getting most wanted list: %s' % (wanted.status_code, wanted.content[300:]))

if __name__ == '__main__':
    get_crimes()
    get_most_wanted()
