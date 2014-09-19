import sys
import requests
import json
from datetime import date, datetime, timedelta
from dateutil import parser

longest_request_ms = 0
longest_request = ''

def set_longest_request_ms(val):
    global longest_request_ms
    longest_request_ms = val

def set_longest_request(val):
    global longest_request
    longest_request = val

def fetch_for_dates(date_start, date_end):
    base_url = 'http://plenar.io'

    print '--- fetching for dates %s - %s ---' % (date_start, date_end)

    yield '%s/v1/api/timeseries/?obs_date__le=%s&obs_date__ge=%s&agg=week' % (base_url,date_end, date_start)
    datasets = fetch_url('%s/v1/api/datasets' % base_url)

    for d in datasets['objects']:
        yield '%s/v1/api/fields/%s' % (base_url, d['dataset_name'])

    for d in datasets['objects']:
        yield '%s/v1/api/datasets/?dataset_name=%s' % (base_url, d['dataset_name'])

    for d in datasets['objects']:
        yield '%s/v1/api/grid/?obs_date__le=%s&obs_date__ge=%s&agg=week&dataset_name=%s&resolution=500' % (base_url, date_end, date_start, d['dataset_name'])

    for d in datasets['objects']:
        yield '%s/v1/api/detail-aggregate/?obs_date__le=%s&obs_date__ge=%s&agg=week&dataset_name=%s' % (base_url, date_end, date_start, d['dataset_name'])

    for d in datasets['objects']:
        yield '%s/v1/api/detail/?obs_date__le=%s&obs_date__ge=%s&agg=week&dataset_name=%s' % (base_url, date_end, date_start, d['dataset_name'])

def fetch_url(url):
    try:
        r = requests.get(url)
        p = 'GET "%s" | %s | %sms' % (url, r.status_code, (r.elapsed.microseconds / 1000))
        if longest_request_ms < r.elapsed.microseconds:
            set_longest_request_ms(r.elapsed.microseconds)
            set_longest_request(p)
        try:
            return r.json()
        except ValueError:
            print 'Junk response %s' % r.content[:100]
            return None
    except requests.exceptions.ConnectTimeout, e:
        print 'Connection timeout %s' % url
        return None
    except requests.exceptions.Timeout, e:
        print 'Request timeout %s' % url
        return None


if __name__ == "__main__":
    from multiprocessing import Pool
    pool = Pool(processes=10)
    cache_end_date = parser.parse(sys.argv[1])
    for d in range (8):
        cache_end_date = cache_end_date - timedelta(days=1)
        cache_start_date = cache_end_date - timedelta(days=90)
        pool.map(fetch_url, list(fetch_for_dates(cache_start_date, cache_end_date)))
    print '\n\nlongest request:'
    print longest_request
