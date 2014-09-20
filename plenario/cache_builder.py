import sys
import time
import requests
import json
from datetime import date, datetime, timedelta
from dateutil import parser

longest_request_ms = 0
longest_request = ''

def fetch_for_dates(date_start, date_end):
    base_url = 'http://plenar.io'

    print '--- fetching for dates %s - %s ---' % (date_start, date_end)

    yield '%s/v1/api/timeseries/?obs_date__le=%s&obs_date__ge=%s&agg=week' % (base_url,date_end, date_start)
    datasets = requests.get('%s/v1/api/datasets' % base_url)
    datasets = datasets.json()

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
        start = time.time()
        r = requests.get(url)
        end = time.time()
        diff = end - start
        try:
            resp = r.json()
            return diff, url
        except ValueError:
            print 'Junk response %s' % r.content[:100]
            print p
            return None, url
    except requests.exceptions.ConnectTimeout, e:
        print 'Connection timeout %s' % url
        return None, url
    except requests.exceptions.Timeout, e:
        print 'Request timeout %s' % url
        return None, url
    except requests.exceptions.ConnectionError, e:
        print 'Connection reset: %s' % url
        return None, url


if __name__ == "__main__":
    from multiprocessing import Pool
    from itertools import groupby
    from operator import itemgetter
    pool = Pool(processes=20)
    cache_end_date = parser.parse(sys.argv[1])
    all_resp = []
    for d in range(8):
        cache_end_date = cache_end_date - timedelta(days=1)
        cache_start_date = cache_end_date - timedelta(days=90)
        resps = pool.map(fetch_url, list(fetch_for_dates(cache_start_date, cache_end_date)))
        all_resp.extend(resps)
    all_resp = sorted(all_resp, key=itemgetter(1))
    all_times = {}
    best = 180
    worst = 0
    fastest_url = ''
    slowest_url = ''
    for k,g in groupby(all_resp, key=itemgetter(1)):
        good_times = [i[0] for i in list(g) if i]
        bad_times = len([i[0] for i in list(g) if i is None])
        best_time, worst_time = good_times[0], good_times[-1]
        if best_time < best:
            best = best_time
            fastest_url = k
        if worst_time > worst:
            worst = worst_time
            slowest_url = k
    print 'Fastest: %s (%s)' % (fastest_url, best)
    print 'Slowest: %s (%s)' % (slowest_url, worst)
