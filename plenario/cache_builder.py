import sys
import requests
import json

def main():
    base_url = 'http://plenar.io'
    date_start = '2014-03-27'
    date_end = '2014-09-23'

    fetch_url('%s/v1/api/timeseries/?obs_date__le=%s&obs_date__ge=%s&agg=week' % (base_url,date_end, date_start))
    datasets = fetch_url('%s/v1/api/datasets' % base_url)

    for d in datasets['objects']:
        fetch_url('%s/v1/api/fields/%s' % (base_url, d['dataset_name']))

    for d in datasets['objects']:
        fetch_url('%s/v1/api/datasets/?dataset_name=%s' % (base_url, d['dataset_name']))

    for d in datasets['objects']:
        fetch_url('%s/v1/api/grid/?obs_date__le=%s&obs_date__ge=%s&dataset_name=%s&resolution=500' % (base_url, date_end, date_start, d['dataset_name']))

    for d in datasets['objects']:
        fetch_url('%s/v1/api/detail-aggregate/?obs_date__le=%s&obs_date__ge=%s&agg=week&dataset_name=%s' % (base_url, date_end, date_start, d['dataset_name']))

def fetch_url(url):
    try:
        r = requests.get(url)
        print url, r.status_code, (r.elapsed.microseconds / 1000), 'ms'
        return r.json()
    except:
        e = sys.exc_info()[0]
        print e
        return None


if __name__ == "__main__":
    main()