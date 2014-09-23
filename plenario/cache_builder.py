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

    yield '%s/explore#aggregate/obs_date__le=%s&obs_date__ge=%s&agg=week' % (base_url,date_end, date_start)
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

def fetch_example_queries():
    
    # examples
    fetch_url('http://plenar.io/explore#aggregate/obs_date__ge=2009%2F09%2F23&obs_date__le=2014%2F09%2F23&location_geom__within=%7B%22type%22%3A%22Feature%22%2C%22properties%22%3A%7B%7D%2C%22geometry%22%3A%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B-122.401578%2C37.789506%5D%2C%5B-122.408574%2C37.784113%5D%2C%5B-122.401857%2C37.77889%5D%2C%5B-122.394948%2C37.784351%5D%2C%5B-122.401578%2C37.789506%5D%5D%5D%7D%7D&agg=week')
    fetch_url('http://plenar.io/v1/api/timeseries/?obs_date__ge=2009-09-23&obs_date__le=2014-09-23&location_geom__within={%22type%22:%22Feature%22,%22properties%22:{},%22geometry%22:{%22type%22:%22Polygon%22,%22coordinates%22:[[[-122.401578,37.789505],[-122.408573,37.784113],[-122.401857,37.77889],[-122.394948,37.784351],[-122.401578,37.789506]]]}}&agg=week')

    fetch_url('http://plenar.io/explore#aggregate/dataset_name__in=311_unified_data,case_data_from_san_francisco_311_sf311,311_service_requests_tree_debris,311_service_requests_tree_trims,311_service_requests_vacant_and_abandoned_building,311_service_requests_street_lights_one_out,311_service_requests_street_lights_all_out,311_service_requests_sanitation_code_complaints,311_service_requests_rodent_baiting,311_service_requests_pot_holes_reported,311_service_requests_graffiti_removal,311_service_requests_garbage_carts,311_service_requests_from_2010_to_present,311_service_requests_abandoned_vehicles,311_service_requests_alley_lights_out,&obs_date__ge=2014-01-01&agg=week')
    fetch_url('http://plenar.io/v1/api/timeseries/?obs_date__ge=2014-01-01&dataset_name__ilike=%25311%25&agg=week')

    fetch_url('http://plenar.io/explore#detail/dataset_name=nypd_motor_vehicle_collisions&obs_date__le=2014%2F09%2F17&obs_date__ge=2013%2F09%2F17&agg=week&resolution=500&borough=MANHATTAN&number_of_persons_killed__gt=0')
    fetch_url('http://plenar.io/v1/api/detail/?dataset_name=nypd_motor_vehicle_collisions&obs_date__ge=2014-03-23&obs_date__le=2014-09-23&borough=MANHATTAN&number_of_persons_killed__gt=0&weather=true')

    fetch_url('http://plenar.io/explore#detail/dataset_name=311_service_requests_pot_holes_reported&obs_date__ge=2014%2F03%2F23&obs_date__le=2014%2F09%2F23&location_geom__within=%7B%22type%22%3A%22Feature%22%2C%22properties%22%3A%7B%7D%2C%22geometry%22%3A%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B-87.62796834111214%2C41.88707119719461%5D%2C%5B-87.62758210301399%2C41.874546019423576%5D%2C%5B-87.63573601841925%2C41.87448210916051%5D%2C%5B-87.63732388615608%2C41.87668697630679%5D%2C%5B-87.63835385441779%2C41.880649155073556%5D%2C%5B-87.63762429356575%2C41.88582517979217%5D%2C%5B-87.63565018773079%2C41.88710314603922%5D%2C%5B-87.62796834111214%2C41.88707119719461%5D%5D%5D%7D%7D&agg=week&resolution=300')
    fetch_url('http://plenar.io/v1/api/detail/?dataset_name=311_service_requests_pot_holes_reported&obs_date__ge=2014-03-23&census_block__ilike=17031839100%25&data_type=csv')

    fetch_url('http://plenar.io/explore#detail/dataset_name=mayors_24_hour_hotline_nemo_snow_related_calls&obs_date__le=2013%2F02%2F11&obs_date__ge=2013%2F02%2F08&agg=day&resolution=200')
    fetch_url('http://plenar.io/v1/api/detail/?dataset_name=mayors_24_hour_hotline_nemo_snow_related_calls&obs_date__ge=2013-02-08&obs_date__le=2013-02-11&agg=day&weather=true')

    fetch_url('http://plenar.io/explore#detail/dataset_name=crimes_2001_to_present&obs_date__ge=2001%2F01%2F01&agg=month&resolution=1000&arrest=yes&iucr=0110')
    fetch_url('http://plenar.io/v1/api/detail-aggregate/?dataset_name=crimes_2001_to_present&obs_date__ge=2001-01-01&iucr=0110&arrest=yes&agg=month')

    # home page
    fetch_url('http://plenar.io/v1/api/detail/?dataset_name=nypd_motor_vehicle_collisions&obs_date__ge=2014-03-23&obs_date__le=2014-09-23&borough=MANHATTAN&number_of_persons_killed__gt=0&weather=true')
    fetch_url('http://plenar.io/explore#aggregate/obs_date__le=2013%2F12%2F31&obs_date__ge=2013%2F01%2F01&location_geom__within=%7B%22type%22%3A%22Feature%22%2C%22properties%22%3A%7B%7D%2C%22geometry%22%3A%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B-87.637939453125%2C41.88694340165636%5D%2C%5B-87.62746810913086%2C41.8875823767912%5D%2C%5B-87.62454986572266%2C41.88898809959183%5D%2C%5B-87.60944366455078%2C41.88834913851702%5D%2C%5B-87.6101303100586%2C41.86700416724044%5D%2C%5B-87.63450622558592%2C41.867387672721804%5D%2C%5B-87.63725280761719%2C41.876974562065904%5D%2C%5B-87.637939453125%2C41.88694340165636%5D%5D%5D%7D%7D&agg=week')

    # cool datasets
    fetch_url('http://plenar.io/explore#detail/dataset_name=sfpd_incident_all_datetime_csv&obs_date__le=2014%2F09%2F19&obs_date__ge=2014%2F03%2F23&agg=week&resolution=300')
    fetch_url('http://plenar.io/explore#detail/dataset_name=cta_ridership_avg_weekday_bus_stop_boardings_in_oc&obs_date__le=2012%2F10%2F01&obs_date__ge=2012%2F10%2F01&agg=week&resolution=500')
    fetch_url('http://plenar.io/explore#detail/dataset_name=cook_county_recorder_of_deeds_foreclosures_2011_co&obs_date__le=2011%2F12%2F31&obs_date__ge=2011%2F01%2F01&agg=week&resolution=500')
    fetch_url('http://plenar.io/explore#detail/obs_date__le=2014%2F09%2F19&obs_date__ge=2014%2F03%2F23&agg=week&dataset_name=311_service_requests_graffiti_removal&resolution=500')
    fetch_url('http://plenar.io/explore#detail/dataset_name=chicago_redlight_tickets_csv&obs_date__le=2014%2F09%2F19&obs_date__ge=2007%2F01%2F01&agg=week&resolution=500')
    fetch_url('http://plenar.io/explore#detail/dataset_name=mayors_24_hour_hotline_nemo_snow_related_calls&obs_date__le=2013%2F02%2F12&obs_date__ge=2013%2F02%2F07&agg=day&resolution=500')
    fetch_url('http://plenar.io/explore#detail/obs_date__le=2014%2F09%2F19&obs_date__ge=2014%2F03%2F23&agg=week&dataset_name=311_unified_data&resolution=500')
    fetch_url('http://plenar.io/explore#detail/obs_date__le=2014%2F09%2F19&obs_date__ge=2014%2F03%2F23&agg=week&dataset_name=311_service_requests_from_2010_to_present&resolution=500')

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
            # print 'Junk response'
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
    pool = Pool(processes=4)
    cache_end_date = parser.parse(sys.argv[1])
    all_resp = []
    fetch_example_queries()
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
