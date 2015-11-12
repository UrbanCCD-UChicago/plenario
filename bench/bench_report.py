from collections import OrderedDict
import subprocess
import csv
import sys
import os
import time

NUM_CLIENTS = '4'
NUM_THREADS = '2'
NUM_TRANSACTIONS_PER_CLIENT = '3'

_pwd = os.path.dirname(os.path.realpath(__file__))
SQL_PATH = os.path.join(_pwd, 'plenarioBench.sql')

try:
    HOSTNAME = sys.argv[1]
    DB_NAME = sys.argv[2]
    REPORT_NAME = sys.argv[3]
except IndexError:
    print "Expected usage: python bench_report.py hostname db_name output_destination"
    sys.exit(1)


# pgbench documentation: http://www.postgresql.org/docs/9.4/static/pgbench.html
def main():
    args = ['pgbench',
            '-h', HOSTNAME,
            '-U', 'postgres',
            '-f', SQL_PATH,
            '-c', NUM_CLIENTS,
            '-j', NUM_THREADS,
            '-t', NUM_TRANSACTIONS_PER_CLIENT,
            '-n',  # Don't try to vacuum the default tables that we don't have.
            '-r',  # Print to stdout the average latency per query.
            DB_NAME]

    start_time = time.time()
    pgbench_output = subprocess.check_output(args)
    end_time = time.time()
    elapsed_time = end_time - start_time

    query_labels = ['timeseries_chicago_broad',
                    'timeseries_everywhere',
                    'detail_homicides',
                    'timeseries_chicago_narrow',
                    'timeseries_san_francisco',
                    'timeseries_311']

    latency = OrderedDict(zip(query_labels, get_latency_in_seconds(pgbench_output)))
    make_report(latency, elapsed_time)


def get_latency_in_seconds(latency_output):
    """
    Return list of floats representing avg latency for each query
    """

    # Find the lines between BEGIN; and END;
    # with the average latency times.
    latency_lines = latency_output.splitlines()
    for i, line in enumerate(latency_lines):
        if line.endswith("BEGIN;"):
            partition_point = i
            break
    avg_latency_lines = latency_lines[partition_point + 1: -1]

    # The lines are formatted like \t[avg time in ms]\t [query text]
    # Just grab all the latency times
    avg_latency_ms = [line.split('\t')[1] for line in avg_latency_lines]
    # and convert them to seconds.
    avg_latency_s = [float(ms)/1000 for ms in avg_latency_ms]
    return avg_latency_s


def make_report(latency, elapsed):
        latency['total_latency'] = sum(latency.values())
        latency['elapsed_time'] = elapsed
        with open(REPORT_NAME, 'w') as out:
            writer = csv.writer(out)
            writer.writerow(['query_name','average_latency_in_seconds'])
            for name, lat_secs in latency.items():
                writer.writerow([name, lat_secs])

if __name__ == "__main__":
    main()
