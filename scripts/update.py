import sys
import plenario.tasks as tasks

# Alternative to celerybeat configuration in plenario/celery_settings.py
#
# I ran into bugs with celerybeat in production,
# but in general celry + redis were working fine.
# This script is part of a workaround to use Celery without celerybeat.
#
# Along with some crontab
# and a wrapper shell script to set up a Python environment,
# you can use this script to roll your own periodic tasks.
#
# N.B. plenario_update.sh will expect this file in the project root directory


def often_update():
    # Keep METAR updates out of the queue
    # so that they run right away even when the ETL is chugging through
    # a big backlog of event dataset updates.
    tasks.update_metar()


def daily_update():
    tasks.update_weather.delay()
    tasks.frequency_update.delay('daily')


def weekly_update():
    tasks.frequency_update.delay('weekly')


def monthly_update():
    tasks.frequency_update.delay('monthly')


def yearly_update():
    tasks.frequency_update.delay('yearly')

dispatch = {
    'often': often_update,
    'daily': daily_update,
    'weekly': weekly_update,
    'monthly': monthly_update,
    'yearly': yearly_update
}

frequency = sys.argv[1]
print 'Running task for time frequency', frequency
func = dispatch.get(frequency)
if func:
    func()
