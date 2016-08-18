from datetime import timedelta

__all__ = [
    'truncate',
    'truncate_second',
    'truncate_minute',
    'truncate_hour',
    'truncate_day',
    'truncate_week',
    'truncate_month',
    'truncate_quarter',
    'truncate_half_year',
    'truncate_year',
]

PERIODS = {
    'second': dict(microsecond=0),
    'minute': dict(microsecond=0, second=0),
    'hour': dict(microsecond=0, second=0, minute=0),
    'day': dict(microsecond=0, second=0, minute=0, hour=0,),
    'month': dict(microsecond=0, second=0, minute=0, hour=0, day=1),
    'year': dict(microsecond=0, second=0, minute=0, hour=0, day=1, month=1),
}
ODD_PERIODS = ['week', 'quarter', 'half_year']


def truncate_second(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'second')` '''
    return truncate(datetime, 'second')


def truncate_minute(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'minute')` '''
    return truncate(datetime, 'minute')


def truncate_hour(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'hour')` '''
    return truncate(datetime, 'hour')


def truncate_day(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'day')` '''
    return truncate(datetime, 'day')


def truncate_week(datetime):
    '''
    Truncates a date to the first day of an ISO 8601 week, i.e. monday.

    :params datetime: an initialized datetime object
    :return: `datetime` with the original day set to monday
    :rtype: :py:mod:`datetime` datetime object
    '''
    return datetime - timedelta(days=datetime.isoweekday() - 1)


def truncate_month(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'month')` '''
    return truncate(datetime, 'month')


def truncate_quarter(datetime):
    '''
    Truncates the datetime to the first day of the quarter for this date.

    :params datetime: an initialized datetime object
    :return: `datetime` with the month set to the first month of this quarter
    :rtype: :py:mod:`datetime` datetime object
    '''
    month = datetime.month
    if month >= 1 and month <= 3:
        return datetime.replace(month=1)
    elif month >= 4 and month <= 6:
        return datetime.replace(month=4)
    elif month >= 7 and month <= 9:
        return datetime.replace(month=7)
    elif month >= 10 and month <= 12:
        return datetime.replace(month=10)


def truncate_half_year(datetime):
    '''
    Truncates the datetime to the first day of the half year for this date.

    :params datetime: an initialized datetime object
    :return: `datetime` with the month set to the first month of this half year
    :rtype: :py:mod:`datetime` datetime object
    '''
    month = datetime.month

    if month >= 1 and month <= 6:
        return datetime.replace(month=1)
    elif month >= 7 and month <= 12:
        return datetime.replace(month=7)


def truncate_year(datetime):
    ''' Sugar for :py:func:`truncate(datetime, 'year')` '''
    return truncate(datetime, 'year')


def truncate(datetime, truncate_to='day'):
    '''
    Truncates a datetime to have the values with higher precision than
    the one set as `truncate_to` as zero (or one for day and month).

    Possible values for `truncate_to`:

    * second
    * minute
    * hour
    * day
    * week (iso week i.e. to monday)
    * month
    * quarter
    * half_year
    * year

    Examples::

       >>> truncate(datetime(2012, 12, 12, 12), 'day')
       datetime(2012, 12, 12)
       >>> truncate(datetime(2012, 12, 14, 12, 15), 'quarter')
       datetime(2012, 10, 1)
       >>> truncate(datetime(2012, 3, 1), 'week')
       datetime(2012, 2, 27)

    :params datetime: an initialized datetime object
    :params truncate_to: The highest precision to keep its original data.
    :return: datetime with `truncated_to` as the highest level of precision
    :rtype: :py:mod:`datetime` datetime object
    '''
    if truncate_to in PERIODS:
        return datetime.replace(**PERIODS[truncate_to])
    elif truncate_to in ODD_PERIODS:
        if truncate_to == 'week':
            return truncate(truncate_week(datetime), 'day')
        elif truncate_to == 'quarter':
            return truncate(truncate_quarter(datetime), 'month')
        elif truncate_to == 'half_year':
            return truncate(truncate_half_year(datetime), 'month')
    else:
        raise ValueError('truncate_to not valid. Valid periods: {}'.format(
            ', '.join(PERIODS.keys() + ODD_PERIODS)
        ))
