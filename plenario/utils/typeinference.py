#!/usr/bin/env python

import datetime

from dateutil.parser import parse
from sqlalchemy import Boolean, Integer, BigInteger, Float, Date, \
    String
from sqlalchemy.dialects.postgresql import TIMESTAMP, TIME

NoneType = type(None)

NULL_VALUES = ('na', 'n/a', 'none', 'null', '.', '', ' ')
TRUE_VALUES = ('yes', 'y', 'true', 't',)
FALSE_VALUES = ('no', 'n', 'false', 'f',)

DEFAULT_DATETIME = datetime.datetime(2999, 12, 31, 0, 0, 0)
NULL_DATE = datetime.date(2999, 12, 31)
NULL_TIME = datetime.time(0, 0, 0)


def normalize_column_type(l):
    """
    Docs to come...
    """
    
    null_values = False

    # Convert "NA", "N/A", etc. to null types.
    for i, x in enumerate(l):
        if x is not None and x.lower() in NULL_VALUES:
            l[i] = None
            null_values = True

    # Are they boolean?
    try:
        for i, x in enumerate(l):
            if x == '' or x is None:
                raise ValueError('Not boolean')
            elif x.lower() in TRUE_VALUES:
                continue
            elif x.lower() in FALSE_VALUES:
                continue
            else:
                raise ValueError('Not boolean')

        return Boolean, null_values
    except ValueError:
        pass

    # Are they integers?
    try:
        normal_types_set = set()
        add = normal_types_set.add
        for i, x in enumerate(l):
            if x == '' or x is None:
                continue
            
            int_x = int(x.replace(',', ''))

            if x[0] == '0' and int(x) != 0:
                raise TypeError('Integer is padded with 0s, so treat it as a string instead.')
            if x.isspace():
                raise TypeError('Integer is nothing but spaces so falling back to string')

            if 9000000000000000000 > int_x > 1000000000:
                add(BigInteger)
            elif 1000000000 > int_x:
                add(Integer)
            else:
                raise ValueError

        if BigInteger in normal_types_set:
            return BigInteger, null_values
        else:
            return Integer, null_values

    except TypeError:
        pass
    except ValueError:
        pass

    # Are they floats?
    try:

        for i, x in enumerate(l):
            if x == '' or x is None:
                continue

            float_x  = float(x.replace(',', ''))

        return Float, null_values
    except ValueError:
        pass

    # Are they datetimes?
    try:
        normal_types_set = set()
        add = normal_types_set.add
        ampm = False
        for i, x in enumerate(l):
            if x == '' or x is None:
                add(NoneType)
                continue
 
            d = parse(x, default=DEFAULT_DATETIME)
 
            # Is it only a time?
            if d.date() == NULL_DATE:
                add(TIME)

            # Is it only a date?
            elif d.time() == NULL_TIME:
                add(Date)
 
            # It must be a date and time
            else:
                add(TIMESTAMP)
            
            if 'am' in x.lower():
                ampm = True
            
            if 'pm' in x.lower():
                ampm = True

            
        normal_types_set.discard(NoneType)
 
        # If a mix of dates and datetimes, up-convert dates to datetimes
        if normal_types_set == set([TIMESTAMP, Date]):
            normal_types_set = set([TIMESTAMP])
        # Datetimes and times don't mix -- fallback to using strings
        elif normal_types_set == set([TIMESTAMP, TIME]):
            normal_types_set = set([String])
        # Dates and times don't mix -- fallback to using strings
        elif normal_types_set == set([Date, TIME]):
            normal_types_set = set([String])
        elif normal_types_set == set([TIME]) and ampm:
            normal_types_set = set([String])
 
        return normal_types_set.pop(), null_values
    except ValueError:
        pass
    except TypeError: #https://bugs.launchpad.net/dateutil/+bug/1247643
        pass

    # Don't know what they are, so they must just be strings 
    return String, null_values
