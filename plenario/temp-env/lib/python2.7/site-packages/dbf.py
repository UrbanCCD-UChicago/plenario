"""
=========
Copyright
=========
    - Copyright: 2008-2012 Ad-Mail, Inc -- All rights reserved.
    - Author: Ethan Furman
    - Contact: ethanf@admailinc.com
    - Organization: Ad-Mail, Inc.
    - Version: 0.93.020 as of 21 May 2012

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name of Ad-Mail, Inc nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY Ad-Mail, Inc ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Ad-Mail, Inc BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

version = (0, 94, 3)

__all__ = (
        'Table', 'Record', 'List', 'Index', 'Iter', 'Date', 'DateTime', 'Time',
        'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
        'recno', 'source_table', 'reset', 'scatter', 'undelete',
        'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
        'InvalidFieldSpecError', 'NonUnicodeError', 'NotFoundError',
        'DbfWarning', 'Eof', 'Bof', 'DoNotIndex',
        'Null', 'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum',
        'NullDate', 'NullDateTime', 'NullTime', 'Vapor',
        'Process',
        'Truth', 'Falsth', 'Unknown', 'NoneType', 'Decimal',
        'guess_table_type', 'table_type',
        'add_fields', 'delete_fields', 'get_fields', 'rename_field',
        'export', 'first_record', 'from_csv', 'info', 'structure',
        )


import codecs
import collections
import csv
import datetime
import locale
import os
import struct
import sys
import time
import weakref

from array import array
from bisect import bisect_left, bisect_right
import decimal
from decimal import Decimal
from math import floor
import types
from types import NoneType

py_ver = sys.version_info[:2]

LOGICAL_BAD_IS_FALSE = True # if bad data in logical fields, return False? (else raise error)

input_decoding = 'ascii'        # treat non-unicode data as ...
default_codepage = 'ascii'      # if no codepage specified on dbf creation, use this

temp_dir = os.environ.get("DBF_TEMP") or os.environ.get("TMP") or os.environ.get("TEMP") or ""

default_type = 'db3'        # default format if none specified
pql_user_functions = {}     # user-defined pql functions  (pql == primitive query language)
                            # it is not real sql and won't be for a long time (if ever)

_Template_Records = dict()  # signature:_meta of template records

# 2.7 constructs
if sys.version_info[:2] < (2, 7):
    # Decimal does not accept float inputs until 2.7
    def Decimal(val=0):
        if isinstance(val, float):
            return decimal.Decimal(str(val))
        return decimal.Decimal(val)

    bytes = str

# 2.6 constructs
if sys.version_info[:2] < (2, 6):
    # define next()
    def next(iterator):
        return iterator.next()

    # 2.6+ property for 2.5-,
    # define our own property type
    class property(object):
        "2.6 properties for 2.5-"    
        def __init__(self, fget=None, fset=None, fdel=None, doc=None):
            self.fget = fget
            self.fset = fset
            self.fdel = fdel
            self.__doc__ = doc or fget.__doc__
        def __call__(self, func):
            self.fget = func
            if not self.__doc__:
                self.__doc__ = fget.__doc__
        def __get__(self, obj, objtype=None):
            if obj is None:
                return self         
            if self.fget is None:
                raise AttributeError("unreadable attribute")
            return self.fget(obj)
        def __set__(self, obj, value):
            if self.fset is None:
                raise AttributeError("can't set attribute")
            self.fset(obj, value)
        def __delete__(self, obj):
            if self.fdel is None:
                raise AttributeError("can't delete attribute")
            self.fdel(obj)
        def setter(self, func):
            self.fset = func
            return self
        def deleter(self, func):
            self.fdel = func
            return self

# 2.5 constructs

try:
    all
except NameError:
    def all(iterable):
        for element in iterable:
            if not element:
                return False
        return True

    def any(iterable):
        for element in iterable:
            if element:
                return True
        return False
    SEEK_SET, SEEK_CUR, SEEK_END = range(3)
else:
    SEEK_SET, SEEK_CUR, SEEK_END = os.SEEK_SET, os.SEEK_CUR, os.SEEK_END


try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        def __init__(self, default_factory=None, *a, **kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, *a, **kw)
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value
        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.iteritems()
        def copy(self):
            return self.__copy__()
        def __copy__(self):
            return type(self)(self.default_factory, self)
        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))
        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))
# other constructs
class MutableDefault(object):
    """Lives in the class, and on first access calls the supplied factory and
    maps the result into the instance it was called on"""
    def __init__(self, func):
        self._name = func.__name__
        self.func = func
    def __call__(self):
        return self
    def __get__(self, instance, owner):
        result = self.func()
        if instance is not None:
            setattr(instance, self._name, result)
        return result
    def __repr__(self):
        result = self.func()
        return "MutableDefault(%r)" % (result, )


def none(*args, **kwargs):
    return None
# Constants
SYSTEM = 0x01
NULLABLE = 0x02
BINARY = 0x04
 #AUTOINC = 0x0c         # not currently supported (not vfp 6)

TYPE = 0
START = 1
LENGTH = 2
END = 3
DECIMALS = 4
FLAGS = 5
CLASS = 6
EMPTY = 7
NULL = 8

FIELD_FLAGS = {
        'null':NULLABLE,
        'binary':BINARY,
        'nocptrans':BINARY,
        #'autoinc':AUTOINC,

        NULLABLE:'null',
        BINARY:'binary',
        SYSTEM:'system',
        #AUTOINC:'autoinc',
        }

IN_MEMORY = 0
ON_DISK = 1

CLOSED = 'closed'
READ_ONLY = 'read-only'
READ_WRITE = 'read-write'

# warnings and errors
class DbfError(Exception):
    "Fatal errors elicit this response."
class DataOverflowError(DbfError):
    "Data too large for field"
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data
class BadDataError(DbfError):
    "bad data in table"
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data
class FieldMissingError(KeyError, DbfError):
    "Field does not exist in table"
    def __init__(self, fieldname):
        KeyError.__init__(self, '%s:  no such field in table' % fieldname)
        DbfError.__init__(self, '%s:  no such field in table' % fieldname)
        self.data = fieldname
class InvalidFieldSpecError(DbfError, ValueError):
    "invalid field specification"
    def __init__(self, message):
        ValueError.__init__(self, message)
        DbfError.__init__(self, message)
class NonUnicodeError(DbfError):
    "Data for table not in unicode"
    def __init__(self, message=None):
        DbfError.__init__(self, message)
class NotFoundError(DbfError, ValueError):
    "record criteria not met"
    def __init__(self, message=None, data=None):
        ValueError.__init__(self, message)
        DbfError.__init__(self, message)
        self.data = data
class DbfWarning(Exception):
    "Normal operations elicit this response"
class Eof(DbfWarning, StopIteration):
    "End of file reached"
    message = 'End of file reached'
    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)
class Bof(DbfWarning, StopIteration):
    "Beginning of file reached"
    message = 'Beginning of file reached'
    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)
class DoNotIndex(DbfWarning):
    "Returned by indexing functions to suppress a record from becoming part of the index"
    message = 'Not indexing record'
    def __init__(self):
        DbfWarning.__init__(self, self.message)
# wrappers around datetime and logical objects to allow null values
Unknown = Other = object() # gets replaced later by their final values
class NullType(object):
    "Null object -- any interaction returns Null"
    def _null(self, *args, **kwargs):
        return self
    __eq__ = __ne__ = __ge__ = __gt__ = __le__ = __lt__ = _null
    __add__ = __iadd__ = __radd__ = _null
    __sub__ = __isub__ = __rsub__ = _null
    __mul__ = __imul__ = __rmul__ = _null
    __div__ = __idiv__ = __rdiv__ = _null
    __mod__ = __imod__ = __rmod__ = _null
    __pow__ = __ipow__ = __rpow__ = _null
    __and__ = __iand__ = __rand__ = _null
    __xor__ = __ixor__ = __rxor__ = _null
    __or__ = __ior__ = __ror__ = _null
    __truediv__ = __itruediv__ = __rtruediv__ = _null
    __floordiv__ = __ifloordiv__ = __rfloordiv__ = _null
    __lshift__ = __ilshift__ = __rlshift__ = _null
    __rshift__ = __irshift__ = __rrshift__ = _null
    __neg__ = __pos__ = __abs__ = __invert__ = _null
    __call__ = __getattr__ = _null

    def __divmod__(self, other):
        return self, self
    __rdivmod__ = __divmod__

    if sys.version_info[:2] >= (2, 6):
        __hash__ = None
    else:
        def __hash__(self):
            raise TypeError("unhashable type: 'Null'")

    def __new__(cls, *args):
        if args:
            print args
        return cls.null
    def __nonzero__(self):
        return False
    def __repr__(self):
        return '<null>'
    def __setattr__(self, name, value):
        return None
    def __setitem___(self, index, value):
        return None
    def __str__(self):
        return ''
NullType.null = object.__new__(NullType)
Null = NullType()

class Vapor(object):
    "used in Vapor Records -- compares unequal with everything"
    def __eq__(self, other):
        return False
    def __ne__(self, other):
        return True
Vapor = Vapor()

class Char(unicode):
    "strips trailing whitespace, and ignores trailing whitespace for comparisons"
    def __new__(cls, text=''):
        if not isinstance(text, (str, unicode, cls)):
            raise ValueError("Unable to automatically coerce %r to Char" % text)
        result = unicode.__new__(cls, text.rstrip())
        result.field_size = len(text)
        return result
    __hash__ = unicode.__hash__
    def __eq__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) == other.rstrip()
    def __ge__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) >= other.rstrip()
    def __gt__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) > other.rstrip()
    def __le__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) <= other.rstrip()
    def __lt__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) < other.rstrip()
    def __ne__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str, unicode)):
            return NotImplemented
        return unicode(self) != other.rstrip()
    def __nonzero__(self):
        "ignores trailing whitespace"
        return bool(unicode(self))
    def __add__(self, other):
        result = self.__class__(unicode(self) + other)
        result.field_size = self.field_size
        return result

class Date(object):
    "adds null capable datetime.date constructs"
    __slots__ = ['_date']
    def __new__(cls, year=None, month=0, day=0):
        """date should be either a datetime.date or date/month/day should all be appropriate integers"""
        if year is None or year is Null:
            return cls._null_date
        nd = object.__new__(cls)
        if isinstance(year, (datetime.date)):
            nd._date = year
        elif isinstance(year, (Date)):
            nd._date = year._date
        else:
            nd._date = datetime.date(year, month, day)
        return nd
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return Date(self._date + other)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._date == other._date
        if isinstance(other, datetime.date):
            return self._date == other
        if isinstance(other, type(None)):
            return self._date is None
        return NotImplemented
    def __getattr__(self, name):
        return self._date.__getattribute__(name)
    def __ge__(self, other):
        if isinstance(other, (datetime.date)):
            return self._date >= other
        elif isinstance(other, (Date)):
            if other:
                return self._date >= other._date
            return False
        return NotImplemented
    def __gt__(self, other):
        if isinstance(other, (datetime.date)):
            return self._date > other
        elif isinstance(other, (Date)):
            if other:
                return self._date > other._date
            return True
        return NotImplemented
    def __hash__(self):
        return hash(self._date)
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date <= other
            elif isinstance(other, (Date)):
                if other:
                    return self._date <= other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date < other
            elif isinstance(other, (Date)):
                if other:
                    return self._date < other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date != other
            elif isinstance(other, (Date)):
                if other:
                    return self._date != other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._date)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return other - self._date
        elif self and isinstance(other, (Date)):
            return other._date - self._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(other - self._date)
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "Date(%d, %d, %d)" % self.timetuple()[:3]
        else:
            return "Date()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return self._date - other
        elif self and isinstance(other, (Date)):
            return self._date - other._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(self._date - other)
        else:
            return NotImplemented
    def date(self):
        if self:
            return self._date
        return None
    @classmethod
    def fromordinal(cls, number):
        if number:
            return cls(datetime.date.fromordinal(number))
        return cls()
    @classmethod
    def fromtimestamp(cls, timestamp):
        return cls(datetime.date.fromtimestamp(timestamp))
    @classmethod
    def fromymd(cls, yyyymmdd):
        if yyyymmdd in ('', '        ','no date'):
            return cls()
        return cls(datetime.date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])))
    def strftime(self, format):
        if self:
            return self._date.strftime(format)
        return ''
    @classmethod
    def today(cls):
        return cls(datetime.date.today())
    def ymd(self):
        if self:
            return "%04d%02d%02d" % self.timetuple()[:3]
        else:
            return '        '
Date.max = Date(datetime.date.max)
Date.min = Date(datetime.date.min)
Date._null_date = object.__new__(Date)
Date._null_date._date = None
NullDate = Date()

class DateTime(object):
    "adds null capable datetime.datetime constructs"
    __slots__ = ['_datetime']
    def __new__(cls, year=None, month=0, day=0, hour=0, minute=0, second=0, microsec=0):
        """year may be a datetime.datetime"""
        if year is None or year is Null:
            return cls._null_datetime
        ndt = object.__new__(cls)
        if isinstance(year, (datetime.datetime)):
            ndt._datetime = year
        elif isinstance(year, (DateTime)):
            ndt._datetime = year._datetime
        elif year is not None:
            ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsec)
        return ndt
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime + other)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._datetime == other._datetime
        if isinstance(other, datetime.date):
            return self._datetime == other
        if isinstance(other, type(None)):
            return self._datetime is None
        return NotImplemented
    def __getattr__(self, name):
        if self:
            attribute = self._datetime.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null DateTime object has no attribute %s' % name)
    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime >= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime >= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime > other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(self):
        return self._datetime.__hash__()
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime <= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime <= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime < other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime < other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime != other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime != other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._datetime)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return other - self._datetime
        elif self and isinstance(other, (DateTime)):
            return other._datetime - self._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(other - self._datetime)
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "DateTime(%d, %d, %d, %d, %d, %d, %d)" % (
                self._datetime.timetuple()[:6] + (self._datetime.microsecond, )
                )
        else:
            return "DateTime()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return self._datetime - other
        elif self and isinstance(other, (DateTime)):
            return self._datetime - other._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime - other)
        else:
            return NotImplemented
    @classmethod
    def combine(cls, date, time):
        if Date(date) and Time(time):
            return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
        return cls()
    def date(self):
        if self:
            return Date(self.year, self.month, self.day)
        return Date()
    def datetime(self):
        if self:
            return self._datetime
        return None
    @classmethod    
    def fromordinal(cls, number):
        if number:
            return cls(datetime.datetime.fromordinal(number))
        else:
            return cls()
    @classmethod
    def fromtimestamp(cls, timestamp):
        return DateTime(datetime.datetime.fromtimestamp(timestamp))
    @classmethod
    def now(cls):
        return cls(datetime.datetime.now())
    def time(self):
        if self:
            return Time(self.hour, self.minute, self.second, self.microsecond)
        return Time()
    @classmethod
    def utcnow(cls):
        return cls(datetime.datetime.utcnow())
    @classmethod
    def today(cls):
        return cls(datetime.datetime.today())
DateTime.max = DateTime(datetime.datetime.max)
DateTime.min = DateTime(datetime.datetime.min)
DateTime._null_datetime = object.__new__(DateTime)
DateTime._null_datetime._datetime = None
NullDateTime = DateTime()

class Time(object):
    "adds null capable datetime.time constructs"
    __slots__ = ['_time']
    def __new__(cls, hour=None, minute=0, second=0, microsec=0):
        """hour may be a datetime.time"""
        if hour is None or hour is Null:
            return cls._null_time
        nt = object.__new__(cls)
        if isinstance(hour, (datetime.time)):
            nt._time = hour
        elif isinstance(hour, (Time)):
            nt._time = hour._time
        elif hour is not None:
            nt._time = datetime.time(hour, minute, second, microsec)
        return nt
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t += other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._time == other._time
        if isinstance(other, datetime.time):
            return self._time == other
        if isinstance(other, type(None)):
            return self._time is None
        return NotImplemented
    def __getattr__(self, name):
        if self:
            attribute = self._time.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null Time object has no attribute %s' % name)
    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time >= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time >= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._time > other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(self):
        return self._datetime.__hash__()
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time <= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time <= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time < other
            elif isinstance(other, (Time)):
                if other:
                    return self._time < other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time != other
            elif isinstance(other, (Time)):
                if other:
                    return self._time != other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._time)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            other = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            other -= t
            return other
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "Time(%d, %d, %d, %d)" % (self.hour, self.minute, self.second, self.microsecond)
        else:
            return "Time()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            o = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        elif self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented
    @staticmethod
    def now():
        return DateTime.now().time()
Time.max = Time(datetime.time.max)
Time.min = Time(datetime.time.min)
Time._null_time = object.__new__(Time)
Time._null_time._time = None
NullTime = Time()

class Logical(object):
    """return type for Logical fields; 
    can take the values of True, False, or None/Null"""
    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, (str, unicode)):
            if value.lower() in ('t','true','y','yes','on'):
                return cls.true
            elif value.lower() in ('f','false','n','no','off'):
                return cls.false
            elif value.lower() in ('?','unknown','null','none',' ',''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Logical: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]
    def __add__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) + i

    __radd__ = __iadd__ = __add__
    def __sub__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) - i
    __isub__ = __sub__
    def __rsub__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i - int(y)
    def __mul__(x, y):
        if x == 0 or y == 0:
            return 0
        elif isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) * i
    __rmul__ = __imul__ = __mul__
    def __div__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__div__(i)
    __idiv__ = __div__
    def __rdiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__div__(int(y))
    def __truediv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__truediv__(i)
    __itruediv__ = __truediv__
    def __rtruediv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__truediv__(int(y))
    def __floordiv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__floordiv__(i)
    __ifloordiv__ = __floordiv__
    def __rfloordiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__floordiv__(int(y))
    def __divmod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return divmod(int(x), i)
    def __rdivmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return divmod(i, int(y))
    def __mod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) % i
    __imod__ = __mod__
    def __rmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i % int(y)
    def __pow__(x, y):
        if not isinstance(y, (x.__class__, bool, type(None), int)):
            return NotImplemented
        if isinstance(y, type(None)) or y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown:
            return Unknown
        return int(x) ** i
    __ipow__ = __pow__
    def __rpow__(y, x):
        if not isinstance(x, (y.__class__, bool, type(None), int)):
            return NotImplemented
        if y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown or isinstance(x, type(None)):
            return Unknown
        return int(x) ** i
    def __lshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) << int(y)
    __ilshift__ = __lshift__
    def __rlshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) << int(y)
    def __rshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) >> int(y)
    __irshift__ = __rshift__
    def __rrshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) >> int(y)
    def __neg__(x):
        "NEG (negation)"
        if x in (Truth, Falsth):
            return -x.value
        return Unknown
    def __pos__(x):
        "POS (posation)"
        if x in (Truth, Falsth):
            return +x.value
        return Unknown
    def __abs__(x):
        if x in (Truth, Falsth):
            return abs(x.value)
        return Unknown
    def __invert__(x):
        if x in (Truth, Falsth):
            return ~x.value
        return Unknown
    def __complex__(x):
        if x.value is None:
            raise ValueError("unable to return complex() of %r" % x)
        return complex(x.value)
    def __int__(x):
        if x.value is None:
            raise ValueError("unable to return int() of %r" % x)
        return int(x.value)
    def __long__(x):
        if x.value is None:
            raise ValueError("unable to return long() of %r" % x)
        return long(x.value)
    def __float__(x):
        if x.value is None:
            raise ValueError("unable to return float() of %r" % x)
        return float(x.value)
    def __oct__(x):
        if x.value is None:
            raise ValueError("unable to return oct() of %r" % x)
        return oct(x.value)
    def __hex__(x):
        if x.value is None:
            raise ValueError("unable to return hex() of %r" % x)
        return hex(x.value)
        
    def __and__(x, y):
        """AND (conjunction) x & y:
        True iff both x, y are True
        False iff at least one of x, y is False
        Unknown otherwise"""
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x == 0 or y == 0:
                return 0
            elif x is Unknown or y is Unknown:
                return Unknown
            return int(x) & int(y)
        elif x in (False, Falsth) or y in (False, Falsth):
            return Falsth
        elif x in (True, Truth) and y in (True, Truth):
            return Truth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __rand__ = __and__
    def __or__(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) | int(y)
        elif x in (True, Truth) or y in (True, Truth):
            return Truth
        elif x in (False, Falsth) and y in (False, Falsth):
            return Falsth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __ror__ = __or__
    def __xor__(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) ^ int(y)
        elif x in (True, Truth, False, Falsth) and y in (True, Truth, False, Falsth):
            return {
                    (True, True)  : Falsth,
                    (True, False) : Truth,
                    (False, True) : Truth,
                    (False, False): Falsth,
                   }[(x, y)]
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __rxor__ = __xor__
    def __nonzero__(x):
        if x is Unknown:
            raise TypeError('True/False value of %r is unknown' % x)
        return x.value is True
    def __eq__(x, y):
        if isinstance(y, x.__class__):
            return x.value == y.value
        elif isinstance(y, (bool, type(None), int)):
            return x.value == y
        return NotImplemented
    def __ge__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value >= y.value
        elif isinstance(y, (bool, int)):
            return x.value >= y
        return NotImplemented
    def __gt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value > y.value
        elif isinstance(y, (bool, int)):
            return x.value > y
        return NotImplemented
    def __le__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value <= y.value
        elif isinstance(y, (bool, int)):
            return x.value <= y
        return NotImplemented
    def __lt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value < y.value
        elif isinstance(y, (bool, int)):
            return x.value < y
        return NotImplemented
    def __ne__(x, y):
        if isinstance(y, x.__class__):
            return x.value != y.value
        elif isinstance(y, (bool, type(None), int)):
            return x.value != y
        return NotImplemented
    def __hash__(x):
        return hash(x.value)
    def __index__(x):
        if x.value is False:
            return 0
        elif x.value is True:
            return 1
        return 2
    def __repr__(x):
        return "Logical(%r)" % x.string
    def __str__(x):
        return x.string
Logical.true = object.__new__(Logical)
Logical.true.value = True
Logical.true.string = 'T'
Logical.false = object.__new__(Logical)
Logical.false.value = False
Logical.false.string = 'F'
Logical.unknown = object.__new__(Logical)
Logical.unknown.value = None
Logical.unknown.string = '?'
Truth = Logical(True)
Falsth = Logical(False)
Unknown = Logical()

class Quantum(object):
    """return type for Logical fields; implements boolean algebra
    can take the values of True, False, or None/Null/Unknown/Other"""
    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, (str, unicode)):
            if value.lower() in ('t','true','y','yes','on'):
                return cls.true
            elif value.lower() in ('f','false','n','no','off'):
                return cls.false
            elif value.lower() in ('?','unknown','null','none',' ',''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Quantum: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]
    def A(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True or y is not Other and y == True:
            return x.true
        elif x.value is False and y is not Other and y == False:
            return x.false
        return Other
    def _C_material(x, y):
        "IMP (material implication) x >> y => False iff x == True and y == False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x.value is False
            or (x.value is True and y is not Other and y == True)):
            return x.true
        elif x.value is True and y is not Other and y == False:
            return False
        return Other
    def _C_material_reversed(y, x):
        "IMP (material implication) x >> y => False iff x = True and y = False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x is not Other and x == False
            or (x is not Other and x == True and y.value is True)):
            return y.true
        elif x is not Other and x == True and y.value is False:
            return y.false
        return Other
    def _C_relevant(x, y):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if x is False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        if x.value is True and y is not Other and y == False:
            return x.false
        return Other
    def _C_relevant_reversed(y, x):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if y is False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x is not Other and x == True and y.value is True:
            return y.true
        if x is not Other and x == True and y.value is False:
            return y.false
        return Other
    def D(x, y):
        "NAND (negative AND) x.D(y): False iff x and y are both True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is False or y is not Other and y == False:
            return x.true
        elif x.value is True and y is not Other and y == True:
            return x.false
        return Other
    def E(x, y):
        "EQV (equivalence) x.E(y): True iff x and y are the same"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other
    def J(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        if (
            (x.value is False and y is not Other and y == False)
            or
            (x.value is True and y is not Other and y == True)
            ):
            return x.false
        return Other
    def K(x, y):
        "AND (conjunction) x & y: True iff both x, y are True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        elif x.value is False or y is not Other and y == False:
            return x.false
        return Other
    def N(x):
        "NEG (negation) -x: True iff x = False"
        if x is x.true:
            return x.false
        elif x is x.false:
            return x.true
        return Other
    @classmethod
    def set_implication(cls, method):
        "sets IMP to material or relevant"
        if not isinstance(method, (str, unicode)) or method.lower() not in ('material','relevant'):
            raise ValueError("method should be 'material' (for strict boolean) or 'relevant', not %r'" % method)
        if method.lower() == 'material':
            cls.C = cls._C_material
            cls.__rshift__ = cls._C_material
            cls.__rrshift__ = cls._C_material_reversed
        elif method.lower() == 'relevant':
            cls.C = cls._C_relevant
            cls.__rshift__ = cls._C_relevant
            cls.__rrshift__ = cls._C_relevant_reversed
    def __eq__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other
    def __hash__(x):
        return hash(x.value)
    def __index__(x):
        if x.value is False:
            return 0
        if x.value is True:
            return 1
        if x.value is None:
            return 2
    def __ne__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.false
        return Other
    def __nonzero__(x):
        if x is Other:
            raise TypeError('True/False value of %r is unknown' % x)
        return x.value is True
    def __repr__(x):
        return "Quantum(%r)" % x.string
    def __str__(x):
        return x.string
    __add__ = A
    __and__ = K
    __mul__ = K
    __neg__ = N
    __or__ = A
    __radd__ = A
    __rand__ = K
    __rshift__ = None
    __rmul__ = K
    __ror__ = A
    __rrshift__ = None
    __rxor__ = J
    __xor__ = J
Quantum.true = object.__new__(Quantum)
Quantum.true.value = True
Quantum.true.string = 'Y'
Quantum.false = object.__new__(Quantum)
Quantum.false.value = False
Quantum.false.string = 'N'
Quantum.unknown = object.__new__(Quantum)
Quantum.unknown.value = None
Quantum.unknown.string = '?'
Quantum.set_implication('material')
On = Quantum(True)
Off = Quantum(False)
Other = Quantum()

# Internal classes
class _Navigation(object):
    "provides VPFish movement methods"
    _index = -1
    def _nav_check(self):
        "implemented by subclass; must return True if underlying structure meets need"
        raise NotImplementedError()
    def _get_index(self, direction, n=1, start=None):
        "returns index of next available record towards direction"
        if start is not None:
            index = start
        else:
            index = self._index
        if direction == 'reverse':
            move = -1 * n
            limit = 0
            index += move
            if index < limit:
                return -1
            else:
                return index
        elif direction == 'forward':
            move = +1 * n
            limit = len(self) - 1
            index += move
            if index > limit:
                return len(self)
            else:
                return index
        else:
            raise ValueError("direction should be 'forward' or 'reverse', not %r" % direction)
    @property
    def bof(self):
        "returns True if no more usable records towards the beginning of the table"
        self._nav_check()
        index = self._get_index('reverse')
        return index == -1
    def bottom(self):
        """sets record index to bottom of table (end of table)"""
        self._nav_check()
        self._index = len(self)
        return self._index
    @property
    def current_record(self):
        "returns current record (deleted or not)"
        self._nav_check()
        index = self._index
        if index < 0:
            return RecordVaporWare('bof', self)
        elif index >= len(self):
            return RecordVaporWare('eof', self)
        return self[index]
    @property
    def current(self):
        "returns current index"
        self._nav_check()
        return self._index
    @property
    def eof(self):
        "returns True if no more usable records towards the end of the table"
        self._nav_check()
        index = self._get_index('forward')
        return index == len(self)
    @property
    def first_record(self):
        "returns first available record (does not move index)"
        self._nav_check()
        index = self._get_index('forward', start=-1)
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)
    def goto(self, where):
        """changes the record pointer to the first matching (deleted) record
        where should be either an integer, or 'top' or 'bottom'. 
        top    -> before first record
        bottom -> after last record"""
        self._nav_check()
        max = len(self)
        if isinstance(where, (int, long)):
            if not -max <= where < max:
                raise IndexError("Record %d does not exist" % where)
            if where < 0:
                where += max
            self._index = where
            return self._index
        move = getattr(self, where, None)
        if move is None:
            raise DbfError("unable to go to %r" % where)
        return move()
    @property
    def last_record(self):
        "returns first available record (does not move index)"
        self._nav_check()
        index = self._get_index('reverse', start=len(self))
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)
    @property
    def next_record(self):
        "returns next available record (does not move index)"
        self._nav_check()
        index = self._get_index('forward')
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('eof', self)
    @property
    def prev_record(self):
        "returns previous available record (does not move index)"
        self._nav_check()
        index = self._get_index('reverse')
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)
    def skip(self, n=1):
        "move index to the next nth available record"
        self._nav_check()
        if n < 0:
            n *= -1
            direction = 'reverse'
        else:
            direction = 'forward'
        self._index = index = self._get_index(direction, n)
        if index < 0:
            raise Bof
        elif index >= len(self):
            raise Eof()
        else:
            return index
    def top(self):
        """sets record index to top of table (beginning of table)"""
        self._nav_check()
        self._index = -1
        return self._index

class Record(object):
    """Provides routines to extract and save data within the fields of a dbf record."""
    __slots__ = ('_recnum', '_meta', '_data', '_old_data', '_dirty', '_memos', '_write_to_disk', '__weakref__')
    def __new__(cls, recnum, layout, kamikaze='', _fromdisk=False):
        """record = ascii array of entire record; layout=record specification; memo = memo object for table"""
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = recnum
        record._meta = layout
        record._memos = {}
        record._write_to_disk = True
        record._old_data = None
        header = layout.header
        record._data = layout.blankrecord[:]
        if kamikaze and len(record._data) != len(kamikaze):
            raise BadDataError("record data is not the correct length (should be %r, not %r)" %
                    (len(record._data), len(kamikaze)), data=kamikaze[:])
        if recnum == -1:                    # not a disk-backed record
            return record
        elif type(kamikaze) == array:
            record._data = kamikaze[:]
        elif type(kamikaze) == str:
            if kamikaze:
                record._data = array('c', kamikaze)
        else:
            raise BadDataError("%r recieved for record data" % kamikaze)
        if record._data[0] == '\x00':
            record._data[0] = ' '
        if record._data[0] not in (' ','*', '\x00'): # the null byte strikes again!
            raise DbfError("record data not correct -- first character should be a ' ' or a '*'.")
        if not _fromdisk and layout.location == ON_DISK:
            record._update_disk()
        return record
    def __contains__(self, value):
        for field in self._meta.user_fields:
            if self[field] == value:
                return True
        return False
    def __enter__(self):
        if not self._write_to_disk:
            raise DbfError("`with record` is not reentrant")
        self._start_flux()
        return self
    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        if isinstance(other, (Record, RecordTemplate)):
            if field_names(self) != field_names(other):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        elif isinstance(other, dict):
            if sorted(field_names(self)) != sorted(other.keys()):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        else: # tuple
            if len(self) != len(other):
                return False
            for s_value, o_value in zip(self, other):
                if s_value is not o_value and s_value != o_value:
                    return False
        return True
    def __exit__(self, *args):
        if args == (None, None, None):
            self._commit_flux()
        else:
            self._rollback_flux()
    def __iter__(self):
        return (self[field] for field in self._meta.user_fields)
    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError, 'Method %s is not implemented.' % name
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._memos:
            return self._memos[name]
        try:
            index = self._meta.fields.index(name)
            value = self._retrieve_field_value(index, name)
            return value
        except DbfError, error:
            error.message = "field --%s-- is %s -> %s" % (name, self._meta.fieldtypes[fielddef['type']]['Type'], error.message)
            raise
    def __getitem__(self, item):
        if isinstance(item, (int, long)):
            fields = self._meta.user_fields
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise IndexError("Field offset %d is not in record" % item)
            field = fields[item]
            if field in self._memos:
                return self._memos[field]
            return self[field]
        elif isinstance(item, slice):
            sequence = []
            if isinstance(item.start, (str, unicode)) or isinstance(item.stop, (str, unicode)):
                field_names = dbf.field_names(self)
                start, stop, step = item.start, item.stop, item.step
                if start not in field_names or stop not in field_names:
                    raise MissingFieldError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, (int, long)):
                    raise DbfError("step value must be an int or long, not %r" % type(step))
                start = field_names.index(start)
                stop = field_names.index(stop) + 1
                item = slice(start, stop, step)
            for index in self._meta.fields[item]:
                sequence.append(self[index])
            return sequence
        elif isinstance(item, (str, unicode)):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)
    def __len__(self):
        return self._meta.user_field_count
    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        return not self == other
    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        if self._meta.status != READ_WRITE:
            raise DbfError("%s not in read/write mode" % self._meta.filename)
        elif self._write_to_disk:
            raise DbfError("unable to modify fields individually except in `with` or `Process()`")
        elif not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._meta.memofields:
            self._memos[name] = value
            self._dirty = True
            return
        index = self._meta.fields.index(name)
        try:
            self._update_field_value(index, name, value)
        except DbfError, error:
            fielddef = self._meta[name]
            message = "%s (%s) = %r --> %s" % (name, self._meta.fieldtypes[fielddef[TYPE]]['Type'], value, error.args)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data)
    def __setitem__(self, name, value):
        if self._meta.status != READ_WRITE:
            raise DbfError("%s not in read/write mode" % self._meta.filename)
        if self._write_to_disk:
            raise DbfError("unable to modify fields individually except in `with` or `Process()`")
        if isinstance(name, (str, unicode)):
            self.__setattr__(name, value)
        elif isinstance(name, (int, long)):
            self.__setattr__(self._meta.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            field_names = dbf.field_names(self)
            if isinstance(name.start, (str, unicode)) or isinstance(name.stop, (str, unicode)):
                start, stop, step = name.start, name.stop, name.step
                if start not in field_names or stop not in field_names:
                    raise MissingFieldError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, (int, long)):
                    raise DbfError("step value must be an int or long, not %r" % type(step))
                start = field_names.index(start)
                stop = field_names.index(stop) + 1
                name = slice(start, stop, step)
            for field in self._meta.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                self[field] = val
        else:
            raise TypeError("%s is not a field name" % name)
    def __str__(self):
        result = []
        for seq, field in enumerate(field_names(self)):
            result.append("%3d - %-10s: %r" % (seq, field, self[field]))
        return '\n'.join(result)
    def __repr__(self):

        return self._data.tostring()
    def _commit_flux(self):
        "stores field updates to disk; if any errors restores previous contents and propogates exception"
        if self._write_to_disk:
            raise DbfError("record not in flux")
        try:
            self._write()
        except Exception, exc:
            self._data[:] = self._old_data
            self._update_disk(data=self._old_data)
            raise DbfError("unable to write updates to disk, original data restored: %r" % (exc,))
        self._memos.clear()
        self._old_data = None
        self._write_to_disk = True
        self._reindex_record()

    @classmethod
    def _create_blank_data(cls, layout):
        "creates a blank record data chunk"
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = -1
        record._meta = layout
        record._data = array('c', ' ' * layout.header.record_length)
        layout.memofields = []
        signature = [layout.table().codepage.name]
        for index, name in enumerate(layout.fields):
            if name == '_nullflags':
                record._data[layout['_nullflags'][START]:layout['_nullflags'][END]] = array('c', chr(0) * layout['_nullflags'][LENGTH])
        for index, name in enumerate(layout.fields):
            signature.append(name)
            if name != '_nullflags':
                type = layout[name][TYPE]
                start = layout[name][START]
                size = layout[name][LENGTH]
                end = layout[name][END]
                record._data[start:end] = array('c', layout.fieldtypes[type]['Blank'](size))
                if layout[name][TYPE] in layout.memo_types:
                    layout.memofields.append(name)
                decimals = layout[name][DECIMALS]
                signature[-1] = '_'.join([str(x) for x in (signature[-1], type, size, decimals)])
        layout.blankrecord = record._data[:]
        layout.record_sig = '___'.join(signature)
    def _reindex_record(self):
        "rerun all indices with this record"
        if self._meta.status == CLOSED:
            raise DbfError("%s is closed; cannot alter indices" % self._meta.filename)
        elif not self._write_to_disk:
            raise DbfError("unable to reindex record until it is written to disk")
        for dbfindex in self._meta.table()._indexen:
            dbfindex(self)
    def _retrieve_field_value(self, index, name):
        """calls appropriate routine to convert value stored in field from array
        @param record_data: the data portion of the record
        @type record_data: array of characters
        @param fielddef: description of the field definition
        @type fielddef: dictionary with keys 'type', 'start', 'length', 'end', 'decimals', and 'flags'
        @returns: python data stored in field"""
        fielddef = self._meta[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE
        binary = flags & BINARY
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]]
            try:
                if ord(null_data[byte]) >> bit & 1:
                    return Null
            except IndexError:
                print null_data
                print index
                print byte, bit
                print len(self._data), self._data
                print null_def
                print null_data
                raise

        record_data = self._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = self._meta.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, self._meta.memo, self._meta.decoder)
        return datum
    def _rollback_flux(self):
        "discards all changes since ._start_flux()"
        if self._write_to_disk:
            raise DbfError("record not in flux")
        self._data = self._old_data
        self._old_data = None
        self._memos.clear()
        self._write_to_disk = True
        self._write()
    def _start_flux(self):
        "Allows record.field_name = ... and record[...] = ...; must use ._commit_flux() to commit changes"
        if self._meta.status == CLOSED:
            raise DbfError("%s is closed; cannot modify record" % self._meta.filename)
        elif self._recnum < 0:
            raise DbfError("record has been packed; unable to update")
        elif not self._write_to_disk:
            raise DbfError("record already in a state of flux")
        self._old_data = self._data[:]
        self._write_to_disk = False
    def _update_field_value(self, index, name, value):
        "calls appropriate routine to convert value to ascii bytes, and save it in record"
        fielddef = self._meta[name]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        binary = flags & BINARY
        nullable = flags & NULLABLE
        update = self._meta.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]].tostring()
            null_data = [ord(c) for c in null_data]
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            null_data = array('c', [chr(n) for n in null_data])
            self._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('c', update(value, fielddef, self._meta.memo, self._meta.input_decoder, self._meta.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflowError("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('c', ' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            self._data[start:end] = blank[:]
        self._dirty = True
    def _update_disk(self, location='', data=None):
        layout = self._meta
        if self._recnum < 0:
            raise DbfError("cannot update a packed record")
        if layout.location == ON_DISK:
            header = layout.header
            if location == '':
                location = self._recnum * header.record_length + header.start
            if data is None:
                data = self._data
            layout.dfd.seek(location)
            layout.dfd.write(data)
            self._dirty = False
        table = layout.table()
        if table is not None:  # is None when table is being destroyed
            for index in table._indexen:
                index(self)
    def _write(self):
        for field, value in self._memos.items():
            index = self._meta.fields.index(field)
            self._update_field_value(index, field, value)
        self._update_disk()
class RecordTemplate(object):
    """Provides routines to mimic a dbf record."""
    __slots__ = ('_meta', '_data', '_old_data', '_memos', '__weakref__')
    def _retrieve_field_value(self, index, name):
        """calls appropriate routine to convert value stored in field from array
        @param record_data: the data portion of the record
        @type record_data: array of characters
        @param fielddef: description of the field definition
        @type fielddef: dictionary with keys 'type', 'start', 'length', 'end', 'decimals', and 'flags'
        @returns: python data stored in field"""
        fielddef = self._meta[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE
        binary = flags & BINARY
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]]
            if ord(null_data[byte]) >> bit & 1:
                return Null
        record_data = self._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = self._meta.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, self._meta.memo, self._meta.decoder)
        return datum
    def _update_field_value(self, index, name, value):
        "calls appropriate routine to convert value to ascii bytes, and save it in record"
        fielddef = self._meta[name]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        binary = flags & BINARY
        nullable = flags & NULLABLE
        update = self._meta.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]].tostring()
            null_data = [ord(c) for c in null_data]
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            null_data = array('c', [chr(n) for n in null_data])
            self._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('c', update(value, fielddef, self._meta.memo, self._meta.input_decoder, self._meta.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflowError("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('c', ' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            self._data[start:end] = blank[:]
    def __new__(cls, layout, original_record=None, defaults=None):
        """record = ascii array of entire record; layout=record specification"""
        sig = layout.record_sig
        if sig not in _Template_Records:
            _Template_Records[sig] = layout.table().new(':%s:' % layout.filename)._meta
        layout = _Template_Records[sig]
        record = object.__new__(cls)
        record._meta = layout
        record._memos = {}
        for name in layout.memofields:
            field_type = layout[name][TYPE]
            record._memos[name] = layout.fieldtypes[field_type]['Empty']()
        header = layout.header
        if original_record is None:
            record._data = layout.blankrecord[:]
        else:
            record._data = original_record._data[:]
            for name in layout.memofields:
                record._memos[name] = original_record[name]
        for field in field_names(defaults or {}):
            record[field] = defaults[field]
        record._old_data = record._data[:]
        return record
    def __contains__(self, key):
        return key in self._meta.user_fields
    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        if isinstance(other, (Record, RecordTemplate)):
            if field_names(self) != field_names(other):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        elif isinstance(other, dict):
            if sorted(field_names(self)) != sorted(other.keys()):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        else: # tuple
            if len(self) != len(other):
                return False
            for s_value, o_value in zip(self, other):
                if s_value is not o_value and s_value != o_value:
                    return False
        return True
    def __iter__(self):
        return (self[field] for field in self._meta.user_fields)
    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError, 'Method %s is not implemented.' % name
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._memos:
            return self._memos[name]
        try:
            index = self._meta.fields.index(name)
            value = self._retrieve_field_value(index, name)
            return value
        except DbfError, error:
            error.message = "field --%s-- is %s -> %s" % (name, self._meta.fieldtypes[fielddef['type']]['Type'], error.message)
            raise
    def __getitem__(self, item):
        if isinstance(item, (int, long)):
            fields = self._meta.user_fields
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise IndexError("Field offset %d is not in record" % item)
            field = fields[item]
            if field in self._memos:
                return self._memos[field]
            return self[field]
        elif isinstance(item, slice):
            sequence = []
            if isinstance(item.start, (str, unicode)) or isinstance(item.stop, (str, unicode)):
                start, stop, step = item.start, item.stop, item.step
                if start not in yo.field_names or stop not in yo.field_names:
                    raise MissingFieldError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, (int, long)):
                    raise DbfError("step value must be an int or long, not %r" % type(step))
                start = yo.field_names.index(start)
                stop = yo.field_names.index(stop) + 1
                item = slice(start, stop, step)
            for index in self._meta.fields[item]:
                sequence.append(self[index])
            return sequence
        elif isinstance(item, (str, unicode)):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)
    def __len__(self):
        return self._meta.user_field_count
    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        return not self == other
    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._meta.memofields:
            self._memos[name] = value
            self._dirty = True
            return
        index = self._meta.fields.index(name)
        try:
            self._update_field_value(index, name, value)
        except DbfError, error:
            fielddef = self._meta[name]
            message = "%s (%s) = %r --> %s" % (name, self._meta.fieldtypes[fielddef[TYPE]]['Type'], value, error.message)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data)
    def __setitem__(self, name, value):
        if isinstance(name, (str, unicode)):
            self.__setattr__(name, value)
        elif isinstance(name, (int, long)):
            self.__setattr__(self._meta.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            for field in self._meta.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                self.__setattr__(field, val)
        else:
            raise TypeError("%s is not a field name" % name)
    def __repr__(self):
        return self._data.tostring()
    def __str__(self):
        result = []
        for seq, field in enumerate(field_names(self)):
            result.append("%3d - %-10s: %r" % (seq, field, self[field]))
        return '\n'.join(result)
class RecordVaporWare(object):
    """Provides routines to mimic a dbf record, but all values are non-existent."""
    __slots__ = ('_recno', '_sequence')
    def __new__(cls, position, sequence):
        """record = ascii array of entire record; layout=record specification; memo = memo object for table"""
        if position not in ('bof', 'eof'):
            raise ValueError("position should be 'bof' or 'eof', not %r" % position)
        vapor = object.__new__(cls)
        vapor._recno = (-1, None)[position == 'eof']
        vapor._sequence = sequence
        return vapor
    def __contains__(self, key):
        return False
    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, RecordVaporWare, dict, tuple)):
            return NotImplemented
        return False

    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError, 'Method %s is not implemented.' % name
        else:
            return Vapor
    def __getitem__(self, item):
        if isinstance(item, (int, long)):
            return Vapor
        elif isinstance(item, slice):
            raise TypeError('slice notation not allowed on Vapor records')
        elif isinstance(item, (str, unicode)):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)
    def __len__(self):
        raise TypeError("Vapor records have no length")
    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, RecordVaporWare, dict, tuple)):
            return NotImplemented
        return True
    def __nonzero__(self):
        "Vapor records are always False"
        return False
    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        raise TypeError("cannot change Vapor record")
    def __setitem__(self, name, value):
        if isinstance(name, (str, unicode, int, long)):
            raise TypeError("cannot change Vapor record")
        elif isinstance(name, slice):
            raise TypeError("slice notation not allowed on Vapor records")
        else:
            raise TypeError("%s is not a field name" % name)
    def __repr__(self):
        return "RecordVaporWare(position=%r, sequence=%r)" % (('bof','eof')[recno(self) is None], self._sequence)
    def __str__(self):
        return 'VaporRecord(%r)' % recno(self)
    @property
    def _recnum(self):
        if self._recno is None:
            return len(self._sequence)
        else:
            return self._recno
class _DbfMemo(object):
    """Provides access to memo fields as dictionaries
       must override _init, _get_memo, and _put_memo to
       store memo contents to disk"""
    def _init(self):
        "initialize disk file usage"
    def _get_memo(self, block):
        "retrieve memo contents from disk"
    def _put_memo(self, data):
        "store memo contents to disk"
    def _zap(self):
        "resets memo structure back to zero memos"
        self.memory.clear()
        self.nextmemo = 1
    def __init__(self, meta):
        ""
        self.meta = meta
        self.memory = {}
        self.nextmemo = 1
        self._init()
        self.meta.newmemofile = False
    def get_memo(self, block):
        "gets the memo in block"
        if self.meta.ignorememos or not block:
            return ''
        if self.meta.location == ON_DISK:
            return self._get_memo(block)
        else:
            return self.memory[block]
    def put_memo(self, data):
        "stores data in memo file, returns block number"
        if self.meta.ignorememos or data == '':
            return 0
        if self.meta.location == IN_MEMORY:
            thismemo = self.nextmemo
            self.nextmemo += 1
            self.memory[thismemo] = data
        else:
            thismemo = self._put_memo(data)
        return thismemo
class _Db3Memo(_DbfMemo):
    def _init(self):
        "dBase III specific"
        self.meta.memo_size= 512
        self.record_header_length = 2
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            if self.meta.newmemofile:
                self.meta.mfd = open(self.meta.memoname, 'w+b')
                self.meta.mfd.write(pack_long_int(1) + '\x00' * 508)
            else:
                try:
                    self.meta.mfd = open(self.meta.memoname, 'r+b')
                    self.meta.mfd.seek(0)
                    next = self.meta.mfd.read(4)
                    self.nextmemo = unpack_long_int(next)
                except Exception, exc:
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args)
    def _get_memo(self, block):
        block = int(block)
        self.meta.mfd.seek(block * self.meta.memo_size)
        eom = -1
        data = ''
        while eom == -1:
            newdata = self.meta.mfd.read(self.meta.memo_size)
            if not newdata:
                return data
            data += newdata
            eom = data.find('\x1a\x1a')
        return data[:eom]
    def _put_memo(self, data):
        data = data
        length = len(data) + self.record_header_length  # room for two ^Z at end of memo
        blocks = length // self.meta.memo_size
        if length % self.meta.memo_size:
            blocks += 1
        thismemo = self.nextmemo
        self.nextmemo = thismemo + blocks
        self.meta.mfd.seek(0)
        self.meta.mfd.write(pack_long_int(self.nextmemo))
        self.meta.mfd.seek(thismemo * self.meta.memo_size)
        self.meta.mfd.write(data)
        self.meta.mfd.write('\x1a\x1a')
        double_check = self._get_memo(thismemo)
        if len(double_check) != len(data):
            uhoh = open('dbf_memo_dump.err','wb')
            uhoh.write('thismemo: %d' % thismemo)
            uhoh.write('nextmemo: %d' % self.nextmemo)
            uhoh.write('saved: %d bytes' % len(data))
            uhoh.write(data)
            uhoh.write('retrieved: %d bytes' % len(double_check))
            uhoh.write(double_check)
            uhoh.close()
            raise DbfError("unknown error: memo not saved")
        return thismemo
    def _zap(self):
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            mfd.write(pack_long_int(1) + '\x00' * 508)
            mfd.flush()

class _VfpMemo(_DbfMemo):
    def _init(self):
        "Visual Foxpro 6 specific"
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            self.record_header_length = 8
            if self.meta.newmemofile:
                if self.meta.memo_size == 0:
                    self.meta.memo_size = 1
                elif 1 < self.meta.memo_size < 33:
                    self.meta.memo_size *= 512
                self.meta.mfd = open(self.meta.memoname, 'w+b')
                nextmemo = 512 // self.meta.memo_size
                if nextmemo * self.meta.memo_size < 512:
                    nextmemo += 1
                self.nextmemo = nextmemo
                self.meta.mfd.write(pack_long_int(nextmemo, bigendian=True) + '\x00\x00' + \
                        pack_short_int(self.meta.memo_size, bigendian=True) + '\x00' * 504)
            else:
                try:
                    self.meta.mfd = open(self.meta.memoname, 'r+b')
                    self.meta.mfd.seek(0)
                    header = self.meta.mfd.read(512)
                    self.nextmemo = unpack_long_int(header[:4], bigendian=True)
                    self.meta.memo_size = unpack_short_int(header[6:8], bigendian=True)
                except Exception, exc:
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args)
    def _get_memo(self, block):
        self.meta.mfd.seek(block * self.meta.memo_size)
        header = self.meta.mfd.read(8)
        length = unpack_long_int(header[4:], bigendian=True)
        return self.meta.mfd.read(length)
    def _put_memo(self, data):
        data = data
        self.meta.mfd.seek(0)
        thismemo = unpack_long_int(self.meta.mfd.read(4), bigendian=True)
        self.meta.mfd.seek(0)
        length = len(data) + self.record_header_length
        blocks = length // self.meta.memo_size
        if length % self.meta.memo_size:
            blocks += 1
        self.meta.mfd.write(pack_long_int(thismemo+blocks, bigendian=True))
        self.meta.mfd.seek(thismemo*self.meta.memo_size)
        self.meta.mfd.write('\x00\x00\x00\x01' + pack_long_int(len(data), bigendian=True) + data)
        return thismemo
    def _zap(self):
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            nextmemo = 512 // self.meta.memo_size
            if nextmemo * self.meta.memo_size < 512:
                nextmemo += 1
            self.nextmemo = nextmemo
            mfd.write(pack_long_int(nextmemo, bigendian=True) + '\x00\x00' + \
                    pack_short_int(self.meta.memo_size, bigendian=True) + '\x00' * 504)
            mfd.flush()

class DbfCsv(csv.Dialect):
    "csv format for exporting tables"
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    skipinitialspace = True
    quoting = csv.QUOTE_NONNUMERIC
csv.register_dialect('dbf', DbfCsv)
class _DeadObject(object):
    "used because you cannot weakref None"
    def __nonzero__(self):
        return False
_DeadObject = _DeadObject()

# Routines for saving, retrieving, and creating fields

VFPTIME = 1721425

def pack_short_int(value, bigendian=False):
        "Returns a two-bye integer from the value, or raises DbfError"
        # 256 / 65,536
        if value > 65535:
            raise DataOverflowError("Maximum Integer size exceeded.  Possible: 65535.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>H', value)
        else:
            return struct.pack('<H', value)
def pack_long_int(value, bigendian=False):
        "Returns a four-bye integer from the value, or raises DbfError"
        # 256 / 65,536 / 16,777,216
        if value > 4294967295:
            raise DataOverflowError("Maximum Integer size exceeded.  Possible: 4294967295.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>L', value)
        else:
            return struct.pack('<L', value)
def pack_str(string):
        "Returns an 11 byte, upper-cased, null padded string suitable for field names; raises DbfError if the string is bigger than 10 bytes"
        if len(string) > 10:
            raise DbfError("Maximum string size is ten characters -- %s has %d characters" % (string, len(string)))
        return struct.pack('11s', string.upper())       
def unpack_short_int(bytes, bigendian=False):
        "Returns the value in the two-byte integer passed in"
        if bigendian:
            return struct.unpack('>H', bytes)[0]
        else:
            return struct.unpack('<H', bytes)[0]
def unpack_long_int(bytes, bigendian=False):
        "Returns the value in the four-byte integer passed in"
        if bigendian:
            return int(struct.unpack('>L', bytes)[0])
        else:
            return int(struct.unpack('<L', bytes)[0])
def unpack_str(chars):
        "Returns a normal, lower-cased string from a null-padded byte string"
        field = struct.unpack('%ds' % len(chars), chars)[0]
        name = []
        for ch in field:
            if ch == '\x00':
                break
            name.append(ch.lower())
        return ''.join(name)
def unsupported_type(something, *ignore):
    "called if a data type is not supported for that style of table"
    return something
def retrieve_character(bytes, fielddef, memo, decoder):
    "Returns the string in bytes as fielddef[CLASS] or fielddef[EMPTY]"
    data = bytes.tostring()
    if not data.strip():
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls(data)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_character(string, fielddef, memo, decoder, encoder):
    "returns the string as bytes (not unicode) as fielddef[CLASS] or fielddef[EMPTY]"
    length = fielddef[LENGTH]
    if string == None:
        return length * ' '
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise ValueError('binary field: %r not in bytes format' % string)
        string = str(string)
        return string
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        string = encoder(string)[0]
        if not string[length:].strip():     # trims trailing white space to fit in field
            string = string[:length]
        return string
def retrieve_currency(bytes, fielddef, *ignore):
    "returns the currency value in bytes"
    value = struct.unpack('<q', bytes)[0]
    return fielddef[CLASS](("%de-4" % value).strip())
def update_currency(value, *ignore):
    "returns the value to be stored in the record's disk data"
    if value == None:
        value = 0
    currency = int(value * 10000)
    if not -9223372036854775808 < currency < 9223372036854775808:
        raise DataOverflowError("value %s is out of bounds" % value)
    return struct.pack('<q', currency)
def retrieve_date(bytes, fielddef, *ignore):
    "Returns the ascii coded date as fielddef[CLASS] or fielddef[EMPTY]"
    text = bytes.tostring()
    if text == '        ':
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    year = int(text[0:4])
    month = int(text[4:6])
    day = int(text[6:8])
    return fielddef[CLASS](year, month, day)
def update_date(moment, *ignore):
    "returns the Date or datetime.date object ascii-encoded (yyyymmdd)"
    if moment == None:
        return '        '
    return "%04d%02d%02d" % moment.timetuple()[:3]
def retrieve_double(bytes, fielddef, *ignore):
    "Returns the double in bytes as fielddef[CLASS] ('default' == float)"
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = float
    return typ(struct.unpack('<d', bytes)[0])
def update_double(value, *ignore):
    "returns the value to be stored in the record's disk data"
    if value == None:
        value = 0
    return struct.pack('<d', float(value))
def retrieve_integer(bytes, fielddef, *ignore):
    "Returns the binary number stored in bytes in little-endian format as fielddef[CLASS]"
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = int
    return typ(struct.unpack('<i', bytes)[0])
def update_integer(value, *ignore):
    "returns value in little-endian binary format"
    if value == None:
        value = 0
    try:
        value = int(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value))
    if not -2147483648 < value < 2147483647:
        raise DataOverflowError("Integer size exceeded.  Possible: -2,147,483,648..+2,147,483,647.  Attempted: %d" % value)
    return struct.pack('<i', int(value))
def retrieve_logical(bytes, fielddef, *ignore):
    "Returns True if bytes is 't', 'T', 'y', or 'Y', None if '?', and False otherwise"
    cls = fielddef[CLASS]
    empty = fielddef[EMPTY]
    bytes = bytes.tostring()
    if bytes in 'tTyY':
        return cls(True)
    elif bytes in 'fFnN':
        return cls(False)
    elif bytes in '? ':
        if empty is NoneType:
            return None
        return empty()
    elif LOGICAL_BAD_IS_FALSE:
        return False
    else:
        raise BadDataError('Logical field contained %r' % bytes)
    return typ(bytes)
def update_logical(data, *ignore):
    "Returns 'T' if logical is True, 'F' if False, '?' otherwise"
    if data is Unknown or data is None or data is Null or data is Other:
        return '?'
    if data == True:
        return 'T'
    if data == False:
        return 'F'
    raise ValueError("unable to automatically coerce %r to Logical" % data)
def retrieve_memo(bytes, fielddef, memo, decoder):
    "Returns the block of data from a memo file"
    stringval = bytes.tostring().strip()
    if not stringval:
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    block = int(stringval)
    data = memo.get_memo(block)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_memo(string, fielddef, memo, decoder, encoder):
    "Writes string as a memo, returns the block number it was saved into"
    if string == None:
        string = ''
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise ValueError('binary field: %r not in bytes format' % string)
        string = str(string)
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        string = encoder(string)[0]
    block = memo.put_memo(string)
    if block == 0:
        block = ''
    return "%*s" % (fielddef[LENGTH], block)
def retrieve_numeric(bytes, fielddef, *ignore):
    "Returns the number stored in bytes as integer if field spec for decimals is 0, float otherwise"
    string = bytes.tostring().replace('\x00','').strip()
    cls = fielddef[CLASS]
    if not string or string[0:1] == '*':  # value too big to store (Visual FoxPro idiocy)
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    if cls == 'default':
        if fielddef[DECIMALS] == 0:
            return int(string)
        else:
            return float(string)
    else:
        return cls(string.strip())
def update_numeric(value, fielddef, *ignore):
    "returns value as ascii representation, rounding decimal portion as necessary"
    if value == None:
        return fielddef[LENGTH] * ' '
    try:
        value = float(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value))
    decimalsize = fielddef[DECIMALS]
    if decimalsize:
        decimalsize += 1
    maxintegersize = fielddef[LENGTH]-decimalsize
    integersize = len("%.0f" % floor(value))
    if integersize > maxintegersize:
        raise DataOverflowError('Integer portion too big')
    return "%*.*f" % (fielddef[LENGTH], fielddef[DECIMALS], value)
def retrieve_vfp_datetime(bytes, fielddef, *ignore):
    """returns the date/time stored in bytes; dates <= 01/01/1981 00:00:00
    may not be accurate;  BC dates are nulled."""
    # two four-byte integers store the date and time.
    # millesecords are discarded from time
    if bytes == array('c','\x00' * 8):
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    cls = fielddef[CLASS]
    time = unpack_long_int(bytes[4:])
    microseconds = (time % 1000) * 1000
    time = time // 1000                      # int(round(time, -3)) // 1000 discard milliseconds
    hours = time // 3600
    mins = time % 3600 // 60
    secs = time % 3600 % 60
    time = datetime.time(hours, mins, secs, microseconds)
    possible = unpack_long_int(bytes[:4])
    possible -= VFPTIME
    possible = max(0, possible)
    date = datetime.date.fromordinal(possible)
    return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
def update_vfp_datetime(moment, *ignore):
    """sets the date/time stored in moment
    moment must have fields year, month, day, hour, minute, second, microsecond"""
    bytes = ['\x00'] * 8
    if moment:
        hour = moment.hour
        minute = moment.minute
        second = moment.second
        millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
        time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
        bytes[4:] = update_integer(time)
        bytes[:4] = update_integer(moment.toordinal() + VFPTIME)
    return ''.join(bytes)
def retrieve_vfp_memo(bytes, fielddef, memo, decoder):
    "Returns the block of data from a memo file"
    block = struct.unpack('<i', bytes)[0]
    if not block:
        cls =  fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    data = memo.get_memo(block)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_vfp_memo(string, fielddef, memo, decoder, encoder):
    "Writes string as a memo, returns the block number it was saved into"
    if string == None:
        string = ''
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise ValueError('binary field: %r not in bytes format' % string)
        string = str(string)
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        string = encoder(string)[0]
    block = memo.put_memo(string)
    return struct.pack('<i', block)
def add_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise InvalidFieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 255:
        raise InvalidFieldSpecError("Character fields must be between 1 and 255")
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_date(format, flags):
    if any([f not in flags for f in format]):
        raise InvalidFieldSpecError("Format for Date field creation is 'D%s', not 'D%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_logical(format, flags):
    if any([f not in flags for f in format]):
        raise InvalidFieldSpecError("Format for Logical field creation is 'L%s', not 'L%s'" % field_spec_error_text(format, flags))
    length = 1
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_memo(format, flags):
    if any(f not in flags for f in format):
        raise InvalidFieldSpecError("Format for Memo field creation is 'M(n)%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 10
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_numeric(format, flags):
    if len(format) > 1 or format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Numeric field creation is 'N(s,d)%s', not 'N%s'" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    if not 0 < length < 18:
        raise InvalidFieldSpecError("Numeric fields must be between 1 and 17 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise InvalidFieldSpecError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag
def add_clp_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise InvalidFieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 65519:
        raise InvalidFieldSpecError("Character fields must be between 1 and 65,519")
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise InvalidFieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 255:
        raise InvalidFieldSpecError("Character fields must be between 1 and 255")
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_currency(format, flags):
    if any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Currency field creation is 'Y%s', not 'Y%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_datetime(format, flags):
    if any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for DateTime field creation is 'T%s', not 'T%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_double(format, flags):
    if any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Double field creation is 'B%s', not 'B%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_integer(format, flags):
    if any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Integer field creation is 'I%s', not 'I%s'" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_memo(format, flags):
    if any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Memo field creation is 'M%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    if 'binary' not in flags:   # general or picture -- binary is implied
        flag |= FIELD_FLAGS['binary']
    return length, decimals, flag
def add_vfp_numeric(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise InvalidFieldSpecError("Format for Numeric field creation is 'N(s,d)%s', not 'N%s'" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    if not 0 < length < 21:
        raise InvalidFieldSpecError("Numeric fields must be between 1 and 20 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise InvalidFieldSpecError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag
def field_spec_error_text(format, flags):
    "generic routine for error text for the add...() functions"
    flg = ''
    if flags:
        flg = ' [ ' + ' | '.join(flags) + ' ]'
    frmt = ''
    if format:
        frmt = ' ' + ' '.join(format)
    return flg, frmt

def ezip(*iters):
    "extends all iters to longest one, using last value from each as necessary"
    iters = [iter(x) for x in iters]
    last = [None] * len(iters)
    while "any iters have items left":
        alive = len(iters)
        for i, iterator in enumerate(iters):
            try:
                value = next(iterator)
                last[i] = value
            except StopIteration:
                alive -= 1
        if alive:
            yield tuple(last)
            alive = len(iters)
            continue
        break

# Public classes

class FieldInfo(tuple):
    "tuple with named attributes for representing a field's dbf type, length, decimal portion, and python class"
    __slots__= ()
    def __new__(cls, *args):
        if len(args) != 4:
            raise TypeError("%s should be called with Type, Length, Decimal size, and Class" % cls.__name__)
        return tuple.__new__(cls, args)
    @property
    def dbf_type(self):
        return self[0]
    @property
    def length(self):
        return self[1]
    @property
    def decimal(self):
        return self[2]
    @property
    def py_type(self):
        return self[3]
class CodePage(tuple):
    "tuple with named attributes for representing a tables codepage"
    __slots__= ()
    def __new__(cls, name):
        "call with name of codepage (e.g. 'cp1252')"
        code, name, desc = _codepage_lookup(name)
        return tuple.__new__(cls, (name, desc, code))
    def __repr__(self):
        return "CodePage(%r, %r, %r)" % (self[0], self[1], self[2])
    def __str__(self):
        return "%s (%s)" % (self[0], self[1])
    @property
    def name(self):
        return self[0]
    @property
    def desc(self):
        return self[1]
    @property
    def code(self):
        return self[2]
class Iter(_Navigation):
    "cycles through records in table"
    def __init__(self, table, include_vapor=False):
        "if include_vapor is True a Vapor record is included as the last record"
        self._table = table
        self._record = None
        self._include_vapor = include_vapor
        self._exhausted = False
    def __iter__(self):
        return self
    def next(self):
        while not self._exhausted:
            if self._index == len(self._table):
                break
            if self._index >= (len(self._table) - 1):
                self._index = max(self._index, len(self._table))
                if self._include_vapor:
                    return RecordVaporWare('eof', self._table)
                break
            self._index += 1
            record = self._table[self._index]
            return record
        self._exhausted = True
        raise StopIteration
class Table(_Navigation):
    """Provides a framework for dbf style tables."""
    _version = 'basic memory table'
    _versionabbr = 'dbf'
    _max_fields = 255
    _max_records = 4294967296
    @MutableDefault
    def _field_types():
        return {
                'C' : {
                        'Type':'Character', 'Init':add_character, 'Blank':lambda x: ' '*x, 'Retrieve':retrieve_character, 'Update':update_character,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'D' : { 
                        'Type':'Date', 'Init':add_date, 'Blank':lambda x: '        ', 'Retrieve':retrieve_date, 'Update':update_date,
                        'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                        },
                'F' : {
                        'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_numeric,
                        'Class':'default', 'Empty':none, 'flags':tuple(),
                        },
                'L' : { 
                        'Type':'Logical', 'Init':add_logical, 'Blank':lambda x: '?', 'Retrieve':retrieve_logical, 'Update':update_logical,
                        'Class':bool, 'Empty':none, 'flags':tuple(),
                        },
                'M' : { 
                        'Type':'Memo', 'Init':add_memo, 'Blank':lambda x: '          ', 'Retrieve':retrieve_memo, 'Update':update_memo,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'N' : { 
                        'Type':'Numeric', 'Init':add_numeric, 'Blank':lambda x: ' '*x, 'Retrieve':retrieve_numeric, 'Update':update_numeric,
                        'Class':'default', 'Empty':none, 'flags':tuple(),
                        },
                }
    _memoext = ''
    _memoClass = _DbfMemo
    _yesMemoMask = ''
    _noMemoMask = ''
    _binary_types = tuple()                # as in non-unicode character, or non-text number
    _character_types = ('C','D','F','L','M','N')          # field represented by text data
    _currency_types = tuple()              # money!
    _date_types = ('D', )                  # dates
    _datetime_types = tuple()              # dates w/times
    _decimal_types = ('N', 'F')            # text-based numeric fields
    _fixed_types = ('M','D','L')           # always same length in table
    _logical_types = ('L', )               # logicals
    _memo_types = tuple('M', )
    _numeric_types = ('N', 'F')            # fields representing a number
    _variable_types = ('C', 'N', 'F')      # variable length in table
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x00'             # table type - none
    _dbfTableHeader[8:10] = array('c', pack_short_int(33))
    _dbfTableHeader[10] = '\x01'            # record length -- one for delete flag
    _dbfTableHeader[29] = '\x00'            # code page -- none, using plain ascii
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = []
    _pack_count = 0
    backup = None

    class _Indexen(object):
        "implements the weakref structure for seperate indexes"
        def __init__(self):
            self._indexen = set()
        def __iter__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])    
            return (s() for s in self._indexen if s() is not None)
        def __len__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])
            return len(self._indexen)
        def add(self, new_index):
            self._indexen.add(weakref.ref(new_index))
            self._indexen = set([s for s in self._indexen if s() is not None])
    class _MetaData(dict):
        "per table values"
        blankrecord = None
        dfd = None              # file handle
        fields = None           # field names
        field_count = 0         # number of fields
        field_types = None      # dictionary of dbf type field specs
        filename = None         # name of .dbf file
        ignorememos = False     # True when memos should be ignored
        memoname = None         # name of .dbt/.fpt file
        mfd = None              # file handle
        memo = None             # memo object
        memofields = None       # field names of Memo type
        newmemofile = False     # True when memo file needs to be created
        nulls = None            # non-None when Nullable fields present
        user_fields = None      # not counting SYSTEM fields
        user_field_count = 0    # also not counting SYSTEM fields
    class _TableHeader(object):
        "represents the data block that defines a tables type and layout"
        def __init__(self, data, pack_date, unpack_date):
            if len(data) != 32:
                raise BadDataError('table header should be 32 bytes, but is %d bytes' % len(data))
            self.packDate = pack_date
            self.unpackDate = unpack_date
            self._data = array('c', data + '\x0d')
        def codepage(self, cp=None):
            "get/set code page of table"
            if cp is None:
                return self._data[29]
            else:
                cp, sd, ld = _codepage_lookup(cp)
                self._data[29] = cp                    
                return cp
        @property
        def data(self):
            "main data structure"
            date = self.packDate(Date.today())
            self._data[1:4] = array('c', date)
            return self._data.tostring()
        @data.setter
        def data(self, bytes):
            if len(bytes) < 32:
                raise BadDataError("length for data of %d is less than 32" % len(bytes))
            self._data[:] = array('c', bytes)
        @property
        def extra(self):
            "extra dbf info (located after headers, before data records)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 33    # skip past CR
            return self._data[cr:].tostring()
        @extra.setter
        def extra(self, data):
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 33    # skip past CR
            self._data[cr:] = array('c', data)                             # extra
            self._data[8:10] = array('c', pack_short_int(len(self._data)))  # start
        @property
        def field_count(self):
            "number of fields (read-only)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure")
            return len(fieldblock[:cr]) // 32
        @property
        def fields(self):
            "field block structure"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure")
            return fieldblock[:cr].tostring()
        @fields.setter
        def fields(self, block):
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 32    # convert to indexing main structure
            fieldlen = len(block)
            if fieldlen % 32 != 0:
                raise BadDataError("fields structure corrupt: %d is not a multiple of 32" % fieldlen)
            self._data[32:cr] = array('c', block)                           # fields
            self._data[8:10] = array('c', pack_short_int(len(self._data)))   # start
            fieldlen = fieldlen // 32
            recordlen = 1                                     # deleted flag
            for i in range(fieldlen):
                recordlen += ord(block[i*32+16])
            self._data[10:12] = array('c', pack_short_int(recordlen))
        @property
        def record_count(self):
            "number of records (maximum 16,777,215)"
            return unpack_long_int(self._data[4:8].tostring())
        @record_count.setter
        def record_count(self, count):
            self._data[4:8] = array('c', pack_long_int(count))
        @property
        def record_length(self):
            "length of a record (read_only) (max of 65,535)"
            return unpack_short_int(self._data[10:12].tostring())
        @record_length.setter
        def record_length(self, length):
            "to support Clipper large Character fields"
            self._data[10:12] = array('c', pack_short_int(length))
        @property
        def start(self):
            "starting position of first record in file (must be within first 64K)"
            return unpack_short_int(self._data[8:10].tostring())
        @start.setter
        def start(self, pos):
            self._data[8:10] = array('c', pack_short_int(pos))
        @property
        def update(self):
            "date of last table modification (read-only)"
            return self.unpackDate(self._data[1:4].tostring())
        @property
        def version(self):
            "dbf version"
            return self._data[0]
        @version.setter
        def version(self, ver):
            self._data[0] = ver
    class _Table(object):
        "implements the weakref table for records"
        def __init__(self, count, meta):
            self._meta = meta
            self._weakref_list = [weakref.ref(_DeadObject)] * count
        def __getitem__(self, index):
            maybe = self._weakref_list[index]()
            if not maybe:
                meta = self._meta
                if meta.status == CLOSED:
                    raise DbfError("%s is closed; record %d is unavailable" % (meta.filename, index))
                header = meta.header
                if index < 0:
                    index += header.record_count
                size = header.record_length
                location = index * size + header.start
                meta.dfd.seek(location)
                if meta.dfd.tell() != location:
                    raise ValueError("unable to seek to offset %d in file" % location)
                bytes = meta.dfd.read(size)
                if not bytes:
                    raise ValueError("unable to read record data from %s at location %d" % (meta.filename, location))
                maybe = Record(recnum=index, layout=meta, kamikaze=bytes, _fromdisk=True)
                self._weakref_list[index] = weakref.ref(maybe)
            return maybe
        def append(self, record):
            self._weakref_list.append(weakref.ref(record))
        def clear(self):
            self._weakref_list[:] = []
        def flush(self):
            for maybe in self._weakref_list:
                maybe = maybe()
                if maybe and not maybe._write_to_disk:
                    raise DbfError("some records have not been written to disk")
        def pop(self):
            return self._weakref_list.pop()
    def _build_header_fields(self):
        "constructs fieldblock for disk table"
        fieldblock = array('c', '')
        memo = False
        nulls = False
        meta = self._meta
        header = meta.header
        header.version = chr(ord(header.version) & ord(self._noMemoMask))
        meta.fields = [f for f in meta.fields if f != '_nullflags']
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise BadDataError("corrupted field structure (noticed in _build_header_fields)")
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str(meta.encoder(field)[0]))
            fielddef[11] = layout[TYPE]
            fielddef[12:16] = array('c', pack_long_int(layout[START]))
            fielddef[16] = chr(layout[LENGTH])
            fielddef[17] = chr(layout[DECIMALS])
            fielddef[18] = chr(layout[FLAGS])
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memo_types:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls = True
        if memo:
            header.version = chr(ord(header.version) | ord(self._yesMemoMask))
            if meta.memo is None:
                meta.memo = self._memoClass(meta)
        else:
            if os.path.exists(meta.memoname):
                if meta.mfd is not None:
                    meta.mfd.close()

                os.remove(meta.memoname)
            meta.memo = None
        if nulls:
            start = layout[START] + layout[LENGTH]
            length, one_more = divmod(len(meta.fields), 8)
            if one_more:
                length += 1
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str('_nullflags'))
            fielddef[11] = '0'
            fielddef[12:16] = array('c', pack_long_int(start))
            fielddef[16] = chr(length)
            fielddef[17] = chr(0)
            fielddef[18] = chr(BINARY | SYSTEM)
            fieldblock.extend(fielddef)
            meta.fields.append('_nullflags')
            nullflags = (
                    '0',                # type
                    start,              # start
                    length,             # length
                    start + length,     # end
                    0,                  # decimals
                    BINARY | SYSTEM,    # flags
                    none,               # class
                    none,               # empty
                    )
            meta['_nullflags'] = nullflags
        header.fields = fieldblock.tostring()
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)

    def _check_memo_integrity(self):
        "checks memo file for problems"
        raise NotImplementedError("_check_memo_integrity must be implemented by subclass")
    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions from the disk file"
        raise NotImplementedError("_initialize_fields must be implemented by subclass")
    def _field_layout(self, i):
        "Returns field information Name Type(Length[,Decimals])"
        name = self._meta.fields[i]
        fielddef = self._meta[name]
        type = fielddef[TYPE]
        length = fielddef[LENGTH]
        decimals = fielddef[DECIMALS]
        set_flags = fielddef[FLAGS]
        flags = []
        if type in ('G','P'):
            printable_flags = NULLABLE, SYSTEM
        else:
            printable_flags = BINARY, NULLABLE, SYSTEM
        for flg in printable_flags:
            if flg & set_flags == flg:
                flags.append(FIELD_FLAGS[flg])
                set_flags &= 255 ^ flg
        if flags:
            flags = ' ' + ' '.join(flags)
        else:
            flags = ''
        if type in self._fixed_types:
            description = "%s %s%s" % (name, type, flags)
        elif type in self._numeric_types:
            description = "%s %s(%d,%d)%s" % (name, type, length, decimals, flags)
        else:
            description = "%s %s(%d)%s" % (name, type, length, flags)
        return description
    def _list_fields(self, specs, sep=','):
        "standardizes field specs"
        if specs is None:
            specs = self.field_names
        elif isinstance(specs, str):
            specs = specs.split(sep)
        else:
            specs = list(specs)
        specs = [s.strip() for s in specs]
        return specs
    def _nav_check(self):
        "raises error if table is closed"
        if self._meta.status == CLOSED:
            raise DbfError('table %s is closed' % self.filename)
    @staticmethod
    def _pack_date(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-1900, date.month, date.day)
    @staticmethod
    def _unpack_date(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 1900
            return Date(year, month, day)
    def _update_disk(self, headeronly=False):
        "synchronizes the disk file with current data"
        if self._meta.location == IN_MEMORY:
            return
        meta = self._meta
        header = meta.header
        fd = meta.dfd
        fd.seek(0)
        fd.write(header.data)
        eof = header.start + header.record_count * header.record_length
        if not headeronly:
            for record in self:
                record._update_disk()
            fd.flush()
            fd.truncate(eof)
        if self._versionabbr in ('db3', 'clp'):
            fd.seek(0, SEEK_END)
            fd.write('\x1a')        # required for dBase III compatibility
            fd.flush()
            fd.truncate(eof + 1)

    def __contains__(self, data):
        "data can be a record, template, dict, or tuple"
        if not isinstance(data, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(data))
        for record in Iter(self):
            if data == record:
                return True
        return False
    def __enter__(self):
        self.open()
        return self
    def __exit__(self, *exc_info):
        self.close()
    def __getattr__(self, name):
        if name in (
                'binary_types',
                'character_types',
                'currency_types',
                'date_types',
                'datetime_types',
                'decimal_types',
                'fixed_types',
                'logical_types',
                'memo_types',
                'numeric_types',
                'variable_types',
                ):
            return getattr(self, '_'+name)
        if name in ('_table'):
                if self._meta.location == ON_DISK:
                    self._table = self._Table(len(self), self._meta)
                else:
                    self._table = []
        return object.__getattribute__(self, name)
    def __getitem__(self, value):
        if isinstance(value, (int, long)):
            if not -self._meta.header.record_count <= value < self._meta.header.record_count: 
                raise IndexError("Record %d is not in table." % value)
            return self._table[value]
        elif type(value) == slice:
            sequence = List(desc='%s -->  %s' % (self.filename, value))
            for index in range(len(self))[value]:
                record = self._table[index]
                sequence.append(record)
            return sequence
        else:
            raise TypeError('type <%s> not valid for indexing' % type(value))
    def __init__(self, filename, field_specs=None, memo_size=128, ignore_memos=False, 
                 codepage=None, default_data_types=None, field_data_types=None,    # e.g. 'name':str, 'age':float
                 dbf_type=None,
                 ):
        """open/create dbf file
        filename should include path if needed
        field_specs can be either a ;-delimited string or a list of strings
        memo_size is always 512 for db3 memos
        ignore_memos is useful if the memo file is missing or corrupt
        read_only will load records into memory, then close the disk file
        keep_memos will also load any memo fields into memory
        meta_only will ignore all records, keeping only basic table information
        codepage will override whatever is set in the table itself"""
        if filename[0] == filename[-1] == ':':
            if field_specs is None:
                raise DbfError("field list must be specified for memory tables")
        self._indexen = self._Indexen()
        self._meta = meta = self._MetaData()
        meta.max_fields = self._max_fields
        meta.max_records = self._max_records
        meta.table = weakref.ref(self)
        meta.filename = filename
        meta.fields = []
        meta.user_fields = []
        meta.user_field_count = 0
        meta.fieldtypes = fieldtypes = self._field_types
        meta.fixed_types = self._fixed_types
        meta.variable_types = self._variable_types
        meta.character_types = self._character_types
        meta.currency_types = self._currency_types
        meta.decimal_types = self._decimal_types
        meta.numeric_types = self._numeric_types
        meta.memo_types = self._memo_types
        meta.ignorememos = meta.original_ignorememos = ignore_memos
        meta.memo_size = memo_size
        meta.input_decoder = codecs.getdecoder(input_decoding)      # from ascii to unicode
        meta.output_encoder = codecs.getencoder(input_decoding)     # and back to ascii
        meta.header = header = self._TableHeader(self._dbfTableHeader, self._pack_date, self._unpack_date)
        header.extra = self._dbfTableHeaderExtra
        if default_data_types is None:
            default_data_types = dict()
        self._default_data_types = default_data_types
        if field_data_types is None:
            field_data_types = dict()
        self._field_data_types = field_data_types
        for field, types in default_data_types.items():
            if not isinstance(types, tuple):
                types = (types, )
            for result_name, result_type in ezip(('Class','Empty','Null'), types):
                fieldtypes[field][result_name] = result_type
        if filename[0] == filename[-1] == ':':
            self._table = []
            meta.location = IN_MEMORY
            meta.memoname = filename
            meta.header.data
        else:
            base, ext = os.path.splitext(filename)
            if ext == '':
                meta.filename =  base + '.dbf'
            meta.memoname = base + self._memoext
            meta.location = ON_DISK
        if codepage is not None:
            header.codepage(codepage)
            cp, sd, ld = _codepage_lookup(codepage)
            self._meta.decoder = codecs.getdecoder(sd) 
            self._meta.encoder = codecs.getencoder(sd)
        if field_specs:
            if meta.location == ON_DISK:
                meta.dfd = open(meta.filename, 'w+b')
                meta.newmemofile = True
            if codepage is None:
                header.codepage(default_codepage)
                cp, sd, ld = _codepage_lookup(header.codepage())
                meta.decoder = codecs.getdecoder(sd) 
                meta.encoder = codecs.getencoder(sd)
            meta.status = READ_WRITE
            self.add_fields(field_specs)
        else:
            try:
                dfd = meta.dfd = open(meta.filename, 'r+b')
            except IOError, e:
                raise DbfError(str(e))
            dfd.seek(0)
            meta.header = header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
            if not header.version in self._supported_tables:
                dfd.close()
                dfd = None
                raise DbfError(
                    "%s does not support %s [%x]" % 
                    (self._version,
                    version_map.get(header.version, 'Unknown: %s' % header.version),
                    ord(header.version)))
            if codepage is None:
                cp, sd, ld = _codepage_lookup(header.codepage())
                self._meta.decoder = codecs.getdecoder(sd) 
                self._meta.encoder = codecs.getencoder(sd)
            fieldblock = dfd.read(header.start - 32)
            for i in range(len(fieldblock)//32+1):
                fieldend = i * 32
                if fieldblock[fieldend] == '\x0d':
                    break
            else:
                raise BadDataError("corrupt field structure in header")
            if len(fieldblock[:fieldend]) % 32 != 0:
                raise BadDataError("corrupt field structure in header")
            old_length = header.data[10:12]
            header.fields = fieldblock[:fieldend]
            header.data = header.data[:10] + old_length + header.data[12:]    # restore original for testing
            header.extra = fieldblock[fieldend+1:]  # skip trailing \r
            self._initialize_fields()
            self._check_memo_integrity()
            dfd.seek(0)

        for field in meta.fields:
            field_type = meta[field][TYPE]
            default_field_type = (
                fieldtypes[field_type]['Class'], 
                fieldtypes[field_type]['Empty'], 
                )
            specific_field_type = field_data_types.get(field)
            if specific_field_type is not None and not isinstance(specific_field_type, tuple):
                specific_field_type = (specific_field_type, )
            classes = []
            for result_name, result_type in ezip(
                    ('class','empty'),
                    specific_field_type or default_field_type,
                    ):
                classes.append(result_type)
            meta[field] = meta[field][:-2] + tuple(classes)
        meta.status = READ_ONLY
        self.close()
        
    def __iter__(self):
        "iterates over the table's records"
        return Iter(self)
    def __len__(self):
        "returns number of records in table"
        return self._meta.header.record_count
    def __new__(cls, filename, field_specs=None, memo_size=128, ignore_memos=False, 
                 codepage=None, default_data_types=None, field_data_types=None,    # e.g. 'name':str, 'age':float
                 dbf_type=None,
                 ):
        if dbf_type is None and isinstance(filename, Table):
            return filename
        if field_specs and dbf_type is None:
            dbf_type = default_type
        if dbf_type is not None:
            dbf_type = dbf_type.lower()
            table = table_types.get(dbf_type)
            if table is None:
                raise DbfError("Unknown table type: %s" % dbf_type)
            return object.__new__(table)
        else:
            possibles = guess_table_type(filename)
            if len(possibles) == 1:
                return object.__new__(possibles[0][2])
            else:
                for type, desc, cls in possibles:
                    if type == default_type:
                        return object.__new__(cls)
                else:
                    types = ', '.join(["%s" % item[1] for item in possibles])
                    abbrs = '[' + ' | '.join(["%s" % item[0] for item in possibles]) + ']'
                    raise DbfError("Table could be any of %s.  Please specify %s when opening" % (types, abbrs))

    def __nonzero__(self):
        "True if table has any records"
        return self._meta.header.record_count != 0
    def __repr__(self):
        return __name__ + ".Table(%r, status=%r)" % (self._meta.filename, self._meta.status)
    def __str__(self):
        status = self._meta.status
        str =  """
        Table:         %s
        Type:          %s
        Codepage:      %s
        Status:        %s
        Last updated:  %s
        Record count:  %d
        Field count:   %d
        Record length: %d """ % (self.filename, version_map.get(self._meta.header.version, 
            'unknown - ' + hex(ord(self._meta.header.version))), self.codepage, status, 
            self.last_update, len(self), self.field_count, self.record_length)
        str += "\n        --Fields--\n"
        for i in range(len(self.field_names)):
            str += "%11d) %s\n" % (i, self._field_layout(i))
        return str
    @property
    def codepage(self):
        "code page used for text translation"
        return CodePage(code_pages[self._meta.header.codepage()][0])
    @codepage.setter
    def codepage(self, codepage):
        if not isinstance(codepage, CodePage):
            raise TypeError("codepage should be a CodePage, not a %r" % type(codepage))
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change codepage' % meta.filename)
        meta.header.codepage(codepage.code)
        meta.decoder = codecs.getdecoder(codepage.name) 
        meta.encoder = codecs.getencoder(codepage.name)
        self._update_disk(headeronly=True)
    @property
    def field_count(self):
        "the number of user fields in the table"
        return self._meta.user_field_count
    @property
    def field_names(self):
        "a list of the user fields in the table"
        return self._meta.user_fields[:]
    @property
    def filename(self):
        "table's file name, including path (if specified on open)"
        return self._meta.filename
    @property
    def last_update(self):
        "date of last update"
        return self._meta.header.update
    @property
    def memoname(self):
        "table's memo name (if path included in filename on open)"
        return self._meta.memoname
    @property
    def record_length(self):
        "number of bytes in a record (including deleted flag and null field size"
        return self._meta.header.record_length
    @property
    def supported_tables(self):
        "allowable table types"
        return self._supported_tables
    @property
    def version(self):
        "returns the dbf type of the table"
        return self._version
    def add_fields(self, field_specs):
        """adds field(s) to the table layout; format is Name Type(Length,Decimals)[; Name Type(Length,Decimals)[...]]
        backup table is created with _backup appended to name
        then zaps table, recreates current structure, and copies records back from the backup"""
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to add fields (%s)' % (meta.filename, meta.status))
        header = meta.header
        fields = self.structure() + self._list_fields(field_specs, sep=';')
        if (len(fields) + ('_nullflags' in meta)) > meta.max_fields:
            raise DbfError(
                    "Adding %d more field%s would exceed the limit of %d" 
                    % (len(fields), ('','s')[len(fields)==1], meta.max_fields)
                    )
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        offset = 1
        for name in meta.fields:
            del meta[name]
        meta.fields[:] = []

        meta.blankrecord = None
        for field in fields:
            field = field.lower()
            pieces = field.split()
            name = pieces.pop(0)
            if '(' in pieces[0]:
                loc = pieces[0].index('(')
                pieces.insert(0, pieces[0][:loc])
                pieces[1] = pieces[1][loc:]
            format = pieces.pop(0).upper()
            if pieces and '(' in pieces[0]:
                for i, p in enumerate(pieces):
                    if ')' in p:
                        pieces[0:i+1] = [''.join(pieces[0:i+1])]
                        break
            if name[0] == '_' or name[0].isdigit() or not name.replace('_','').isalnum():
                raise FieldSpecError("%s invalid:  field names must start with a letter, and can only contain letters, digits, and _" % name)
            name = unicode(name)
            if name in meta.fields:
                raise DbfError("Field '%s' already exists" % name)
            field_type = format.encode('ascii')
            if len(name) > 10:
                raise FieldSpecError("Maximum field name length is 10.  '%s' is %d characters long." % (name, len(name)))
            if not field_type in meta.fieldtypes.keys():
                raise FieldSpecError("Unknown field type:  %s" % field_type)
            init = self._meta.fieldtypes[field_type]['Init']
            flags = self._meta.fieldtypes[field_type]['flags']
            length, decimals, flags = init(pieces, flags)
            start = offset
            end = offset + length
            offset = end
            meta.fields.append(name)
            cls = meta.fieldtypes[field_type]['Class']
            empty = meta.fieldtypes[field_type]['Empty']
            meta[name] = (
                    field_type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record))
            old_table.close()

    def allow_nulls(self, fields):
        "set fields to allow null values"
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field types' % meta.filename)
        elif self._versionabbr in ('db3', ):
            raise DbfError("Nullable fields are not allowed in %s tables" % self._version)
        header = meta.header
        fields = self._list_fields(fields)
        missing = set(fields) - set(self.field_names)
        if missing:
            raise FieldMissingError(', '.join(missing))
        if len(self.field_names) + 1 > meta.max_fields:
            raise DbfError(
                    "Adding the hidden _nullflags field would exceed the limit of %d fields for this table" 
                    % (meta.max_fields, )
                    )
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        for field in fields:
            specs = list(meta[field])
            specs[FLAGS] |= NULLABLE
            meta[field] = tuple(specs)
        meta.blankrecord = None
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record))
            old_table.close()

    def append(self, data='', drop=False, multiple=1):
        "adds <multiple> blank records, and fills fields with dict/tuple values if present"
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to append records' % meta.filename)
        if not self.field_count:
            raise DbfError("No fields defined, cannot append")
        empty_table = len(self) == 0
        dictdata = False
        tupledata = False
        header = meta.header
        kamikaze = ''
        if header.record_count == meta.max_records:
            raise DbfError("table %r is full; unable to add any more records" % self)
        if isinstance(data, (Record, RecordTemplate)):
            if data._meta.record_sig == self._meta.record_sig:
                kamikaze = data._data
        else:
            if isinstance(data, dict):
                dictdata = data
                data = ''
            elif isinstance(data, tuple):
                if len(data) > self.field_count:
                    raise DbfError("incoming data has too many values")
                tupledata = data
                data = ''
            elif data:
                raise TypeError("data to append must be a tuple, dict, record, or template; not a %r" % type(data))
        newrecord = Record(recnum=header.record_count, layout=meta, kamikaze=kamikaze)
        if kamikaze and meta.memofields:
            newrecord._start_flux()
            for field in meta.memofields:
                newrecord[field] = data[field]
            newrecord._commit_flux()

        self._table.append(newrecord)
        header.record_count += 1
        if not kamikaze:
            try:
                if dictdata:
                    gather(newrecord, dictdata, drop=drop)
                elif tupledata:
                    newrecord._start_flux()
                    for index, item in enumerate(tupledata):
                        newrecord[index] = item
                    newrecord._commit_flux()
                elif data:
                    newrecord._start_flux()
                    data_fields = field_names(data)
                    my_fields = self.field_names
                    for field in data_fields:
                        if field not in my_fields:
                            if not drop:
                                raise DbfError("field %r not in table %r" % (field, self))
                        else:
                            newrecord[field] = data[field]
                    newrecord._commit_flux()
            except Exception:
                self._table.pop()     # discard failed record
                header.record_count = header.record_count - 1
                self._update_disk()
                raise
        multiple -= 1
        if multiple:
            data = newrecord._data
            single = header.record_count
            total = single + multiple
            while single < total:
                multi_record = Record(single, meta, kamikaze=data)
                multi_record._start_flux()
                self._table.append(multi_record)
                for field in meta.memofields:
                    multi_record[field] = newrecord[field]
                single += 1
                multi_record._commit_flux()
            header.record_count = total   # += multiple
            newrecord = multi_record
        self._update_disk(headeronly=True)
    def close(self):
        """closes disk files, flushing record data to disk
        ensures table data is available if keep_table
        ensures memo data is available if keep_memos"""
        if self._meta.location == ON_DISK and self._meta.status != CLOSED:
            self._table.flush()
            if self._meta.mfd is not None:
                self._meta.mfd.close()
                self._meta.mfd = None
            self._meta.dfd.close()
            self._meta.dfd = None
        self._meta.status = CLOSED
    def create_backup(self, new_name=None):
        "creates a backup table"
        meta = self._meta
        already_open = meta.status != CLOSED
        if not already_open:
            self.open()
        if self.filename[0] == self.filename[-1] == ':' and new_name is None:
            new_name = self.filename[:-1] + '_backup:'
        if new_name is None:
            upper = self.filename.isupper()
            name, ext = os.path.splitext(os.path.split(self.filename)[1])
            extra = ('_backup', '_BACKUP')[upper]
            new_name = os.path.join(temp_dir, name + extra + ext)
        bkup = Table(new_name, self.structure(), codepage=self.codepage.name, dbf_type=self._versionabbr)
        bkup.open()
        for record in self:
            bkup.append(record)
        bkup.close()
        self.backup = new_name
        if not already_open:
            self.close()
        return bkup
    def create_index(self, key):
        "creates an in-memory index using the function key"
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        return Index(self, key)
    def create_template(self, record=None, defaults=None):
        "returns a record template that can be used like a record"
        return RecordTemplate(self._meta, original_record=record, defaults=defaults)
    def delete_fields(self, doomed):
        """removes field(s) from the table
        creates backup files with _backup appended to the file name,
        then modifies current structure"""
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to delete fields' % meta.filename)
        doomed = self._list_fields(doomed)
        header = meta.header
        for victim in doomed:
            if victim not in meta.user_fields:
                raise DbfError("field %s not in table -- delete aborted" % victim)
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        if '_nullflags' in meta.fields:
            doomed.append('_nullflags')
        for victim in doomed:
            layout = meta[victim]
            meta.fields.pop(meta.fields.index(victim))
            start = layout[START]
            end = layout[END]
            for field in meta.fields:
                if meta[field][START] == end:
                    specs = list(meta[field])
                    end = specs[END]                    #self._meta[field][END]
                    specs[START] = start                #self._meta[field][START] = start
                    specs[END] = start + specs[LENGTH]  #self._meta[field][END] = start + self._meta[field][LENGTH]
                    start = specs[END]                  #self._meta[field][END]
                    meta[field] =  tuple(specs)
        self._build_header_fields()
        self._update_disk()
        for name in list(meta):
            if name not in meta.fields:
                del meta[name]
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record), drop=True)
            old_table.close()

    def disallow_nulls(self, fields):
        "set fields to not allow null values"
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field types' % meta.filename)
        fields = self._list_fields(fields)
        missing = set(fields) - set(self.field_names)
        if missing:
            raise FieldMissingError(', '.join(missing))
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        for field in fields:
            specs = list(meta[field])
            specs[FLAGS] &= 0xff ^ NULLABLE
            meta[field] =  tuple(specs)
        meta.blankrecord = None
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record))
            old_table.close()

    def field_info(self, field):
        "returns (dbf type, class, size, dec) of field"
        if field in self.field_names:
            field = self._meta[field]
            return FieldInfo(field[TYPE], field[LENGTH], field[DECIMALS], field[CLASS])
        raise FieldMissingError("%s is not a field in %s" % (field, self.filename))
    def index(self, record, start=None, stop=None):
        """returns the index of record between start and stop
        start and stop default to the first and last record"""
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.Table.index(x): x not in table", data=record)

    def new(self, filename, field_specs=None, memo_size=None, ignore_memos=None, codepage=None, default_data_types=None, field_data_types=None):
        "returns a new table of the same type"
        if field_specs is None:
            field_specs = self.structure()
        if not (filename[0] == filename[-1] == ':'):
            path, name = os.path.split(filename)
            if path == "":
                filename = os.path.join(os.path.split(self.filename)[0], filename)
            elif name == "":
                filename = os.path.join(path, os.path.split(self.filename)[1])
        if memo_size is None:
            memo_size = self._meta.memo_size
        if ignore_memos is None:
            ignore_memos = self._meta.ignorememos
        if codepage is None:
            codepage = self._meta.header.codepage()[0]
        if default_data_types is None:
            default_data_types = self._default_data_types
        if field_data_types is None:
            field_data_types = self._field_data_types
        return Table(filename, field_specs, memo_size, ignore_memos, codepage, default_data_types, field_data_types, dbf_type=self._versionabbr)
    def nullable_field(self, field):
        "returns True if field allows Nulls"
        if field not in self.field_names:
            raise MissingField(field)
        return bool(self._meta[field][FLAGS] & NULLABLE)
    def open(self, mode=READ_WRITE):
        "(re)opens disk table, (re)initializes data structures"
        if mode not in (READ_WRITE, READ_ONLY):
            raise DbfError("mode for open must be 'read-write' or 'read-only', not %r" % mode)
        meta = self._meta
        if meta.status == mode:
            return self     # no-op
        meta.status = mode
        if meta.location == IN_MEMORY:
            return self
        if '_table' in dir(self):
            del self._table
        dfd = meta.dfd = open(meta.filename, 'r+b')
        dfd.seek(0)
        header = meta.header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
        if not header.version in self._supported_tables:
            dfd.close()
            dfd = None
            raise DbfError("Unsupported dbf type: %s [%x]" % (version_map.get(header.version, 'Unknown: %s' % header.version), ord(header.version)))
        fieldblock = dfd.read(header.start - 32)
        for i in range(len(fieldblock)//32+1):
            fieldend = i * 32
            if fieldblock[fieldend] == '\x0d':
                break
        else:
            raise BadDataError("corrupt field structure in header")
        if len(fieldblock[:fieldend]) % 32 != 0:
            raise BadDataError("corrupt field structure in header")
        header.fields = fieldblock[:fieldend]
        header.extra = fieldblock[fieldend+1:]  # skip trailing \r
        self._meta.ignorememos = self._meta.original_ignorememos
        self._initialize_fields()
        self._check_memo_integrity()
        self._index = -1
        dfd.seek(0)
        return self
    def pack(self):
        "physically removes all deleted records"
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to pack records' % meta.filename)
        for dbfindex in self._indexen:
            dbfindex._clear()
        newtable = []
        index = 0
        for record in self._table:
            if is_deleted(record):
                record._recnum = -1
            else:
                record._recnum = index
                newtable.append(record)
                index += 1
        if meta.location == ON_DISK:
            self._table.clear()
        else:
            self._table[:] = []
        for record in newtable:
            self._table.append(record)
        self._pack_count += 1
        self._meta.header.record_count = index
        self._index = -1
        self._update_disk()
        self.reindex()

    def query(self, criteria):
        """criteria is a string that will be converted into a function that returns a List of all matching records"""
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        return pql(self, criteria)

    def reindex(self):
        "reprocess all indices for this table"
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        for dbfindex in self._indexen:
            dbfindex._reindex()
    def rename_field(self, oldname, newname):
        "renames an existing field"
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field names' % meta.filename)
        if self:
            self.create_backup()
        if not oldname in self._meta.user_fields:
            raise FieldMissingError("field --%s-- does not exist -- cannot rename it." % oldname)
        if newname[0] == '_' or newname[0].isdigit() or not newname.replace('_','').isalnum():
            raise FieldSpecError("field names cannot start with _ or digits, and can only contain the _, letters, and digits")
        newname = newname.lower()
        if newname in self._meta.fields:
            raise DbfError("field --%s-- already exists" % newname)
        if len(newname) > 10:
            raise FieldSpecError("maximum field name length is 10.  '%s' is %d characters long." % (newname, len(newname)))
        self._meta[newname] = self._meta[oldname]
        self._meta.fields[self._meta.fields.index(oldname)] = newname
        self._build_header_fields()
        self._update_disk(headeronly=True)
    def resize_field(self, chosen, new_size):
        """resizes field (C only at this time)
        creates backup file, then modifies current structure"""
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field size' % meta.filename)
        if not 0 < new_size < 256:
            raise DbfError("new_size must be between 1 and 255 (use delete_fields to remove a field)")
        chosen = self._list_fields(chosen)
        for candidate in chosen:
            if candidate not in self._meta.user_fields:
                raise DbfError("field %s not in table -- resize aborted" % candidate)
            elif self.field_info(candidate).dbf_type != 'C':
                raise DbfError("field %s is not Character -- resize aborted" % candidate)
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        struct = self.structure()
        meta.user_fields[:] = []
        new_struct = []
        for field_spec in struct:
            name, spec = field_spec.split(' ', 1)
            if name in chosen:
                spec = "C(%d)" % new_size
            new_struct.append(' '.join([name, spec]))
        self.add_fields(';'.join(new_struct))
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record), drop=True)
            old_table.close()
    def structure(self, fields=None):
        """return field specification list suitable for creating same table layout
        fields should be a list of fields or None for all fields in table"""
        field_specs = []
        fields = self._list_fields(fields)
        try:
            for name in fields:
                field_specs.append(self._field_layout(self.field_names.index(name)))
        except ValueError:
            raise DbfError("field %s does not exist" % name)
        return field_specs
    def zap(self):
        """removes all records from table -- this cannot be undone!"""
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to zap table' % meta.filename)
        if meta.location == IN_MEMORY:
            self._table[:] = []
        else:
            self._table.clear()
            if meta.memo:
                meta.memo._zap()
        meta.header.record_count = 0
        self._index = -1
        self._update_disk()
class Db3Table(Table):
    """Provides an interface for working with dBase III tables."""
    _version = 'dBase III Plus'
    _versionabbr = 'db3'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: ' '*x, 'Init':add_character,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: '        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            'F' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: '?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: '          ', 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    } }
    _memoext = '.dbt'
    _memoClass = _Db3Memo
    _yesMemoMask = '\x80'
    _noMemoMask = '\x7f'
    _binary_types = ()
    _character_types = ('C','M') 
    _currency_types = tuple()
    _date_types = ('D',)
    _datetime_types = tuple()
    _decimal_types = ('N', 'F')
    _fixed_types = ('D','L','M')
    _logical_types = ('L',)
    _memo_types = ('M',)
    _numeric_types = ('N', 'F')
    _variable_types = ('C','N')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x03'         # version - dBase III w/o memo's
    _dbfTableHeader[8:10] = array('c', pack_short_int(33))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = ['\x03', '\x83']

    def _check_memo_integrity(self):
        "dBase III and Clipper"
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memo_types:
                    memo_fields = True
                    break
            if memo_fields and self._meta.header.version != '\x83':
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist, header declares no memos")
            elif memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception, exc:
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise BadDataError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        meta = self._meta
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['empty'] = meta[name][EMPTY]
            old_fields[name]['class'] = meta[name][CLASS]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError("Header shows %d fields, but field definition block has %d fields" % (meta.header.field_count, len(fieldsdef)//32))
        total_length = meta.header.record_length
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
            meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = meta.fieldtypes[type]['Class']
                empty = meta.fieldtypes[type]['Empty']
            meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)
class ClpTable(Db3Table):
    """Provides an interface for working with Clipper tables."""
    _version = 'Clipper 5'
    _versionabbr = 'clp'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: ' '*x, 'Init':add_clp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: '        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            'F' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: '?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: '          ', 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    } }
    _memoext = '.dbt'
    _memoClass = _Db3Memo
    _yesMemoMask = '\x80'
    _noMemoMask = '\x7f'
    _binary_types = ()
    _character_types = ('C','M') 
    _currency_types = tuple()
    _date_types = ('D',)
    _datetime_types = tuple()
    _decimal_types = ('N', 'F')
    _fixed_types = ('D','L','M')
    _logical_types = ('L',)
    _memo_types = ('M',)
    _numeric_types = ('N', 'F')
    _variable_types = ('C','N')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x03'         # version - dBase III w/o memo's
    _dbfTableHeader[8:10] = array('c', pack_short_int(33))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = ['\x03', '\x83']

    def _build_header_fields(self):
        "constructs fieldblock for disk table"
        fieldblock = array('c', '')
        memo = False
        nulls = False
        meta = self._meta
        header = meta.header
        header.version = chr(ord(header.version) & ord(self._noMemoMask))
        meta.fields = [f for f in meta.fields if f != '_nullflags']
        total_length = 1    # delete flag
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise BadDataError("corrupted field structure (noticed in _build_header_fields)")
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str(meta.encoder(field)[0]))
            fielddef[11] = layout[TYPE]
            fielddef[12:16] = array('c', pack_long_int(layout[START]))
            total_length += layout[LENGTH]
            if layout[TYPE] == 'C':     # long character field
                fielddef[16] = chr(layout[LENGTH] % 256)
                fielddef[17] = chr(layout[LENGTH] // 256)
            else:
                fielddef[16] = chr(layout[LENGTH])
                fielddef[17] = chr(layout[DECIMALS])
            fielddef[18] = chr(layout[FLAGS])
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memo_types:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls = True
        if memo:
            header.version = chr(ord(header.version) | ord(self._yesMemoMask))
            if meta.memo is None:
                meta.memo = self._memoClass(meta)
        else:
            if os.path.exists(meta.memoname):
                if meta.mfd is not None:
                    meta.mfd.close()

                os.remove(meta.memoname)
            meta.memo = None
        if nulls:
            start = layout[START] + layout[LENGTH]
            length, one_more = divmod(len(meta.fields), 8)
            if one_more:
                length += 1
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str('_nullflags'))
            fielddef[11] = '0'
            fielddef[12:16] = array('c', pack_long_int(start))
            fielddef[16] = chr(length)
            fielddef[17] = chr(0)
            fielddef[18] = chr(BINARY | SYSTEM)
            fieldblock.extend(fielddef)
            meta.fields.append('_nullflags')
            nullflags = (
                    '0',                # type
                    start,              # start
                    length,             # length
                    start + length,     # end
                    0,                  # decimals
                    BINARY | SYSTEM,    # flags
                    none,               # class
                    none,               # empty
                    )
            meta['_nullflags'] = nullflags
        header.fields = fieldblock.tostring()
        header.record_length = total_length
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)

    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['empty'] = meta[name][EMPTY]
            old_fields[name]['class'] = meta[name][CLASS]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError("Header shows %d fields, but field definition block has %d fields" % (meta.header.field_count, len(fieldsdef)//32))
        total_length = meta.header.record_length
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            decimals = ord(fieldblock[17])
            if type == 'C':
                length += decimals * 256
            offset += length
            end = start + length
            flags = ord(fieldblock[18])
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
            meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = meta.fieldtypes[type]['Class']
                empty = meta.fieldtypes[type]['Empty']
            meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)
class FpTable(Table):
    'Provides an interface for working with FoxPro 2 tables'
    _version = 'Foxpro'
    _versionabbr = 'fp'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: ' '*x, 'Init':add_vfp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: '?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: '        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':lambda x: '\x00'*x, 'Init':None,
                    'Class':none, 'Empty':none, 'flags':('binary','system', ),
                    } }
    _memoext = '.fpt'
    _memoClass = _VfpMemo
    _yesMemoMask = '\xf5'               # 1111 0101
    _noMemoMask = '\x03'                # 0000 0011
    _binary_types = ('G','P')
    _character_types = ('C','D','F','L','M','N')       # field representing character data
    _currency_types = tuple()
    _date_types = ('D',)
    _datetime_types = tuple()
    _fixed_types = ('D','G','L','M','P')
    _logical_types = ('L',)
    _memo_types = ('G','M','P')
    _numeric_types = ('F','N')
    _text_types = ('C','M')
    _variable_types = ('C','F','N')
    _supported_tables = ('\x03', '\xf5')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x30'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[8:10] = array('c', pack_short_int(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    def _check_memo_integrity(self):
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memo_types:
                    memo_fields = True
                    break
            if memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
            elif not memo_fields and os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  no memo fields exist but memo file does")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception, exc:
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise BadDataError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError("Header shows %d fields, but field definition block has %d fields" % (meta.header.field_count, len(fieldsdef)//32))
        total_length = meta.header.record_length
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
            meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = meta.fieldtypes[type]['Class']
                empty = meta.fieldtypes[type]['Empty']
            meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)
    @staticmethod
    def _pack_date(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-2000, date.month, date.day)
    @staticmethod
    def _unpack_date(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 2000
            return Date(year, month, day)
            
class VfpTable(FpTable):
    'Provides an interface for working with Visual FoxPro 6 tables'
    _version = 'Visual Foxpro'
    _versionabbr = 'vfp'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: ' '*x, 'Init':add_vfp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'Y' : {
                    'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':lambda x: '\x00'*8, 'Init':add_vfp_currency,
                    'Class':Decimal, 'Empty':none, 'flags':('null', ),
                    },
            'B' : {
                    'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':lambda x: '\x00'*8, 'Init':add_vfp_double,
                    'Class':float, 'Empty':none, 'flags':('null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: ' '*x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'I' : {
                    'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':lambda x: '\x00'*4, 'Init':add_vfp_integer,
                    'Class':int, 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: '?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: '        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'T' : {
                    'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':lambda x: '\x00'*8, 'Init':add_vfp_datetime,
                    'Class':datetime.datetime, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: '\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':lambda x: '\x00'*x, 'Init':int,
                    'Class':none, 'Empty':none, 'flags':('binary','system',),
                    } }
    _memoext = '.fpt'
    _memoClass = _VfpMemo
    _yesMemoMask = '\x30'               # 0011 0000
    _noMemoMask = '\x30'                # 0011 0000
    _binary_types = ('B','G','I','P','T','Y')
    _character_types = ('C','D','F','L','M','N')       # field representing character data
    _currency_types = ('Y',)
    _date_types = ('D','T')
    _datetime_types = ('T',)
    _fixed_types = ('B','D','G','I','L','M','P','T','Y')
    _logical_types = ('L',)
    _memo_types = ('G','M','P')
    _numeric_types = ('B','F','I','N','Y')
    _variable_types = ('C','F','N')
    _supported_tables = ('\x30','\x31')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x30'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[8:10] = array('c', pack_short_int(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
            old_fields[name]['empty'] = meta[name][EMPTY]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        meta.nullflags = None
        total_length = meta.header.record_length
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = unpack_long_int(fieldblock[12:16])
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
            meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = meta.fieldtypes[type]['Class']
                empty = meta.fieldtypes[type]['Empty']
            meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)
                    
class List(_Navigation):
    "list of Dbf records, with set-like behavior"
    _desc = ''
    def __init__(self, records=None, desc=None, key=None):
        self._list = []
        self._set = set()
        self._tables = dict()
        if key is not None:
            self.key = key
            if key.__doc__ is None:
                key.__doc__ = 'unknown'
        key = self.key
        self._current = -1
        if isinstance(records, self.__class__) and key is records.key:
                self._list = records._list[:]
                self._set = records._set.copy()
                self._current = 0
        elif records is not None:
            for record in records:
                value = key(record)
                item = (source_table(record), recno(record), value)
                if value not in self._set:
                    self._set.add(value)
                    self._list.append(item)
            self._current = 0
        if desc is not None:
            self._desc = desc
    def __add__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = self.__class__()
            result._set = self._set.copy()
            result._list[:] = self._list[:]
            result._tables = {}
            result._tables.update(self._tables)
            result.key = self.key
            if key is other.key:   # same key?  just compare key values
                for item in other._list:
                    result._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in other:
                    result._maybe_add((source_table(rec), recno(rec), key(rec)))
            return result
        return NotImplemented
    def __contains__(self, data):
        self._still_valid_check()
        if not isinstance(data, (Record, RecordTemplate, tuple, dict)):
            raise TypeError("%r is not a record, templace, tuple, nor dict" % (data, ))
        try:    # attempt quick method
            item = self.key(data)
            if not isinstance(item, tuple):
                item = (item, )
            return item in self._set
        except Exception:       # argh, try brute force method
            for record in self:
                if record == data:
                    return True
            return False

    def __delitem__(self, key):
        self._still_valid_check()
        if isinstance(key, int):
            item = self._list.pop[key]
            self._set.remove(item[2])
        elif isinstance(key, slice):
            self._set.difference_update([item[2] for item in self._list[key]])
            self._list.__delitem__(key)
        elif isinstance(key, (Record, RecordTemplate, dict, tuple)):
            index = self.index(key)
            item = self._list.pop[index]
            self._set.remove(item[2])
        else:
            raise TypeError('%r should be an int, slice, record, template, tuple, or dict -- not a %r' % (key, type(key)))
    def __getitem__(self, key):
        self._still_valid_check()
        if isinstance(key, int):
            count = len(self._list)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            return self._get_record(*self._list[key])
        elif isinstance(key, slice):
            result = self.__class__()
            result._list[:] = self._list[key]
            result._set = set(result._list)
            result.key = self.key
            return result
        elif isinstance(key, (Record, RecordTemplate, dict, tuple)):
            index = self.index(key)
            return self._get_record(*self._list[index])
        else:
            raise TypeError('%r should be an int, slice, record, record template, tuple, or dict -- not a %r' % (key, type(key)))
    def __iter__(self):
        self._still_valid_check()
        return Iter(self)
    def __len__(self):
        self._still_valid_check()
        return len(self._list)
    def __nonzero__(self):
        self._still_valid_check()
        return len(self) > 0
    def __radd__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = other.__class__()
            result._set = other._set.copy()
            result._list[:] = other._list[:]
            result._tables = {}
            result._tables.update(self._tables)
            result.key = other.key
            if key is other.key:   # same key?  just compare key values
                for item in self._list:
                    result._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in self:
                    result._maybe_add((source_table(rec), recno(rec), key(rec)))
            return result
        return NotImplemented
    def __repr__(self):
        self._still_valid_check()
        if self._desc:
            return "%s(key=(%s), decs=%s)" % (self.__class__, self.key.__doc__, self._desc)
        else:
            return "%s(key=(%s))" % (self.__class__, self.key.__doc__)
    def __rsub__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = other.__class__()
            result._list[:] = other._list[:]
            result._set = other._set.copy()
            result._tables = {}
            result._tables.update(other._tables)
            result.key = key
            lost = set()
            if key is other.key:
                for item in self._list:
                    if item[2] in result._list:
                        result._set.remove(item[2])
                        lost.add(item)
            else:
                for rec in self:
                    value = key(rec)
                    if value in result._set:
                        result._set.remove(value)
                        lost.add((source_table(rec), recno(rec), value))
            result._list = [item for item in result._list if item not in lost]
            lost = set(result._tables.keys())
            for table, _1, _2 in result._list:
                if table in result._tables:
                    lost.remove(table)
                    if not lost:
                        break
            for table in lost:
                del result._tables[table]
            return result
        return NotImplemented
    def __sub__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = self.__class__()
            result._list[:] = self._list[:]
            result._set = self._set.copy()
            result._tables = {}
            result._tables.update(self._tables)
            result.key = key
            lost = set()
            if key is other.key:
                for item in other._list:
                    if item[2] in result._set:
                        result._set.remove(item[2])
                        lost.add(item[2])
            else:
                for rec in other:
                    value = key(rec)
                    if value in result._set:
                        result._set.remove(value)
                        lost.add(value)
            result._list = [item for item in result._list if item[2] not in lost]
            lost = set(result._tables.keys())
            for table, _1, _2 in result._list:
                if table in result._tables:
                    lost.remove(table)
                    if not lost:
                        break
            for table in lost:
                del result._tables[table]
            return result
        return NotImplemented
    def _maybe_add(self, item):
        self._still_valid_check()
        table, recno, key = item
        self._tables[table] = table._pack_count
        if key not in self._set:
            self._set.add(key)
            self._list.append(item)
    def _get_record(self, table=None, rec_no=None, value=None):
        if table is rec_no is None:
            table, rec_no, value = self._list[self._index]
        return table[rec_no]
    def _purge(self, record, old_record_number, offset):
        partial = source_table(record), old_record_number
        records = sorted(self._list, key=lambda item: (item[0], item[1]))
        for item in records:
            if partial == item[:2]:
                found = True
                break
            elif partial[0] is item[0] and partial[1] < item[1]:
                found = False
                break
        else:
            found = False
        if found:
            self._list.pop(self._list.index(item))
            self._set.remove(item[2])
        start = records.index(item) + found
        for item in records[start:]:
            if item[0] is not partial[0]:       # into other table's records
                break
            i = self._list.index(item)
            self._set.remove(item[2])
            item = item[0], (item[1] - offset), item[2]
            self._list[i] = item
            self._set.add(item[2])
        return found
    def _still_valid_check(self):
        for table, last_pack in self._tables.items():
            if last_pack != getattr(table, '_pack_count'):
                raise DbfError("table has been packed; list is invalid")
    _nav_check = _still_valid_check
    def append(self, record):
        self._still_valid_check()
        self._maybe_add((source_table(record), recno(record), self.key(record)))
    def clear(self):
        self._list = []
        self._set = set()
        self._index = -1
        self._tables.clear()
    def extend(self, records):
        self._still_valid_check()
        key = self.key
        if isinstance(records, self.__class__):
            if key is records.key:   # same key?  just compare key values
                for item in records._list:
                    self._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in records:
                    value = key(rec)
                    self._maybe_add((source_table(rec), recno(rec), value))
        else:
            for rec in records:
                value = key(rec)
                self._maybe_add((source_table(rec), recno(rec), value))
    def index(self, record, start=None, stop=None):
        """returns the index of record between start and stop
        start and stop default to the first and last record"""
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        self._still_valid_check()
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.List.index(x): x not in List", data=record)
    def insert(self, i, record):
        self._still_valid_check()
        item = source_table(record), recno(record), self.key(record)
        if item not in self._set:
            self._set.add(item[2])
            self._list.insert(i, item)
    def key(self, record):
        "table_name, record_number"
        self._still_valid_check()
        return source_table(record), recno(record)
    def pop(self, index=None):
        self._still_valid_check()
        if index is None:
            table, recno, value = self._list.pop()
        else:
            table, recno, value = self._list.pop(index)
        self._set.remove(value)
        return self._get_record(table, recno, value)
    def query(self, criteria):
        """criteria is a callback that returns a truthy value for matching record"""
        return pql(self, criteria)

    def remove(self, data):
        self._still_valid_check()
        if not isinstance(data, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("%r(%r) is not a record, template, tuple, nor dict" % (type(data), data))
        index = self.index(data)
        record = self[index]
        item = source_table(record), recno(record), self.key(record)
        self._list.remove(item)
        self._set.remove(item[2])
    def reverse(self):
        self._still_valid_check()
        return self._list.reverse()
    def sort(self, key=None, reverse=False):
        self._still_valid_check()
        if key is None:
            return self._list.sort(reverse=reverse)
        return self._list.sort(key=lambda item: key(item[0][item[1]]), reverse=reverse)

class Index(_Navigation):
    def __init__(self, table, key):
        self._table = table
        self._values = []             # ordered list of values
        self._rec_by_val = []         # matching record numbers
        self._records = {}            # record numbers:values
        self.__doc__ = key.__doc__ or 'unknown'
        self.key = key
        for record in table:
            value = key(record)
            if value is DoNotIndex:
                continue
            rec_num = recno(record)
            if not isinstance(value, tuple):
                value = (value, )
            vindex = bisect_right(self._values, value)
            self._values.insert(vindex, value)
            self._rec_by_val.insert(vindex, rec_num)
            self._records[rec_num] = value
        table._indexen.add(self)
    def __call__(self, record):
        rec_num = recno(record)
        key = self.key(record)
        if not isinstance(key, tuple):
            key = (key, )
        if rec_num in self._records:
            if self._records[rec_num] == key:
                return
            old_key = self._records[rec_num]
            vindex = bisect_left(self._values, old_key)
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
            del self._records[rec_num]
            assert rec_num not in self._records
        if key == (DoNotIndex, ):
            return
        vindex = bisect_right(self._values, key)
        self._values.insert(vindex, key)
        self._rec_by_val.insert(vindex, rec_num)
        self._records[rec_num] = key
    def __contains__(self, data):
        if not isinstance(data,(Record, RecordTemplate, tuple, dict)):
            raise TypeError("%r is not a record, templace, tuple, nor dict" % (data, ))
        if isinstance(data, Record) and source_table(data) is self._table:
            return recno(data) in self._records
        else:
            try:
                value = self.key(data)
                return value in self._values
            except Exception:
                for record in self:
                    if record == data:
                        return True
                return False
    def __getitem__(self, key):
        if isinstance(key, int):
            count = len(self._values)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            rec_num = self._rec_by_val[key]
            return self._table[rec_num]
        elif isinstance(key, slice):
            result = List()
            start, stop, step = key.start, key.stop, key.step
            if start is None: start = 0
            if stop is None: stop = len(self._rec_by_val)
            if step is None: step = 1
            if step < 0:
                start, stop = stop - 1, -(stop - start + 1)
            for loc in range(start, stop, step):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            return result
        elif isinstance (key, (str, unicode, tuple, Record)):
            if isinstance(key, Record):
                key = self.key(key)
            elif not isinstance(key, tuple):
                key = (key, )
            loc = self.find(key)
            if loc == -1:
                raise KeyError(key)
            return self._table[self._rec_by_val[loc]]
        else:
            raise TypeError('indices must be integers, match objects must by strings or tuples')
    def __enter__(self):
        return self
    def __exit__(self, *exc_info):
        self._table.close()
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()
        return False
    def __iter__(self):
        return Iter(self)
    def __len__(self):
        return len(self._records)
    def _clear(self):
        "removes all entries from index"
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()
    def _nav_check(self):
        "raises error if table is closed"
        if self._table._meta.status == CLOSED:
            raise DbfError('indexed table %s is closed' % self.filename)
    def _partial_match(self, target, match):
        target = target[:len(match)]
        if isinstance(match[-1], (str, unicode)):
            target = list(target)
            target[-1] = target[-1][:len(match[-1])]
            target = tuple(target)
        return target == match
    def _purge(self, rec_num):
        value = self._records.get(rec_num)
        if value is not None:
            vindex = bisect_left(self._values, value)
            del self._records[rec_num]
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
    def _reindex(self):
        "reindexes all records"
        for record in self._table:
            self(record)
    def _search(self, match, lo=0, hi=None):
        if hi is None:
            hi = len(self._values)
        return bisect_left(self._values, match, lo, hi)
    def index(self, record, start=None, stop=None):
        """returns the index of record between start and stop
        start and stop default to the first and last record"""
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        self._still_valid_check()
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.Index.index(x): x not in Index", data=record)
    def query(self, criteria):
        """criteria is a callback that returns a truthy value for matching record"""
        return pql(self, criteria)
    def search(self, match, partial=False):
        "returns dbf.List of all (partially) matching records"
        result = List()
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match)
        if loc == len(self._values):
            return result
        while loc < len(self._values) and self._values[loc] == match:
            record = self._table[self._rec_by_val[loc]]
            result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            loc += 1
        if partial:
            while loc < len(self._values) and self._partial_match(self._values[loc], match):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
                loc += 1
        return result

table_types = {
    'db3' : Db3Table,
    'clp' : ClpTable,
    'fp'  : FpTable,
    'vfp' : VfpTable,
    }

version_map = {
        '\x02' : 'FoxBASE',
        '\x03' : 'dBase III Plus',
        '\x04' : 'dBase IV',
        '\x05' : 'dBase V',
        '\x30' : 'Visual FoxPro',
        '\x31' : 'Visual FoxPro (auto increment field)',
        '\x32' : 'Visual FoxPro (VarChar, VarBinary, or BLOB enabled)',
        '\x43' : 'dBase IV SQL table files',
        '\x63' : 'dBase IV SQL system files',
        '\x83' : 'dBase III Plus w/memos',
        '\x8b' : 'dBase IV w/memos',
        '\x8e' : 'dBase IV w/SQL table',
        '\xf5' : 'FoxPro w/memos'}

code_pages = {
        '\x00' : ('ascii', "plain ol' ascii"),
        '\x01' : ('cp437', 'U.S. MS-DOS'),
        '\x02' : ('cp850', 'International MS-DOS'),
        '\x03' : ('cp1252', 'Windows ANSI'),
        '\x04' : ('mac_roman', 'Standard Macintosh'),
        '\x08' : ('cp865', 'Danish OEM'),
        '\x09' : ('cp437', 'Dutch OEM'),
        '\x0A' : ('cp850', 'Dutch OEM (secondary)'),
        '\x0B' : ('cp437', 'Finnish OEM'),
        '\x0D' : ('cp437', 'French OEM'),
        '\x0E' : ('cp850', 'French OEM (secondary)'),
        '\x0F' : ('cp437', 'German OEM'),
        '\x10' : ('cp850', 'German OEM (secondary)'),
        '\x11' : ('cp437', 'Italian OEM'),
        '\x12' : ('cp850', 'Italian OEM (secondary)'),
        '\x13' : ('cp932', 'Japanese Shift-JIS'),
        '\x14' : ('cp850', 'Spanish OEM (secondary)'),
        '\x15' : ('cp437', 'Swedish OEM'),
        '\x16' : ('cp850', 'Swedish OEM (secondary)'),
        '\x17' : ('cp865', 'Norwegian OEM'),
        '\x18' : ('cp437', 'Spanish OEM'),
        '\x19' : ('cp437', 'English OEM (Britain)'),
        '\x1A' : ('cp850', 'English OEM (Britain) (secondary)'),
        '\x1B' : ('cp437', 'English OEM (U.S.)'),
        '\x1C' : ('cp863', 'French OEM (Canada)'),
        '\x1D' : ('cp850', 'French OEM (secondary)'),
        '\x1F' : ('cp852', 'Czech OEM'),
        '\x22' : ('cp852', 'Hungarian OEM'),
        '\x23' : ('cp852', 'Polish OEM'),
        '\x24' : ('cp860', 'Portugese OEM'),
        '\x25' : ('cp850', 'Potugese OEM (secondary)'),
        '\x26' : ('cp866', 'Russian OEM'),
        '\x37' : ('cp850', 'English OEM (U.S.) (secondary)'),
        '\x40' : ('cp852', 'Romanian OEM'),
        '\x4D' : ('cp936', 'Chinese GBK (PRC)'),
        '\x4E' : ('cp949', 'Korean (ANSI/OEM)'),
        '\x4F' : ('cp950', 'Chinese Big 5 (Taiwan)'),
        '\x50' : ('cp874', 'Thai (ANSI/OEM)'),
        '\x57' : ('cp1252', 'ANSI'),
        '\x58' : ('cp1252', 'Western European ANSI'),
        '\x59' : ('cp1252', 'Spanish ANSI'),
        '\x64' : ('cp852', 'Eastern European MS-DOS'),
        '\x65' : ('cp866', 'Russian MS-DOS'),
        '\x66' : ('cp865', 'Nordic MS-DOS'),
        '\x67' : ('cp861', 'Icelandic MS-DOS'),
        '\x68' : (None, 'Kamenicky (Czech) MS-DOS'),
        '\x69' : (None, 'Mazovia (Polish) MS-DOS'),
        '\x6a' : ('cp737', 'Greek MS-DOS (437G)'),
        '\x6b' : ('cp857', 'Turkish MS-DOS'),
        '\x78' : ('cp950', 'Traditional Chinese (Hong Kong SAR, Taiwan) Windows'),
        '\x79' : ('cp949', 'Korean Windows'),
        '\x7a' : ('cp936', 'Chinese Simplified (PRC, Singapore) Windows'),
        '\x7b' : ('cp932', 'Japanese Windows'),
        '\x7c' : ('cp874', 'Thai Windows'),
        '\x7d' : ('cp1255', 'Hebrew Windows'),
        '\x7e' : ('cp1256', 'Arabic Windows'),
        '\xc8' : ('cp1250', 'Eastern European Windows'),
        '\xc9' : ('cp1251', 'Russian Windows'),
        '\xca' : ('cp1254', 'Turkish Windows'),
        '\xcb' : ('cp1253', 'Greek Windows'),
        '\x96' : ('mac_cyrillic', 'Russian Macintosh'),
        '\x97' : ('mac_latin2', 'Macintosh EE'),
        '\x98' : ('mac_greek', 'Greek Macintosh') }

default_codepage = code_pages.get(default_codepage, code_pages.get('\x00'))[0]

# SQL functions
def pql_select(records, chosen_fields, condition, field_names):
    if chosen_fields != '*':
        field_names = chosen_fields.replace(' ','').split(',')
    result = condition(records)
    result.modified = 0, 'record' + ('','s')[len(result)>1]
    result.field_names = field_names
    return result

def pql_update(records, command, condition, field_names):
    possible = condition(records)
    modified = pql_cmd(command, field_names)(possible)
    possible.modified = modified, 'record' + ('','s')[modified>1]
    return possible

def pql_delete(records, dead_fields, condition, field_names):
    deleted = condition(records)
    deleted.modified = len(deleted), 'record' + ('','s')[len(deleted)>1]
    deleted.field_names = field_names
    if dead_fields == '*':
        for record in deleted:
            record.delete_record()
            record.write_record()
    else:
        keep = [f for f in field_names if f not in dead_fields.replace(' ','').split(',')]
        for record in deleted:
            record.reset_record(keep_fields=keep)
            record.write_record()
    return deleted

def pql_recall(records, all_fields, condition, field_names):
    if all_fields != '*':
        raise DbfError('SQL RECALL: fields must be * (only able to recover at the record level)')
    revivified = List()
    for record in condition(records):
        if is_deleted(record):
            revivified.append(record)
            undelete(record)
    revivified.modfied = len(revivified), 'record' + ('','s')[len(revivified)>1]
    return revivified

def pql_add(records, new_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.add_fields(new_fields)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_drop(records, dead_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.delete_fields(dead_fields)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_pack(records, command, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.pack()
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_resize(records, fieldname_newsize, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    fieldname, newsize = fieldname_newsize.split()
    newsize = int(newsize)
    for table in tables:
        table.resize_field(fieldname, newsize)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_criteria(records, criteria):
    "creates a function matching the pql criteria"
    function = """def func(records):
    \"\"\"%s
    \"\"\"
    _matched = dbf.List()
    for _rec in records:
        %s

        if %s:
            _matched.append(_rec)
    return _matched"""
    fields = []
    for field in field_names(records):
        if field in criteria:
            fields.append(field)
    criteria = criteria.replace('recno()','recno(_rec)').replace('is_deleted()','is_deleted(_rec)')
    fields = '\n        '.join(['%s = _rec.%s' % (field, field) for field in fields])
    g = dict()
    g['dbf'] = dbf
    g.update(pql_user_functions)
    function %= (criteria, fields, criteria)
    exec function in g
    return g['func']

def pql_cmd(command, field_names):
    "creates a function matching to apply command to each record in records"
    function = """def func(records):
    \"\"\"%s
    \"\"\"
    _changed = 0
    for _rec in records:
        _tmp = dbf.create_template(_rec)
        %s

        %s

        %s
        if _tmp != _rec:
            dbf.gather(_rec, _tmp)
            _changed += 1
    return _changed"""
    fields = []
    for field in field_names:
        if field in command:
            fields.append(field)
    command = command.replace('recno()','recno(_rec)').replace('is_deleted()','is_deleted(_rec)')
    pre_fields = '\n        '.join(['%s = _tmp.%s' % (field, field) for field in fields])
    post_fields = '\n        '.join(['_tmp.%s = %s' % (field, field) for field in fields])
    g = pql_user_functions.copy()
    g['dbf'] = dbf
    g['recno'] = recno
    g['create_template'] = create_template
    g['gather'] = gather
    if ' with ' in command.lower():
        offset = command.lower().index(' with ')
        command = command[:offset] + ' = ' + command[offset+6:]
    function %= (command, pre_fields, command, post_fields)
    exec function in g
    return g['func']

def pql(records, command):
    """recognized pql commands are SELECT, UPDATE | REPLACE, DELETE, RECALL, ADD, DROP"""
    close_table = False
    if isinstance(records, (str, unicode)):
        records = Table(records)
        close_table = True
    try:
        if not records:
            return List()
        pql_command = command
        if ' where ' in command:
            command, condition = command.split(' where ', 1)
            condition = pql_criteria(records, condition)
        else:
            def condition(records):
                return records[:]
        name, command = command.split(' ', 1)
        command = command.strip()
        name = name.lower()
        fields = field_names(records)
        if pql_functions.get(name) is None:
            raise DbfError('unknown SQL command: %s' % name.upper())
        result = pql_functions[name](records, command, condition, fields)
        tables = set()
        for record in result:
            tables.add(source_table(record))
    finally:
        if close_table:
            records.close()
    return result

pql_functions = {
        'select' : pql_select,
        'update' : pql_update,
        'replace': pql_update,
        'insert' : None,
        'delete' : pql_delete,
        'recall' : pql_recall,
        'add'    : pql_add,
        'drop'   : pql_drop,
        'count'  : None,
        'pack'   : pql_pack,
        'resize' : pql_resize,
        }


def _nop(value):
    "returns parameter unchanged"
    return value
def _normalize_tuples(tuples, length, filler):
    "ensures each tuple is the same length, using filler[-missing] for the gaps"
    final = []
    for t in tuples:
        if len(t) < length:
            final.append( tuple([item for item in t] + filler[len(t)-length:]) )
        else:
            final.append(t)
    return tuple(final)
def _codepage_lookup(cp):
    if cp not in code_pages:
        for code_page in sorted(code_pages.keys()):
            sd, ld = code_pages[code_page]
            if cp == sd or cp == ld:
                if sd is None:
                    raise DbfError("Unsupported codepage: %s" % ld)
                cp = code_page
                break
        else:
            raise DbfError("Unsupported codepage: %s" % cp)
    sd, ld = code_pages[cp]
    return cp, sd, ld
# miscellany

def codepage(cp=None):
    "get/set default codepage for any new tables"
    global default_codepage
    cp, sd, ld = _codepage_lookup(cp or default_codepage)
    default_codepage = sd
    return "%s (LDID: 0x%02x - %s)" % (sd, ord(cp), ld)
def encoding(cp=None):
    "get/set default encoding for non-unicode strings passed into a table"
    global input_decoding
    cp, sd, ld = _codepage_lookup(cp or input_decoding)
    default_codepage = sd
    return "%s (LDID: 0x%02x - %s)" % (sd, ord(cp), ld)
class _Db4Table(Table):
    version = 'dBase IV w/memos (non-functional)'
    _versionabbr = 'db4'
    @MutableDefault
    def _field_types():
        return {
            'C' : {'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':str, 'Init':add_vfp_character},
            'Y' : {'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':Decimal, 'Init':add_vfp_currency},
            'B' : {'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':float, 'Init':add_vfp_double},
            'F' : {'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':float, 'Init':add_vfp_numeric},
            'N' : {'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_vfp_numeric},
            'I' : {'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':int, 'Init':add_vfp_integer},
            'L' : {'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical},
            'D' : {'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date},
            'T' : {'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':DateTime, 'Init':add_vfp_datetime},
            'M' : {'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            'G' : {'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            'P' : {'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            '0' : {'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':int, 'Init':None} }
    _memoext = '.dbt'
    _memotypes = ('G','M','P')
    _memoClass = _VfpMemo
    _yesMemoMask = '\x8b'               # 0011 0000
    _noMemoMask = '\x04'                # 0011 0000
    _fixed_fields = ('B','D','G','I','L','M','P','T','Y')
    _variable_fields = ('C','F','N')
    _binary_fields = ('G','P')
    _character_fields = ('C','M')       # field representing character data
    _decimal_fields = ('F','N')
    _numeric_fields = ('B','F','I','N','Y')
    _currency_fields = ('Y',)
    _supported_tables = ('\x04', '\x8b')
    _dbfTableHeader = ['\x00'] * 32
    _dbfTableHeader[0] = '\x8b'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = ''.join(_dbfTableHeader)
    _dbfTableHeaderExtra = ''
    def _check_memo_integrity(self):
        "dBase III specific"
        if self._meta.header.version == '\x8b':
            try:
                self._meta.memo = self._memoClass(self._meta)
            except:
                self._meta.dfd.close()
                self._meta.dfd = None
                raise
        if not self._meta.ignorememos:
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memotypes:
                    if self._meta.header.version != '\x8b':
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise BadDataError("Table structure corrupt:  memo fields exist, header declares no memos")
                    elif not os.path.exists(self._meta.memoname):
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
                    break

# utility functions
def create_template(table_or_record, defaults=None):
    if isinstance(table_or_record, Table):
        return RecordTemplate(table_or_record._meta, defaults)
    else:
        return RecordTemplate(table_or_record._meta, table_or_record, defaults)
def delete(record):
    "marks record as deleted"
    if record._meta.status == CLOSED:
        raise DbfError("%s is closed; cannot delete record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not record_in_flux:
        record._start_flux()
    try:
        record._data[0] = '*'
        record._dirty = True
    except:
        if not record_in_flux:
            record._rollback_flux()
        raise
    if not record_in_flux:
        record._commit_flux()
def export(table_or_records, filename, field_names=None, format='csv', header=True, codepage=None):
    """writes the records using CSV or tab-delimited format, using the filename
    given if specified, otherwise the table name
    if table_or_records is a collection of records (not an actual table) they
    should all be of the same format"""
    table = source_table(table_or_records[0])
    if field_names is None:
        field_names = table.field_names
    if isinstance(field_names, (str, unicode)):
        field_names = [f.strip() for f in field_names.split(',')]
    format = format.lower()
    if format not in ('csv', 'tab', 'fixed'):
        raise DbfError("export format: csv, tab, or fixed -- not %s" % format)
    if format == 'fixed':
        format = 'txt'
    if codepage is None:
        codepage = table.codepage.name
    encoder = codecs.getencoder(codepage)
    if isinstance(field_names[0], unicode):
        header_names = [encoder(f) for f in field_names]
    else:
        header_names = field_names
    base, ext = os.path.splitext(filename)
    if ext.lower() in ('', '.dbf'):
        filename = base + "." + format[:3]
    fd = open(filename, 'w')
    try:
        if format == 'csv':
            csvfile = csv.writer(fd, dialect='dbf')
            if header:
                csvfile.writerow(header_names)
            for record in table_or_records:
                fields = []
                for fieldname in field_names:
                    data = record[fieldname]
                    if isinstance(data, unicode):
                        fields.append(encoder(data)[0])
                    else:
                        fields.append(data)
                csvfile.writerow(fields)
        elif format == 'tab':
            if header:
                fd.write('\t'.join(header_names) + '\n')
            for record in table_or_records:
                fields = []
                for fieldname in field_names:
                    data = record[fieldname]
                    if isinstance(data, unicode):
                        fields.append(encoder(data)[0])
                    else:
                        fields.append(str(data))
                fd.write('\t'.join(fields) + '\n')
        else: # format == 'fixed'
            header = open("%s_layout.txt" % os.path.splitext(filename)[0], 'w')
            header.write("%-15s  Size\n" % "Field Name")
            header.write("%-15s  ----\n" % ("-" * 15))
            sizes = []
            for field in field_names:
                size = table.field_info(field).length
                sizes.append(size)
                field = encoder(field)[0]
                header.write("%-15s  %3d\n" % (field, size))
            header.write('\nTotal Records in file: %d\n' % len(table_or_records))
            header.close()
            for record in table_or_records:
                fields = []
                for i, fieldname in enumerate(field_names):
                    data = record[fieldname]
                    if isinstance(data, unicode):
                        fields.append("%-*s" % (sizes[i], encoder(data)[0]))
                    else:
                        fields.append("%-*s" % (sizes[i], data))
                fd.write(''.join(fields) + '\n')
    finally:
        fd.close()
        fd = None
    return len(table_or_records)
def field_names(thing):
    "fields in table/record, keys in dict"
    if isinstance(thing, dict):
        return thing.keys()
    elif isinstance(thing, (Table, Record, RecordTemplate)):
        return thing._meta.user_fields[:]
    elif isinstance(thing, Index):
        return thing._table._meta.user_fields[:]
    else:
        for record in thing:    # grab any record
            return record._meta.user_fields[:]

def is_deleted(record):
    "marked for deletion?"
    return record._data[0] == '*'
def recno(record):
    "physical record number"
    return record._recnum
def reset(record, keep_fields=None):
    "sets record's fields back to original, except for fields in keep_fields"
    template = record_in_flux = False
    if isinstance(record, RecordTemplate):
        template = True
    else:
        record_in_flux = not record._write_to_disk
        if record._meta.status == CLOSED:
            raise DbfError("%s is closed; cannot modify record" % record._meta.filename)
    if keep_fields is None:
        keep_fields = []
    keep = {}
    for field in keep_fields:
        keep[field] = record[field]
    record._data[:] = record._meta.blankrecord[:]
    for field in keep_fields:
        record[field] = keep[field]
    if not template:
        if record._write_to_disk:
            record._write()
        else:
            record._dirty = True
def source_table(record):
    "table associated with record"
    table = record._meta.table()
    if table is None:
        raise DbfError("table is no longer available")
    return table
def undelete(record):
    "marks record as active"
    if record._meta.status == CLOSED:
        raise DbfError("%s is closed; cannot undelete record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not record_in_flux:
        record._start_flux()
    try:
        record._data[0] = ' '
        record._dirty = True
    except:
        if not record_in_flux:
            record._rollback_flux()
        raise
    if not record_in_flux:
        record._commit_flux()
def write(record, **kwargs):
    "write record data to disk (updates indices)"
    if record._meta.status == CLOSED:
        raise DbfError("%s is closed; cannot update record" % record._meta.filename)
    elif not record._write_to_disk:
        raise DbfError("unable to use .write_record() while record is in flux")
    if kwargs:
        gather(record, kwargs)
    if record._dirty:
        record._write()
def Process(records):
    "commits each record to disk before returning the next one; undoes all changes to that record if exception raised"
    for record in records:
        try:
            record._start_flux()
            yield record
        except:
            record._rollback_flux()
        else:
            record._commit_flux()

def index(sequence):
    "returns integers 0 - len(sequence)"
    for i in xrange(len(sequence)):
        yield i    
def guess_table_type(filename):
    reported = table_type(filename)
    possibles = []
    version = reported[0]
    for tabletype in (Db3Table, ClpTable, FpTable, VfpTable):
        if version in tabletype._supported_tables:
            possibles.append((tabletype._versionabbr, tabletype._version, tabletype))
    if not possibles:
        raise DbfError("Tables of type %s not supported" % str(reported))
    return possibles
def table_type(filename):
    "returns text representation of a table's dbf version"
    base, ext = os.path.splitext(filename)
    if ext == '':
        filename = base + '.dbf'
    if not os.path.exists(filename):
        raise DbfError('File %s not found' % filename)
    fd = open(filename)
    version = fd.read(1)
    fd.close()
    fd = None
    if not version in version_map:
        raise DbfError("Unknown dbf type: %s (%x)" % (version, ord(version)))
    return version, version_map[version]

def add_fields(table_name, field_specs):
    "adds fields to an existing table"
    table = Table(table_name)
    table.open()
    try:
        table.add_fields(field_specs)
    finally:
        table.close()
def delete_fields(table_name, field_names):
    "deletes fields from an existing table"
    table = Table(table_name)
    table.open()
    try:
        table.delete_fields(field_names)
    finally:
        table.close()
def first_record(table_name):
    "prints the first record of a table"
    table = Table(table_name)
    table.open()
    try:
        print str(table[0])
    finally:
        table.close()
def from_csv(csvfile, to_disk=False, filename=None, field_names=None, extra_fields=None,
        dbf_type='db3', memo_size=64, min_field_size=1,
        encoding=None, errors=None):
    """creates a Character table from a csv file
    to_disk will create a table with the same name
    filename will be used if provided
    field_names default to f0, f1, f2, etc, unless specified (list)
    extra_fields can be used to add additional fields -- should be normal field specifiers (list)"""
    reader = csv.reader(codecs.open(csvfile, 'r', encoding=encoding, errors=errors))
    if field_names:
        if ' ' not in field_names[0]:
            field_names = ['%s M' % fn for fn in field_names]
    else:
        field_names = ['f0 M']
    mtable = Table(':memory:', [field_names[0]], dbf_type=dbf_type, memo_size=memo_size, codepage=encoding)
    mtable.open()
    fields_so_far = 1
    for row in reader:
        while fields_so_far < len(row):
            if fields_so_far == len(field_names):
                field_names.append('f%d M' % fields_so_far)
            mtable.add_fields(field_names[fields_so_far])
            fields_so_far += 1
        mtable.append(tuple(row))
    if filename:
        to_disk = True
    if not to_disk:
        if extra_fields:
            mtable.add_fields(extra_fields)
    else:
        if not filename:
            filename = os.path.splitext(csvfile)[0]
        length = [min_field_size] * len(field_names)
        for record in mtable:
            for i in index(mtable.field_names):
                length[i] = max(length[i], len(record[i]))
        fields = mtable.field_names
        fielddef = []
        for i in index(length):
            if length[i] < 255:
                fielddef.append('%s C(%d)' % (fields[i], length[i]))
            else:
                fielddef.append('%s M' % (fields[i]))
        if extra_fields:
            fielddef.extend(extra_fields)
        csvtable = Table(filename, fielddef, dbf_type=dbf_type, codepage=encoding)
        csvtable.open()
        for record in mtable:
            csvtable.append(scatter(record))
        csvtable.close()
        return csvtable
    mtable.close()
    return mtable
def get_fields(table_name):
    "returns the list of field names of a table"
    table = Table(table_name)
    return table.field_names
def info(table_name):
    "prints table info"
    table = Table(table_name)
    print str(table)
def rename_field(table_name, oldfield, newfield):
    "renames a field in a table"
    table = Table(table_name)
    try:
        table.rename_field(oldfield, newfield)
    finally:
        table.close()
def structure(table_name, field=None):
    "returns the definition of a field (or all fields)"
    table = Table(table_name)
    return table.structure(field)
def hex_dump(records):
    "just what it says ;)"
    for index,dummy in enumerate(records):
        chars = dummy._data
        print "%2d: " % index,
        for char in chars[1:]:
            print " %2x " % ord(char),
        print

# Foxpro functions
def gather(record, data, drop=False):
    """saves data into a record's fields; writes to disk if not in flux
    keys with no matching field will raise a FieldMissingError exception unless drop_missing == True
    if an Exception occurs the record is restored before reraising"""
    if record._meta.status == CLOSED:
        raise DbfError("%s is closed; cannot modify record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not record_in_flux:
        record._start_flux()
    try:
        record_fields = field_names(record)
        for key in field_names(data):
            value = data[key]
            if not key in record_fields:
                if drop:
                    continue
                raise FieldMissingError(key)
            record[key] = value
    except:
        if not record_in_flux:
            record._rollback_flux()
        raise
    if not record_in_flux:
        record._commit_flux()
def scan(table, direction='forward', filter=lambda rec: True):
    """moves record pointer forward 1; returns False if Eof reached
    table must be derived from _Navigation or have skip() method"""
    if direction not in ('forward', 'reverse'):
        raise TypeError("direction should be 'forward' or 'reverse', not %r" % direction)
    if direction == 'forward':
        n = +1
        no_more_records = Eof
    else:
        n = -1
        no_more_records = Bof
    try:
        while True:
            table.skip(n)
            if filter(table.current_record):
                return True
    except no_more_records:
        return False
def scatter(record, as_type=create_template, _mappings=getattr(collections, 'Mapping', dict)):
    "returns as_type() of [fieldnames and] values."
    if isinstance(as_type, types.FunctionType):
        return as_type(record)
    elif issubclass(as_type, _mappings):
        return as_type(zip(field_names(record), record))
    else:
        return as_type(record)

# from dbf.api import *
class fake_module(object):
    def __init__(self, name, *args):
        self.name = name
        self.__all__ = []
        all_objects = globals()
        for name in args:
            self.__dict__[name] = all_objects[name]
            self.__all__.append(name)
    def register(self):
        sys.modules["%s.%s" % (__name__, self.name)] = self

fake_module('api',
    'Table', 'Record', 'List', 'Index',  'Iter', 'Null', 'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum',
    'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
    'recno', 'source_table', 'reset', 'scatter', 'undelete',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 'Vapor',
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other',
    'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
    'InvalidFieldSpecError', 'NonUnicodeError', 'NotFoundError',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex',
    'Process',
    ).register()

dbf = fake_module('dbf', *__all__)
