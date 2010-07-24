__author__ = 'Matt Croydon'
__version__ = (0, 3, 1)

from dateutil.relativedelta import relativedelta, MO
from dateutil.parser import parse
from django.conf import settings
from django.db.models import Count
from django.db import DatabaseError
from functools import partial
import datetime
import time

class QuerySetStatsError(Exception):
    pass

class InvalidInterval(QuerySetStatsError):
    pass

class UnsupportedEngine(QuerySetStatsError):
    pass

class InvalidOperator(QuerySetStatsError):
    pass

class DateFieldMissing(QuerySetStatsError):
    pass

class QuerySetMissing(QuerySetStatsError):
    pass

def _to_date(dt):
    return datetime.date(dt.year, dt.month, dt.day)

def _to_datetime(dt):
    if isinstance(dt, datetime.datetime):
        return dt
    return datetime.datetime(dt.year, dt.month, dt.day)

def get_bounds(dt, interval):
    ''' Returns interval bounds the datetime is in.
    Interval can be day, week, month and year. '''

    day = _to_datetime(_to_date(dt))
    dt = _to_datetime(dt)

    if interval == 'minute':
        begin = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)
        end = begin + relativedelta(minutes=1)
    elif interval == 'hour':
        begin = datetime.datetime(dt.year, dt.month, dt.day, dt.hour)
        end = begin + relativedelta(hours=1)
    elif interval == 'day':
        begin = end = day
    elif interval == 'week':
        begin = day - relativedelta(weekday=MO(-1))
        end = begin + datetime.timedelta(days=7)
    elif interval == 'month':
        begin = datetime.datetime(dt.year, dt.month, 1)
        end = begin + relativedelta(day=31)
    elif interval == 'year':
        begin = datetime.datetime(dt.year, 1, 1)
        end = datetime.datetime(dt.year, 12, 31)
    else:
        raise InvalidInterval('Inverval not supported.')
    return begin, end


class QuerySetStats(object):
    """
    Generates statistics about a queryset using Django aggregates.  QuerySetStats
    is able to handle snapshots of data (for example this day, week, month, or
    year) or generate time series data suitable for graphing.
    """
    def __init__(self, qs=None, date_field=None, aggregate_field=None, aggregate_class=None, operator=None):
        self.qs = qs
        self.date_field = date_field
        self.aggregate_field = aggregate_field or getattr(settings, 'QUERYSETSTATS_DEFAULT_AGGREGATE_FIELD', 'id')
        self.aggregate_class = aggregate_class or getattr(settings, 'QUERYSETSTATS_DEFAULT_AGGREGATE_CLASS', Count)
        self.operator = operator or getattr(settings, 'QUERYSETSTATS_DEFAULT_OPERATOR', 'lte')

        # MC_TODO: Danger in caching this?
        self.update_today()

    # Aggregates for a specific period of time

    def for_interval(self, interval, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        begin, end = get_bounds(dt, interval)
        return self.get_aggregate(begin, end, date_field, aggregate_field, aggregate_class)

    def this_interval(self, interval, date_field=None, aggregate_field=None, aggregate_class=None):
        method = getattr(self, 'for_%s' % interval)
        return method(self.today, date_field, aggregate_field, aggregate_class)

    # code below is without magic so IDE autocomplete will work
    # the short version would be (instead of all for_* and this_* methods):

#    def __getattr__(self, name):
#        if name.startswith('for_'):
#            return partial(self.for_interval, name[4:])
#        if name.startswith('this_'):
#            return partial(self.this_interval, name[5:])
#

    def for_minute(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_interval('minute', dt, date_field, aggregate_field, aggregate_class)

    def for_hour(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_interval('hour', dt, date_field, aggregate_field, aggregate_class)

    def for_day(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        date_field = date_field or self.date_field
        kwargs = {
            '%s__year' % date_field : dt.year,
            '%s__month' % date_field : dt.month,
            '%s__day' % date_field : dt.day,
        }
        return self._aggregate(date_field, aggregate_field, aggregate_class, kwargs)

    def for_week(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_interval('week', dt, date_field, aggregate_field, aggregate_class)

    def for_month(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_interval('month', dt, date_field, aggregate_field, aggregate_class)

    def for_year(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_interval('year', dt, date_field, aggregate_field, aggregate_class)


    def this_hour(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_hour(self.today, date_field, aggregate_field, aggregate_class)

    def this_minute(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_minute(self.today, date_field, aggregate_field, aggregate_class)

    def this_day(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_day(self.today, date_field, aggregate_field, aggregate_class)

    def this_week(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_week(self.today, date_field, aggregate_class)

    def this_month(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_month(self.today, date_field, aggregate_class)

    def this_year(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_year(self.today, date_field, aggregate_field, aggregate_class)

    # Aggregate over time intervals

    def time_series(self, start_date, end_date=None, interval='days', date_field=None, aggregate_field=None, aggregate_class=None, engine='mysql'):
        end_date = end_date or self.today + datetime.timedelta(days=1)
        args = [start_date, end_date, interval, date_field, aggregate_field, aggregate_class]
        try:
            #TODO: engine should be guessed
            return self._fast_time_series(*(args+[engine]))
        except (QuerySetStatsError, DatabaseError,):
            return self._slow_time_series(*args)

    def _slow_time_series(self, start_date, end_date, interval='days', date_field=None, aggregate_field=None, aggregate_class=None):
        try:
            method = getattr(self, 'for_%s' % interval[:-1])
        except AttributeError:
            raise InvalidInterval('Interval not supported.')

        stat_list = []
        dt, end_date = _to_datetime(start_date), _to_datetime(end_date)
        while dt < end_date:
            result = method(dt, date_field=date_field,
                            aggregate_field=aggregate_field,
                            aggregate_class=aggregate_class)
            stat_list.append((dt, result,))
            dt = dt + relativedelta(**{interval : 1})
        return stat_list

    def _fast_time_series(self, start_date, end_date, interval='days', date_field=None, aggregate_field=None, aggregate_class=None, engine='mysql'):
        date_field = date_field or self.date_field
        aggregate_field = aggregate_field or self.aggregate_field
        aggregate_class = aggregate_class or self.aggregate_class

        begin, _ = get_bounds(start_date, interval.rstrip('s'))
        _, end = get_bounds(end_date, interval.rstrip('s'))

        # sql should return the beginning of each interval
        SQL = {
            'mysql': {
                'minutes': "DATE_FORMAT(`" + date_field +"`, '%%Y-%%m-%%d %%H:%%i')",
                'hours': "DATE_FORMAT(`" + date_field +"`, '%%Y-%%m-%%d %%H:00')",
                'days': "DATE_FORMAT(`" + date_field +"`, '%%Y-%%m-%%d')",
                'weeks': "DATE_FORMAT(DATE_SUB(`"+date_field+"`, INTERVAL(WEEKDAY(`"+date_field+"`)) DAY), '%%Y-%%m-%%d')",
                'months': "DATE_FORMAT(`" + date_field +"`, '%%Y-%%m-01')",
                'years': "DATE_FORMAT(`" + date_field +"`, '%%Y-01-01')",
            }
        }

        try:
            engine_sql = SQL[engine]
        except KeyError:
            raise UnsupportedEngine('%s DB engine is not supported' % engine)

        try:
            interval_sql = engine_sql[interval]
        except KeyError:
            raise InvalidInterval('Interval is not supported for %s DB backend.' % engine)

        kwargs = {'%s__range' % date_field : (begin, end)}
        aggregate = self.qs.extra(select = {'d': interval_sql}).\
                        filter(**kwargs).order_by().values('d').\
                        annotate(agg=aggregate_class(aggregate_field))

        data = dict((parse(item['d'], yearfirst=True), item['agg']) for item in aggregate)

        stat_list = []
        dt = begin
        while dt < end:
            value = data.get(dt, 0)
            stat_list.append((dt, value,))
            dt = dt + relativedelta(**{interval : 1})
        return stat_list


    # Aggregate totals using a date or datetime as a pivot

    def until(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.pivot(dt, 'lte', date_field, aggregate_field, aggregate_class)

    def until_now(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.pivot(datetime.datetime.now(), 'lte', date_field, aggregate_field, aggregate_class)

    def after(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.pivot(dt, 'gte', date_field, aggregate_field, aggregate_class)

    def after_now(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.pivot(datetime.datetime.now(), 'gte', date_field, aggregate_field, aggregate_class)

    def pivot(self, dt, operator=None, date_field=None, aggregate_field=None, aggregate_class=None):
        operator = operator or self.operator
        if operator not in ['lt', 'lte', 'gt', 'gte']:
            raise InvalidOperator("Please provide a valid operator.")

        kwargs = {'%s__%s' % (date_field or self.date_field, operator) : dt}
        return self._aggregate(date_field, aggregate_field, aggregate_class, kwargs)

    # Utility functions
    def update_today(self):
        self.today = datetime.date.today()

    def _aggregate(self, date_field=None, aggregate_field=None, aggregate_class=None, filter=None):
        date_field = date_field or self.date_field
        aggregate_field = aggregate_field or self.aggregate_field
        aggregate_class = aggregate_class or self.aggregate_class

        if not date_field:
            raise DateFieldMissing("Please provide a date_field.")

        if self.qs is None:
            raise QuerySetMissing("Please provide a queryset.")

        agg = self.qs.filter(**filter).aggregate(agg=aggregate_class(aggregate_field))
        return agg['agg']

    def get_aggregate(self, first_day, last_day, date_field=None, aggregate_field=None, aggregate_class=None):
        date_field = date_field or self.date_field
        kwargs = {'%s__range' % date_field : (first_day, last_day)}
        return self._aggregate(date_field, aggregate_field, aggregate_class, kwargs)
