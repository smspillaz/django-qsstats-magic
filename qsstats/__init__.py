__author__ = 'Matt Croydon'
__version__ = (0, 3, 1)

from dateutil.relativedelta import relativedelta, MO
from django.conf import settings
from django.db.models import Count
import datetime

class QuerySetStatsError(Exception):
    pass

class InvalidInterval(QuerySetStatsError):
    pass

class InvalidOperator(QuerySetStatsError):
    pass

class DateFieldMissing(QuerySetStatsError):
    pass

class QuerySetMissing(QuerySetStatsError):
    pass

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

    def for_day(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        date_field = date_field or self.date_field
        kwargs = {
            '%s__year' % date_field : dt.year,
            '%s__month' % date_field : dt.month,
            '%s__day' % date_field : dt.day,
        }
        return self._aggregate(date_field, aggregate_field, aggregate_class, kwargs)

    def this_day(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_day(self.today, date_field, aggregate_field, aggregate_class)

    def for_week(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        first_day = dt - relativedelta(weekday=MO(-1))
        last_day = first_day + datetime.timedelta(days=7)
        return self.get_aggregate(first_day, last_day, date_field, aggregate_field, aggregate_class)

    def this_week(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_week(self.today, date_field, aggregate_class)

    def for_month(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        first_day = datetime.date(year=dt.year, month=dt.month, day=1)
        last_day = first_day + relativedelta(day=31)
        return self.get_aggregate(first_day, last_day, date_field, aggregate_field, aggregate_class)

    def this_month(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_month(self.today, date_field, aggregate_class)

    def for_year(self, dt, date_field=None, aggregate_field=None, aggregate_class=None):
        first_day = datetime.date(year=dt.year, month=1, day=1)
        last_day = datetime.date(year=dt.year, month=12, day=31)
        return self.get_aggregate(first_day, last_day, date_field, aggregate_field, aggregate_class)

    def this_year(self, date_field=None, aggregate_field=None, aggregate_class=None):
        return self.for_year(self.today, date_field, aggregate_field, aggregate_class)

    # Aggregate over time intervals

    def time_series(self, start_date, end_date, interval='days', date_field=None, aggregate_field=None, aggregate_class=None):
        if interval not in ('years', 'months', 'weeks', 'days'):
            raise InvalidInterval('Inverval not supported.')

        date_field = date_field or self.date_field
        aggregate_class = aggregate_class or self.aggregate_class
        aggregate_field = aggregate_field or self.aggregate_field

        if not date_field:
            raise DateFieldMissing("Please provide a date_field.")

        if not self.qs:
            raise QuerySetMissing("Please provide a queryset.")

        stat_list = []
        dt = start_date
        while dt < end_date:
            # MC_TODO: Less hacky way of doing this?
            method = getattr(self, 'for_%s' % interval.rstrip('s'))
            stat_list.append((dt, method(dt, date_field=date_field, aggregate_field=aggregate_field, aggregate_class=aggregate_class)))
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

        if not self.qs:
            raise QuerySetMissing("Please provide a queryset.")

        agg = self.qs.filter(**filter).aggregate(agg=aggregate_class(aggregate_field))
        return agg['agg']

    def get_aggregate(self, first_day, last_day, date_field=None, aggregate_field=None, aggregate_class=None):
        date_field = date_field or self.date_field
        kwargs = {'%s__range' % date_field : (first_day, last_day)}
        return self._aggregate(date_field, aggregate_field, aggregate_class, kwargs)
