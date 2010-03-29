from django.test import TestCase
from django.contrib.auth.models import User
from qsstats import QuerySetStats, InvalidInterval, DateFieldMissing, QuerySetMissing
import datetime

class QuerySetStatsTestCase(TestCase):
    def test_basic_today(self):
        # We'll be making sure that this user is found
        u1 = User.objects.create_user('u1', 'u1@example.com')
        # And that this user is not
        u2 = User.objects.create_user('u2', 'u2@example.com')
        u2.is_active = False
        u2.save()

        # Create a QuerySet and QuerySetStats
        qs = User.objects.filter(is_active=True)
        qss = QuerySetStats(qs, 'date_joined')
        
        # We should only see a single user
        self.assertEqual(qss.this_day(), 1)

    def test_time_series(self):
        today = datetime.date.today()
        seven_days_ago = today - datetime.timedelta(days=7)
        for j in range(1,8):
            for i in range(0,j):
                u = User.objects.create_user('p-%s-%s' % (j, i), 'p%s-%s@example.com' % (j, i))
                u.date_joined = today - datetime.timedelta(days=i)
                u.save()
        qs = User.objects.all()
        qss = QuerySetStats(qs, 'date_joined')
        self.assertEqual(qss.time_series(seven_days_ago, today), [])

    # MC_TODO: aggregate_field tests

    def test_query_set_missing(self):
        qss = QuerySetStats(date_field='foo')
        for method in ['this_day', 'this_month', 'this_year']:
            self.assertRaises(QuerySetMissing, getattr(qss, method))

    def test_date_field_missing(self):
        qss = QuerySetStats(User.objects.all())
        for method in ['this_day', 'this_month', 'this_year']:
            self.assertRaises(DateFieldMissing, getattr(qss, method))

    def test_invalid_interval(self):
        qss = QuerySetStats(User.objects.all(), 'date_joined')
        def _invalid():
            qss.time_series(qss.today, qss.today, interval='monkeys')
        self.assertRaises(InvalidInterval, _invalid)
