from __future__ import absolute_import

from datetime import datetime
import json

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db.models.fields import FieldDoesNotExist
from django.test import TestCase, skipIfDBFeature, skipUnlessDBFeature
from django.test.utils import override_settings
from django.utils.translation import ugettext_lazy
from django.core import serializers
from django.core.management import call_command
from django.conf import settings
from django.utils.timezone import utc
from django.db.utils import IntegrityError
from django.utils.rangetypes import DateTimeRange
from django import forms

from .models import LogEvent, Exercise, PersonalExercise, PersonalExercise2

class TestCaseFixtureLoadingTestsNaive(TestCase):
    fixtures = ['fixture1.json']

    @override_settings(USE_TZ=False)
    def test_no_timezone(self):
        self.assertEqual(LogEvent.objects.count(), 3)
        self.assertQuerysetEqual(LogEvent.objects.all(), [
            '<LogEvent: [2006-06-16 12:00:00, 2006-06-16 12:30:00]>',
            '<LogEvent: (2006-06-16 12:30:00, 2006-06-16 13:00:00]>',
            '<LogEvent: (2006-06-16 13:00:00, 2006-06-16 13:30:00]>',
        ])

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_timezone(self):
        # XXX load again: override_settings is executed AFTER  test 
        # fixture loading
        call_command('loaddata', 'fixture1.json', verbosity=0, commit=False)
        self.assertEqual(LogEvent.objects.count(), 3)
        self.assertEqual(LogEvent.objects.get(pk=1).period_t,
                         datetime(2006, 06, 16, 10, 0, 0, tzinfo=utc))
        self.assertQuerysetEqual(LogEvent.objects.all(), [
            '<LogEvent: [2006-06-16 10:00:00+00:00, 2006-06-16 10:30:00+00:00]>',
            '<LogEvent: (2006-06-16 10:30:00+00:00, 2006-06-16 11:00:00+00:00]>',
            '<LogEvent: (2006-06-16 11:00:00+00:00, 2006-06-16 11:30:00+00:00]>',
        ])

class TestCaseFixtureLoadingTestsAware(TestCase):
    fixtures = ['fixture2.json']

    @override_settings(USE_TZ=False)
    def test_no_timezone(self):
        self.assertEqual(LogEvent.objects.count(), 3)
        self.assertQuerysetEqual(LogEvent.objects.all(), [
            '<LogEvent: [2006-06-16 07:00:00, 2006-06-16 07:30:00]>',
            '<LogEvent: (2006-06-16 07:30:00, 2006-06-16 08:00:00]>',
            '<LogEvent: (2006-06-16 08:00:00, 2006-06-16 08:30:00]>',
        ])

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_timezone(self):
        self.assertEqual(LogEvent.objects.count(), 3)
        self.assertQuerysetEqual(LogEvent.objects.all(), [
            '<LogEvent: [2006-06-16 12:00:00+00:00, 2006-06-16 12:30:00+00:00]>',
            '<LogEvent: (2006-06-16 12:30:00+00:00, 2006-06-16 13:00:00+00:00]>',
            '<LogEvent: (2006-06-16 13:00:00+00:00, 2006-06-16 13:30:00+00:00]>',
        ])

class ModelTest(TestCase):

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_timezone_naive(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0),
                    datetime(2005, 7, 28, 10, 10)],
            period_t=datetime(2005, 7, 28, 10, 10)
        )

        a.save()

        a_fetch = LogEvent.objects.get(pk=a.pk)
        self.assertEquals(a_fetch.period_t,
                          datetime(2005, 7, 28, 8, 10, tzinfo=utc))
        self.assertEquals(a_fetch.period.start,
                          datetime(2005, 7, 28, 8, 0, tzinfo=utc))
        self.assertEquals(a_fetch.period.end,
                          datetime(2005, 7, 28, 8, 10, tzinfo=utc))

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_timezone_aware(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc)
        )
        
        a.save()

        a_fetch = LogEvent.objects.get(pk=a.pk)
        self.assertEquals(a_fetch.period_t,
                          datetime(2005, 7, 28, 10, 10, tzinfo=utc))
        self.assertEquals(a_fetch.period.start,
                          datetime(2005, 7, 28, 10, 0, tzinfo=utc))
        self.assertEquals(a_fetch.period.end,
                          datetime(2005, 7, 28, 10, 10, tzinfo=utc))

    @override_settings(USE_TZ=False, TIME_ZONE='Europe/Rome')
    def test_no_timezone_aware(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc)
        )
        
        a.save()

        a_fetch = LogEvent.objects.get(pk=a.pk)
        self.assertEquals(a_fetch.period_t,
                          datetime(2005, 7, 28, 12, 10))

    @override_settings(USE_TZ=False, TIME_ZONE='Europe/Rome')
    def test_no_timezone_naive(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc)
        )
        
        a.save()

        a_fetch = LogEvent.objects.get(pk=a.pk)
        self.assertEquals(a_fetch.period_t,
                          datetime(2005, 7, 28, 12, 10))
        self.assertEquals(a_fetch.period.start,
                          datetime(2005, 7, 28, 12, 0))
        self.assertEquals(a_fetch.period.end,
                          datetime(2005, 7, 28, 12, 10))

    @override_settings(USE_TZ=False)
    def test_serialize_no_timezone_naive(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0),
                    datetime(2005, 7, 28, 10, 10)],
            period_t=datetime(2005, 7, 28, 10, 10)
        )
        ser_data = serializers.serialize('json', [a])
        data = json.loads(ser_data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['fields']['period_t'],
                         '2005-07-28T10:10:00')
        self.assertEqual(data[0]['fields']['period'],
                         '[2005-07-28T10:00:00, 2005-07-28T10:10:00]')


    @override_settings(USE_TZ=False)
    def test_serialize_no_timezone_aware(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc)
        )
        ser_data = serializers.serialize('json', [a])
        data = json.loads(ser_data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['fields']['period_t'],
                         '2005-07-28T10:10:00Z')
        self.assertEqual(data[0]['fields']['period'],
                         '[2005-07-28T10:00:00+00:00, 2005-07-28T10:10:00+00:00]')

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_serialize_timezone_naive(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0),
                    datetime(2005, 7, 28, 10, 10)],
            period_t=datetime(2005, 7, 28, 10, 10)
        )
        ser_data = serializers.serialize('json', [a])
        data = json.loads(ser_data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['fields']['period_t'],
                         '2005-07-28T10:10:00+02:00')
        self.assertEqual(data[0]['fields']['period'],
                         '[2005-07-28T10:00:00+02:00, 2005-07-28T10:10:00+02:00]')

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_serialize_timezone_aware(self):
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc)
        )
        ser_data = serializers.serialize('json', [a])
        data = json.loads(ser_data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['fields']['period_t'],
                         '2005-07-28T10:10:00Z')
        self.assertEqual(data[0]['fields']['period'],
                         '[2005-07-28T10:00:00+00:00, 2005-07-28T10:10:00+00:00]')

    #@override_settings(USE_TZ=False)
    def test_lookup_no_timezone(self):
        self.assertQuerysetEqual(LogEvent.objects.all(), [])

        # Create an instance
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0),
                    datetime(2005, 7, 28, 10, 10)],
            n_events=1,
            n_errors=1,
        )

        a.save()

        self.assertEqual(LogEvent.objects.count(), 1)
        self.assertQuerysetEqual(LogEvent.objects.all(),
            ['<LogEvent: [2005-07-28 10:00:00, 2005-07-28 10:10:00]>'])

        self.assertTrue(LogEvent.objects.filter(
                period__range_contains=a.period
                ))
        self.assertFalse(LogEvent.objects.exclude(
                period__range_contains=a.period
                ))

    @override_settings(USE_TZ=True, TIME_ZONE='Europe/Rome')
    def test_lookup_timezone(self):
        self.assertQuerysetEqual(LogEvent.objects.all(), [])

        # Create an instance
        a = LogEvent(
            period=[datetime(2005, 7, 28, 10, 0, tzinfo=utc),
                    datetime(2005, 7, 28, 10, 10, tzinfo=utc)],
            period_t=datetime(2005, 7, 28, 10, 10, tzinfo=utc),
            n_events=1,
            n_errors=1,
        )

        a.save()

        self.assertEqual(LogEvent.objects.count(), 1)
        self.assertQuerysetEqual(LogEvent.objects.all(),
            ['<LogEvent: [2005-07-28 10:00:00+00:00, 2005-07-28 10:10:00+00:00]>'])

        self.assertTrue(LogEvent.objects.filter(
                period__range_contains=a.period
                ))
        self.assertFalse(LogEvent.objects.exclude(
                period__range_contains=a.period
                ))

    def test_null(self):
        a = Exercise(name='test')
        a.full_clean()
        a.save()
        self.assertTrue(Exercise.objects.count(), 1)
        self.assertEquals(Exercise.objects.get().period, None)

    def test_unique(self):
        period = DateTimeRange(
            datetime(2012, 1, 1, 12, 30),
            datetime(2012, 1, 1, 12, 33))
        a = PersonalExercise(name='test', period=period)
        a.full_clean()
        a.save()
        self.assertTrue(PersonalExercise.objects.count(), 1)
        self.assertEquals(PersonalExercise.objects.get().period, period)
        # save the same period again
        a1 = PersonalExercise(name='test 1', period=period)
        self.failUnlessRaises(ValidationError, a1.full_clean)
        self.failUnlessRaises(IntegrityError, a1.save)

    def test_range_overlap(self):
        period = DateTimeRange(
            datetime(2012, 1, 1, 12, 30),
            datetime(2012, 1, 1, 12, 33))
        a = PersonalExercise2(name='test', period=period)
        a.full_clean()
        a.save()
        self.assertTrue(PersonalExercise2.objects.count(), 1)
        self.assertEquals(PersonalExercise2.objects.get().period, period)
        # save an overlapping period
        period2 = DateTimeRange(
            datetime(2012, 1, 1, 12, 32),
            datetime(2012, 1, 1, 12, 34))
        a1 = PersonalExercise2(name='test 1', period=period)
        self.failUnlessRaises(ValidationError, a1.full_clean)
        self.failUnlessRaises(IntegrityError, a1.save)

    def test_format(self):
        pass

    @override_settings(USE_L10N=True, LANGUAGE_CODE='it-it')
    def test_localize(self):
        from django.utils.formats import localize
        loc_range = unicode(localize(DateTimeRange(
                datetime(2012, 1, 1, 12, 32),
                datetime(2012, 1, 1, 12, 34))))
        self.assertIn('1/2012', loc_range)

    def test_db_index(self):
        pass

    def test_admin(self):
        pass

    def test_modelform(self):
        class LogEventModelForm(forms.ModelForm):
            class Meta:
                model = LogEvent
        # empty form
        form = LogEventModelForm()
        self.assertIn('id_period', form.as_table())
        # bad input form
        form = LogEventModelForm({'period': 'xxxx'})
        self.assertFalse(form.is_valid())

    def test_formfield(self):
        pass
