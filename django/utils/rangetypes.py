'''
Python range types inplementation.
'''

import re
import datetime

from django.core.exceptions import ValidationError
from django.utils.encoding import smart_str
from django.utils.dateparse import parse_date, parse_datetime
from django.utils.translation import ugettext_lazy as _
from django.utils import timezone
from django.conf import settings

RANGE_RE = re.compile(r'^([[\(])' # range start "[" inclusive or "(" exclusive
                      r'(.*),[ ]*(.*)' # range start and end values
                      r'([]\)])$') # range stop (just like range start)

class AbstractRange(object):
    error_messages = {
        'overlap': _(u'%(model_name)s with overlapping %(field_label)s '
                     u'already exists.'),
        'invalid_range_format': _(u"'%s' value has to be a valid range format"),
        'invalid_range': _(u"'%s' value has to be a list or tuple.")
        }

    bound_to_python = None

    def __init__(self, start, end, start_inclusive=True, end_inclusive=True):
        if self.bound_to_python:
            start, end = self.bound_to_python(start), self.bound_to_python(end)
        self.start, self.end = start, end
        self.start_inclusive, self.end_inclusive =\
            start_inclusive, end_inclusive

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, str(self))

    def __str__(self):
        return self.__class__.to_string(self.start, self.end, 
                                        self.start_inclusive,
                                        self.end_inclusive)

    def __unicode__(self):
        return self.__str__()

    def __eq__(self, other):
        return (isinstance(other, AbstractRange) and
                self.start == other.start and
                self.end == other.end and
                self.start_inclusive == other.start_inclusive and
                self.end_inclusive == other.end_inclusive)

    @classmethod
    def from_string(cls, stringval):
        mo = RANGE_RE.match(stringval)
        if not mo:
            raise ValueError('bad range format')
        groups = mo.groups()
        start, end = groups[1:3]
        if cls.bound_to_python:
            start = cls.bound_to_python(start)
            end = cls.bound_to_python(end)
        return cls(start,
                   end,
                   start_inclusive=groups[0]=='[',
                   end_inclusive=groups[3]==']')

    @staticmethod
    def to_string(start, end, start_inclusive=True, end_inclusive=True):
        return '%s%s, %s%s' % (
            '[' if start_inclusive else '(',
            start if start is not None else '',
            end if end is not None else '',
            ']' if end_inclusive else ')',
            )

    @classmethod
    def to_python(cls, value):
        if value is None:
            return value
        if isinstance(value, basestring):
            try:
                value = cls.from_string(value)
            except ValueError, e:
                msg = cls.error_messages['invalid_range_format'] % value
                raise ValidationError(msg)
        elif isinstance(value, (list, tuple)):
            if len(value) != 2:
                msg = cls.error_messages['invalid_range'] % value
                raise ValidationError(msg)
            value = cls(*value)
        if not isinstance(value, cls):
            msg = cls.error_messages['invalid_range'] % value
            raise ValidationError(msg)

def to_python_datetime(value):
    '''Convert a value into a datetime.

    copied from django/db/models/fields.py
    '''
    error_messages = {
        'invalid': _(u"'%s' value has an invalid format. It must be in "
                     u"YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ] format."),
        'invalid_date': _(u"'%s' value has the correct format "
                          u"(YYYY-MM-DD) but it is an invalid date."),
        'invalid_datetime': _(u"'%s' value has the correct format "
                              u"(YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ]) "
                              u"but it is an invalid date/time."),
        }
    if value is None:
        return value
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        value = datetime.datetime(value.year, value.month, value.day)
        if settings.USE_TZ:
            # For backwards compatibility, interpret naive datetimes in
            # local time. This won't work during DST change, but we can't
            # do much about it, so we let the exceptions percolate up the
            # call stack.
            warnings.warn(u"DateTimeField received a naive datetime (%s)"
                          u" while time zone support is active." % value,
                          RuntimeWarning)
            default_timezone = timezone.get_default_timezone()
            value = timezone.make_aware(value, default_timezone)
        return value

    value = smart_str(value)

    try:
        parsed = parse_datetime(value)
        if parsed is not None:
            return parsed
    except ValueError:
        msg = error_messages['invalid_datetime'] % value
        raise ValidationError(msg)

    try:
        parsed = parse_date(value)
        if parsed is not None:
            return datetime.datetime(parsed.year, parsed.month, parsed.day)
    except ValueError:
        msg = error_messages['invalid_date'] % value
        raise ValidationError(msg)

    msg = error_messages['invalid'] % value
    raise ValidationError(msg)

class DateTimeRange(AbstractRange):

    bound_to_python = staticmethod(to_python_datetime)

    def isoformat(self):
        return self.__class__.to_string(
            self.start.isoformat() if self.start is not None else None,
            self.end.isoformat() if self.end is not None else None,
            self.start_inclusive,
            self.end_inclusive)

    def is_naive(self):
        return ((self.start is not None and timezone.is_naive(self.start))
                or
                (self.end is not None and timezone.is_naive(self.end)))

    @classmethod
    def to_python(cls, value, make_aware=False):
        value = super(DateTimeRange, cls).to_python(value)
        if make_aware and settings.USE_TZ and value is not None:
            current_timezone = None
            for attname, attval in (('start', value.start),
                                    ('end', value.end)):
                if timezone.is_naive(attval):
                    if current_timezone is None:
                        current_timezone = timezone.get_current_timezone()
                    try:
                        attval = timezone.make_aware(attval, current_timezone)
                    except Exception, e:
                        raise ValidationError(
                            _('%(datetime)s couldn\'t be interpreted '
                              'in time zone %(current_timezone)s; it '
                              'may be ambiguous or it may not exist.')
                            % {'datetime': attval,
                               'current_timezone': current_timezone})
                    seatttr(self, attname, attval)
        return value
