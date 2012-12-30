import datetime
import warnings

from django.db.models.fields import Field, DateTimeField
from django.utils.translation import ugettext_lazy as _
from django.utils.rangetypes import DateTimeRange
from django.core import exceptions
from django.conf import settings
from django.utils import timezone
from django import forms

__all__ = ['DateTimeRangeField']

class DateTimeRangeField(Field):
    default_error_messages = DateTimeRange.error_messages

    empty_strings_allowed = False

    description = _("Date time range")

    def contribute_to_class(self, cls, name):
        super(DateTimeRangeField, self).contribute_to_class(cls, name)
        if self.disallow_overlap:
            self._register_disallow_overlap()

    def get_internal_type(self):
        return "DateTimeRangeField"

    def to_python(self, value):
        if value is None:
            return value
        value = DateTimeRange.to_python(value)
        return value

    def get_prep_value(self, value):
        value = self.to_python(value)
        if value is not None and settings.USE_TZ and value.is_naive():
            # For backwards compatibility, interpret naive datetimes in local
            # time. This won't work during DST change, but we can't do much
            # about it, so we let the exceptions percolate up the call stack.
            for attr in ('start', 'end'):
                val = getattr(value, attr)
                if timezone.is_naive(val):
                    default_timezone = timezone.get_default_timezone()
                    val = timezone.make_aware(val, default_timezone)
                    setattr(value, attr, val)
            warnings.warn(u"DateTimeField received a naive datetime (%s)"
                          u" while time zone support is active." % value,
                          RuntimeWarning)
        return value

    def get_db_prep_value(self, value, connection, prepared=False):
        # Casts dates into the formvat expected by the backend
        if not prepared:
            value = self.get_prep_value(value)
        if value is None:
            return value
        return connection.ops.value_to_db_datetimerange(value,
                                                        value.start_inclusive,
                                                        value.end_inclusive)

    #def pre_save(self, model_instance, add):
    #    pass

    def formfield(self, **kwargs):
        defaults = {'form_class': forms.DateTimeRangeField}
        defaults.update(kwargs)
        return super(DateTimeRangeField, self).formfield(**defaults)

    def value_to_string(self, obj):
        val = self._get_val_from_obj(obj)
        val = self.get_prep_value(val)
        return '' if val is None else val.isoformat()

    def _disallow_overlap_cb(self, sender, **kwargs):
        # XXX TODO tablespace support
        # XXX TODO move this in db backend and sql.* modules maybe
        from django.db import connections
        db = kwargs['db']
        connection = connections[db]
        curs = connection.cursor()
        curs.execute(
            'ALTER TABLE %s ADD EXCLUDE USING gist (%s WITH &&);' % (
                self.model._meta.db_table,
                self.column))
        curs.close()

    def _register_disallow_overlap(self):
        '''Make sure the constraint will be added at db level.

        '''
        from django.db.models.signals import post_syncdb
        from importlib import import_module
        module = import_module(self.model.__module__)
        post_syncdb.connect(self._disallow_overlap_cb, sender=module)
