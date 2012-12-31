# coding: utf-8
from django.db import models

class LogEvent(models.Model):
    period = models.DateTimeRangeField() 
    period_t = models.DateTimeField(null=True, blank=True)
    n_events = models.IntegerField(null=True, blank=True)
    n_errors = models.IntegerField(null=True, blank=True)

    def __unicode__(self):
        return '%s' % self.period

class Exercise(models.Model):
    name = models.CharField(max_length=200)
    period = models.DateTimeRangeField(null=True, blank=True) 

    def __unicode__(self):
        return self.name or ''

class PersonalExercise(models.Model):
    name = models.CharField(max_length=200)
    period = models.DateTimeRangeField(null=True, blank=True, unique=True)

    def __unicode__(self):
        return self.name or ''

class PersonalExercise2(models.Model):
    name = models.CharField(max_length=200)
    period = models.DateTimeRangeField(null=True, blank=True,
                                       disallow_overlap=True)

    def __unicode__(self):
        return self.name or ''
