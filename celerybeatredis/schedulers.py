# Copyright 2014 Kong Luoxing

# Licensed under the Apache License, Version 2.0 (the 'License'); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0

import datetime
from copy import deepcopy

try:
    import simplejson as json
except ImportError:
    import json

from celery.beat import Scheduler, ScheduleEntry
from celery.utils.log import get_logger
from celery import current_app
from celerymethods import celeryconfig
import celery.schedules

from redis.client import StrictRedis

from decoder import DateTimeDecoder, DateTimeEncoder


class ValidationError(Exception):
    pass


class PeriodicTask(object):
    '''represents a periodic task
    '''

    def __init__(self, name, task, schedule, key=None, enabled=True, task_args=[], task_kwargs={}, **kwargs):
        self.task = task
        self.enabled = enabled
        self.interval = None
        self.crontab = None
        if isinstance(schedule, self.Interval):
            self.interval = schedule
        if isinstance(schedule, self.Crontab):
            self.crontab = schedule
        self.args = task_args
        self.kwargs = task_kwargs

        #
        # Use celeryconfig for adding tasks outside celery process
        # where current_app is not available
        #
        if not key:
            self.name = celeryconfig.CELERY_REDIS_SCHEDULER_KEY_PREFIX + name
        else:
            self.name = celeryconfig.CELERY_REDIS_SCHEDULER_KEY_PREFIX + name + ':' + key

        self.type_ = None

        self.queue = None
        self.exchange = None
        self.routing_key = None

        # datetime
        self.expires = None

        # datetime
        self.last_run_at = None

        self.total_run_count = 0

        self.date_changed = None
        self.description = None

    class Interval(object):

        def __init__(self, every, period='seconds'):
            self.every = every
            # could be seconds minutes hours
            self.period = period

        @property
        def schedule(self):
            return celery.schedules.schedule(datetime.timedelta(**{self.period: self.every}))

        @property
        def period_singular(self):
            return self.period[:-1]

        def __unicode__(self):
            if self.every == 1:
                return 'every {0.period_singular}'.format(self)
            return 'every {0.every} {0.period}'.format(self)

    class Crontab(object):

        def __init__(self, minute, hour, day_of_week, day_of_month, month_of_year):
            self.minute = minute
            self.hour = hour
            self.day_of_week = day_of_week
            self.day_of_month = day_of_month
            self.month_of_year = month_of_year

        @property
        def schedule(self):
            return celery.schedules.crontab(minute=self.minute,
                                            hour=self.hour,
                                            day_of_week=self.day_of_week,
                                            day_of_month=self.day_of_month,
                                            month_of_year=self.month_of_year)

        def __unicode__(self):
            rfield = lambda f: f and str(f).replace(' ', '') or '*'
            return '{0} {1} {2} {3} {4} (m/h/d/dM/MY)'.format(
                rfield(self.minute), rfield(self.hour), rfield(self.day_of_week),
                rfield(self.day_of_month), rfield(self.month_of_year),
            )

    @staticmethod
    def get_all():
        """get all of the tasks, for best performance with large amount of tasks, return a generator
        """
        rdb = StrictRedis.from_url(current_app.conf.CELERY_REDIS_SCHEDULER_URL)
        tasks = rdb.keys(current_app.conf.CELERY_REDIS_SCHEDULER_KEY_PREFIX + '*')
        for task_name in tasks:
            yield json.loads(rdb.get(task_name), cls=DateTimeDecoder)

    def save(self):
        # must do a deepcopy
        #
        # Use celeryconfig for adding tasks outside celery process
        # when current_app is not available
        #
        rdb = StrictRedis.from_url(celeryconfig.CELERY_REDIS_SCHEDULER_URL)
        self_dict = deepcopy(self.__dict__)
        if self_dict.get('interval', None) is not None:
            self_dict['interval'] = self.interval.__dict__
        if self_dict.get('crontab', None) is not None:
            self_dict['crontab'] = self.crontab.__dict__
        rdb.set(self.name, json.dumps(self_dict, cls=DateTimeEncoder))

    def update(self):
        if self.dirty:
            self.save()

    def clean(self):
        """validation to ensure that you only have
        an interval or crontab schedule, but not both simultaneously"""
        if self.interval and self.crontab:
            msg = 'Cannot define both interval and crontab schedule.'
            raise ValidationError(msg)
        if not (self.interval or self.crontab):
            msg = 'Must defined either interval or crontab schedule.'
            raise ValidationError(msg)

    @staticmethod
    def from_dict(d):
        """
        build PeriodicTask instance from dict
        :param d: dict
        :return: PeriodicTask instance
        """
        schedule = None
        if d.get('interval', None) is not None:
            schedule = PeriodicTask.Interval(d['interval']['every'], d['interval']['period'])
        if d.get('crontab', None) is not None:
            schedule = PeriodicTask.Crontab(
                d['crontab']['minute'],
                d['crontab']['hour'],
                d['crontab']['day_of_week'],
                d['crontab']['day_of_month'],
                d['crontab']['month_of_year']
            )
        if schedule is None:
            # Invalid task
            return None

        task = PeriodicTask(d['name'], d['task'], schedule)
        for key in d:
            if key not in ('interval', 'crontab', 'schedule'):
                setattr(task, key, d[key])
        return task

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        elif self.crontab:
            return self.crontab.schedule
        else:
            raise Exception('must define interval or crontab schedule')

    def __unicode__(self):
        fmt = '{0.name}: {{no schedule}}'
        if self.interval:
            fmt = '{0.name}: {0.interval}'
        elif self.crontab:
            fmt = '{0.name}: {0.crontab}'
        else:
            raise Exception('must define internal or crontab schedule')
        return fmt.format(self)


class RedisScheduleEntry(ScheduleEntry):
    def __init__(self, task):
        self._task = task

        self.app = current_app._get_current_object()
        self.name = self._task.name
        self.task = self._task.task

        self.schedule = self._task.schedule

        self.args = self._task.args
        self.kwargs = self._task.kwargs
        self.options = {
            'queue': self._task.queue,
            'exchange': self._task.exchange,
            'routing_key': self._task.routing_key,
            'expires': self._task.expires
        }
        if not self._task.total_run_count:
            self._task.total_run_count = 0
        self.total_run_count = self._task.total_run_count

        if not self._task.last_run_at:
            self._task.last_run_at = self._default_now()
        self.last_run_at = self._task.last_run_at

    def _default_now(self):
        return self.app.now()

    def next(self):
        self._task.last_run_at = self.app.now()
        self._task.total_run_count += 1
        return self.__class__(self._task)

    __next__ = next

    def is_due(self):
        if not self._task.enabled:
            return False, 5.0  # 5 second delay for re-enable.
        return self.schedule.is_due(self.last_run_at)

    def __repr__(self):
        return '<RedisScheduleEntry ({0} {1}(*{2}, **{3}) {{4}})>'.format(
            self.name, self.task, self.args,
            self.kwargs, self.schedule,
        )

    def reserve(self, entry):
        new_entry = Scheduler.reserve(self, entry)
        return new_entry

    def save(self):
        dirty = False
        if self.total_run_count > self._task.total_run_count:
            self._task.total_run_count = self.total_run_count
            dirty = True
        if self.last_run_at and self._task.last_run_at and self.last_run_at > self._task.last_run_at:
            self._task.last_run_at = self.last_run_at
            dirty = True
        if dirty:
            self._task.save()

    def update(self, task):
        self._task = task
        self.name = task.name
        self.task = task.task

        self.schedule = task.schedule

        self.args = task.args
        self.kwargs = task.kwargs


class RedisScheduler(Scheduler):
    # how often should we sync in schedule information
    # from the backend redis database
    UPDATE_INTERVAL = datetime.timedelta(seconds=5)

    Entry = RedisScheduleEntry

    def __init__(self, *args, **kwargs):
        if hasattr(current_app.conf, 'CELERY_REDIS_SCHEDULER_URL'):
            get_logger(__name__).info('backend scheduler using %s',
                                      current_app.conf.CELERY_REDIS_SCHEDULER_URL)
        else:
            get_logger(__name__).info('backend scheduler using %s',
                                      current_app.conf.CELERY_REDIS_SCHEDULER_URL)

        self._schedule = {}
        self._last_updated = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval')
                             or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 300)

    def setup_schedule(self):
        pass

    def requires_update(self):
        """check whether we should pull an updated schedule
        from the backend database"""
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.datetime.now()

    def update_from_database(self):
        for task in PeriodicTask.get_all():
            t = PeriodicTask.from_dict(task)
            if t is None:
                continue
            if t.name in self._schedule:
                self._schedule[t.name].update(t)
            else:
                self._schedule[t.name] = RedisScheduleEntry(t)

    @property
    def schedule(self):
        if self.requires_update():
            self.update_from_database()
            self._last_updated = datetime.datetime.now()
            self.sync()
        return self._schedule

    def sync(self):
        for entry in self._schedule.values():
            entry.save()

    def close(self):
        self.sync()
