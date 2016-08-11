# -*- coding: utf-8 -*-

__author__ = 'return'

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


'''
	任务调度模块
'''


class TaskSched(object):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__task_scheduler = BackgroundScheduler()
		self.__run_background_sched()

	def __run_background_sched(self):
		self.__task_scheduler.start()

	def add_normal_task(self, task_func, task_id, task_name):
		self.logger.debug('add normal task: {0}'.format(task_name))
		self.__task_scheduler.add_job(func=task_func, id=task_id, name=task_name)

	def add_interval_task(self, task_func, task_id, task_name, **trigger_args):
		self.logger.debug('add interval task: {0}'.format(task_name))
		self.__task_scheduler.add_job(
			func=task_func, id=task_id, name=task_name, trigger='interval', **trigger_args)

	def add_cron_task(self, task_func, task_id, task_name, **trigger_args):
		self.logger.debug('add cron task: {0}'.format(task_name))
		self.__task_scheduler.add_job(
			func=task_func, id=task_id, name=task_name, trigger='cron', **trigger_args)

	def _get_task_scheduler(self):
		return self.__task_scheduler

	def stop_one_task(self, task_id):
		self.__task_scheduler.pause_job(task_id)

	def remove_one_task(self, task_id):
		self.__task_scheduler.remove_job(task_id)

	def reschedule_one_normal_task(self, task_id):
		self.__task_scheduler.reschedule_job(task_id)

	def stop_background_sched(self):
		self.logger.info('shutdown background scheduler')
		self.__task_scheduler.shutdown()



