# -*- coding: utf-8 -*-

__author__ = 'return'


class BaseTask(object):
	def __init__(self):
		self.__is_running = False

	def run(self):
		try:
			self.__is_running = True
			self._run_task()
		finally:
			self.__is_running = False

	def _run_task(self):
		raise BaseException('sub class should rewrite _run_task')

	def stop(self):
		raise BaseException('sub class should rewrite stop')

	def is_running(self):
		return self.__is_running
