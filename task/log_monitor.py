# -*- coding: utf-8 -*-

__author__ = 'return'

import subprocess
import logging
import traceback
import time
import random
import sys

from etc import ads_const
from task import BaseTask


'''
	用户实时读取http的log, 转化成对应的事件交由事件中心处理
'''


class LogMonitor(BaseTask):
	def __init__(self, log_parser, log_path, event_center):
		super(LogMonitor, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__log_reader_process = None
		self.__log_file_handler = None
		self.__event_center = None
		self.__stop_log_monitor = False
		self.__log_parser = log_parser
		self.__log_path = log_path
		self.__event_center = event_center

	def register_event_center(self, event_center):
		self.__event_center = event_center

	def _run_task(self):
		try:
			self.__stop_log_monitor = False

			if sys.platform.startswith('win'):
				self.__log_file_handler = open(self.__log_path)
			else:
				self.__log_reader_process = subprocess.Popen(
					'tail -n 1 -f ' + self.__log_path,
					stdout=subprocess.PIPE,
					stderr=subprocess.PIPE,
					shell=True)

				self.logger.info(
					'log monitoring process (pid: {0}) is launched'.
						format(self.__log_reader_process.pid))

				if self.__log_reader_process.returncode is not None:
					self.logger.error('sub process is not launched successfully.'
									  ' return code is {0}'.format(self.__log_reader_process.returncode))
					return

			self.__log_handler()

		except Exception as e:
			self.logger.error(traceback.format_exc())

	def __log_handler(self):
		self.logger.debug('start log handler')

		while not self.__stop_log_monitor:
			try:
				if sys.platform.startswith('win'):
					line = self.__log_file_handler.readline()
				else:
					line = self.__log_reader_process.stdout.readline().strip()

				if not line:
					time.sleep(ads_const.ADS_READ_LOG_SLEEP_TIME)
					continue

				if line:
					log_event = self.__log_parser.parse(line)
					if log_event:
						self.__event_center.dispatch_event(log_event)

			except Exception as e:
				if line:
					self.logger.error('unhandled request: {0}'.format(line))
				self.logger.error(traceback.format_exc())

		self.logger.debug('end of log handler')

	def stop(self):
		self.logger.info('stop log monitor')
		self.__stop_log_monitor = True
		if self.__log_reader_process:
			self.__log_reader_process.kill()
		if self.__log_file_handler:
			self.__log_file_handler.close()

	def restart_log_monitor(self):
		self.logger.info('restart log monitor')
		self.stop_log_monitor()
		self.start_log_monitor()
