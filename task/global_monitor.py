# -*- coding: utf-8 -*-

__author__ = 'return'

import logging
from common.resource_mgr import ResourceMgr
from task import BaseTask
from etc import ads_const


'''
	全局监控模块
'''


class GlobalMonitor(BaseTask):
	def __init__(self):
		super(GlobalMonitor, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)

	def _run_task(self):
		self.logger.debug('start global monitor task')

		global_task_sched = ResourceMgr.instance().get_resource(ads_const.RESOURCE_TASK_SCHED)
		if not global_task_sched:
			self.logger.error('global task schedule is not running !!!')
			return

		# 检查log monitor是否在运行
		log_monitor_task = ResourceMgr.instance().get_resource(ads_const.RESOURCE_LOG_MONITOR)
		if not log_monitor_task.is_running():
			self.logger.error('log monitor task is not running.')
			log_monitor_task.stop()

			self.logger.info('restart log monitor task')
			log_monitor_task_name = log_monitor_task.__class__.__name__
			global_task_sched.add_normal_task(
				log_monitor_task.run, log_monitor_task_name, log_monitor_task_name)

		# TODO 每个processor的self maintenance

	def stop(self):
		pass


