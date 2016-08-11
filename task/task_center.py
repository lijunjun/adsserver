# -*- coding: utf-8 -*-

__author__ = 'return'

import logging
import traceback

from log_monitor import LogMonitor
from log_parser import LogParser
from etc import ads_const
from task_sched import TaskSched
from record_cleaner import RecordCleaner
from channel.activate_processor import ActivateProcessor
from channel.game_processor import GameProcessor
from channel.collect_processor import CollectProcessor
from event.event_center import EventCenter
from common.resource_mgr import ResourceMgr
from global_monitor import GlobalMonitor


class TaskCenter(object):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)

	def init_ads_tasks(self):

		try:

			# 初始化全局任务调度器
			global_task_sched = TaskSched()
			ResourceMgr.instance().add_resource(
				ads_const.RESOURCE_TASK_SCHED, global_task_sched)

			# 初始化全局事件调度器
			global_event_dispatcher = EventCenter()
			ResourceMgr.instance().add_resource(
				ads_const.RESOURCE_EVENT_CENTER, global_event_dispatcher)

			# 初始化点击广告任务
			game_processor = GameProcessor()
			ResourceMgr.instance().add_resource(
				ads_const.RESOURCE_GAME_PROCESSOR, game_processor)
			game_processor.run(ads_const.ADS_GAME_USER_PROCESSOR_TASK_COUNT)

			# 初始化激活任务
			if ads_const.ADS_COLLECT_USER_IDFA <= 0:
				activate_processor = ActivateProcessor()
				ResourceMgr.instance().add_resource(
					ads_const.RESOURCE_ACTIVATE_PROCESSOR, activate_processor)
				activate_processor.run(ads_const.ADS_ACTIVATE_PROCESSOR_TASK_COUNT)
			else:
				collect_processor = CollectProcessor()
				ResourceMgr.instance().add_resource(
					ads_const.RESOURCE_COLLECT_PROCESSOR, collect_processor)
				collect_processor.run(1)

			# 初始化log处理任务
			log_monitor = LogMonitor(
				LogParser(), ads_const.ADS_SYSLOG_PATH, global_event_dispatcher)
			ResourceMgr.instance().add_resource(
				ads_const.RESOURCE_LOG_MONITOR, log_monitor)
			log_monitor_task_name = log_monitor.__class__.__name__
			global_task_sched.add_normal_task(
				log_monitor.run, log_monitor_task_name, log_monitor_task_name)

			# 初始化监控任务
			global_monitor = GlobalMonitor()
			global_monitor_task_name = global_monitor.__class__.__name__
			global_task_sched.add_interval_task(
				global_monitor.run,
				global_monitor_task_name,
				global_monitor_task_name,
				minutes=ads_const.ADS_GLOBAL_MONITOR_INTERVAL
			)

			# 初始化数据库清理任务
			# record_cleaner = RecordCleaner()
			# record_cleaner_task_name = record_cleaner.__class__.__name__
			# global_task_sched.add_interval_task(
			# 	record_cleaner.run,
			# 	record_cleaner_task_name,
			# 	record_cleaner_task_name,
			# 	hours=ads_const.ADS_CLEANUP_TIME_INTERVAL
			# 	)

		except Exception as e:
			self.stop_all_ads_task()
			self.logger.error(traceback.format_exc())
			raise e

	def stop_all_ads_task(self):
		try:
			log_monitor_task = ResourceMgr.instance().get_resource(ads_const.RESOURCE_LOG_MONITOR)
			if log_monitor_task:
				log_monitor_task.stop()

			game_processor = ResourceMgr.instance().get_resource(ads_const.RESOURCE_GAME_PROCESSOR)
			if game_processor:
				game_processor.stop()

			activate_processor = ResourceMgr.instance().get_resource(ads_const.RESOURCE_ACTIVATE_PROCESSOR)
			if activate_processor:
				activate_processor.stop()

			global_task_sched = ResourceMgr.instance().get_resource(ads_const.RESOURCE_TASK_SCHED)
			if global_task_sched:
				global_task_sched.stop_background_sched()
		except Exception as e:
			self.logger.error(traceback.format_exc())
