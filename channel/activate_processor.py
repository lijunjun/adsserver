# -*- coding: utf-8 -*-

__author__ = 'return'

import logging

from channel import BasicChannel
from activate_game import ActivateGame
from common.resource_mgr import ResourceMgr
from processor import BasicProcessor
from etc import ads_const

'''
	游戏激活子模块, 一个channel，一个或多个处理任务
'''


class ActivateProcessor(BasicProcessor):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__activate_channel = BasicChannel()
		self.__activate_processor_tasks = {}
		self.__processor_task_count = 1

	def __get_global_res(self):
		global_task_sched = ResourceMgr.instance().get_resource(ads_const.RESOURCE_TASK_SCHED)
		if global_task_sched is None:
			raise BaseException('global task scheduler is not initialized')

		global_task_center = ResourceMgr.instance().get_resource(ads_const.RESOURCE_EVENT_CENTER)
		if global_task_center is None:
			raise BaseException('global event center is not initialized')

		return global_task_sched, global_task_center

	def run(self, task_count):
		self.__processor_task_count = task_count
		if self.__processor_task_count < 1:
			raise BaseException('processor task count should not less than one !')

		global_task_sched, global_task_center = self.__get_global_res()

		global_task_center.register_event_sub_channel(
				ads_const.EVENT_TYPE_ACTIVATE_EVENT, self.__activate_channel)

		self.logger.info('activate task count: {0}'.format(self.__processor_task_count))
		for i in xrange(self.__processor_task_count):
			activate_game_task = ActivateGame(self.__activate_channel)
			activate_game_task_name = activate_game_task.__class__.__name__ + '_' + str(i)
			global_task_sched.add_normal_task(
				activate_game_task.run, activate_game_task_name, activate_game_task_name)

			self.__activate_processor_tasks[activate_game_task_name] = activate_game_task

	def stop(self):
		self.logger.info('stop activate processor')

		global_task_sched, global_task_center = self.__get_global_res()

		global_task_center.un_register_event_sub_channel(ads_const.EVENT_TYPE_ACTIVATE_EVENT)

		for task_id, activate_game_task in self.__activate_processor_tasks.iteritems():
			global_task_sched.stop_one_task(task_id)
			if activate_game_task:
				activate_game_task.stop()


