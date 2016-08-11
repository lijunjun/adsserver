# -*- coding: utf-8 -*-

__author__ = 'return'


import logging
import traceback

from common import encrypt
from etc import ads_const
from task.task import BaseTask


class CollectUserIDFA(BaseTask):
	def __init__(self, processor_channel):
		super(CollectUserIDFA, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__processor_channel = processor_channel
		self.__stop_collect_user_idfa = False
		self.__idfa = open('idfa_list.log', 'w+')

	def __check_activate_request(self, query_paras):
		if query_paras:
			active_part_string = query_paras.get(ads_const.ADS_SOURCE_ACTIVATE_TYPE, None)
			active_sign = query_paras.get(ads_const.ADS_SOURCE_ACTIVATE_SIGN_TYPE, None)
			active_sign_key = ads_const.ADS_SOURCE_ACTIVATE_SIGN_KEY

			if active_part_string and active_sign:
				active_string = active_part_string[0] + active_sign_key
				if active_string and active_sign[0] == encrypt.md5_encode(active_string).upper():
					return True

		return False

	def _run_task(self):
		while not self.__stop_collect_user_idfa:
			try:
				event = self.__processor_channel.consume_one_event()
				query_paras = event.get_content()

				if not self.__check_activate_request(query_paras):
					self.logger.error('failed to check sign {0}'.format(query_paras))
					continue

				if query_paras:
					idfa = query_paras.get(ads_const.ADS_SOURCE_PARA_IDFA, None)
					time = query_paras.get(ads_const.ADS_SOURCE_PARA_TIME, None)

					if idfa and time:
						self.__idfa.write(time + ',' + idfa[0] + '\n')
						self.__idfa.flush()

			except:
				self.logger.error(traceback.format_exc())

	def stop(self):
		self.logger.info('stop activating game task')
		self.__stop_collect_user_idfa = True
