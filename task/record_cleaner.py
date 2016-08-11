# -*- coding: utf-8 -*-

__author__ = 'return'


import logging
import time

from task import BaseTask
from etc import ads_const
from database.db_proxy_factory import DBProxyFactory


'''
	定时清理陈旧数据
'''


class RecordCleaner(BaseTask):
	def __init__(self):
		super(RecordCleaner, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__db_client = DBProxyFactory.instance().create_mongo_db_proxy()

	def _run_task(self):
		self.logger.debug('start record cleaner task')

		record_limit_time = int(time.time()) - ads_const.ADS_CLEANUP_TIME_LIMIT

		self.__db_client.db_delete_doc(
			ads_const.ADS_SOURCE_COLLECTION_ADS,
			{
				ads_const.ADS_SOURCE_COLLECTION_TIME:
					{
						"$lt": record_limit_time
					}
			},
			callback=lambda status: self.logger.info('status of deleting old item: {0}'.format(status))
		)

		# self.logger.debug('stop record cleaner task')

	def stop(self):
		pass