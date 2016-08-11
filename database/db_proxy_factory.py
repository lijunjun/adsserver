# -*- coding: utf-8 -*-

__author__ = 'return'

import logging

from common.extendabletype import extendabletype
from common.resource_mgr import ResourceMgr
from database.mongo_proxy import MongoDBProxy
from database.mongo_result import MongoResult
from etc import ads_const


'''
	用户创建数据库代理，隐藏所有的DB底层接口，上层使用的时候可以方便的进行异步DB操作
'''


class DBProxyFactory(object):
	__metaclass__ = extendabletype
	_instance = None

	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)

	@classmethod
	def instance(cls):
		if cls._instance is None:
			cls._instance = DBProxyFactory()
		return cls._instance

	def create_mongo_db_proxy(self):
		mongodb_manager = ResourceMgr.instance().get_resource(ads_const.RESOURCE_MONGO_DB_MANAGER)
		if mongodb_manager is None:
			self.logger.error('mongodb manager is not created !')
			return None

		if ads_const.ADS_DB_NAME is None:
			self.logger.error('database name is not defined !')
			return None

		mongodb_service = mongodb_manager.create_db_service()
		mongodb_service.register_result_client(MongoResult())

		self.logger.info('one db proxy is created')

		return MongoDBProxy(mongodb_service, ads_const.ADS_DB_NAME)
