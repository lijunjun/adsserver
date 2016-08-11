# -*- coding: utf-8 -*-

__author__ = 'return'


import logging
import cb_mgr
import traceback


'''
	异步数据库操作的结果处理，主要是进行callback的回调
'''


class MongoResult(object):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)

	def db_find_doc_reply(self, callback_id, status, docs):
		self.__normal_db_reply_with_result(callback_id, status, docs)

	def db_update_doc_reply(self, callback_id, status):
		self.__normal_db_reply(callback_id, status)

	def db_delete_doc_reply(self, callback_id, status):
		self.__normal_db_reply(callback_id, status)

	def db_insert_doc_reply(self, callback_id, status, insert_id):
		callback = cb_mgr.pop_db_callback(callback_id)
		if callback is not None:
			try:
				callback(status, insert_id)
			except:
				# 重复插入就不需要在记录exception了
				pass

	def db_count_doc_reply(self, callback_id, status, count):
		self.__normal_db_reply_with_result(callback_id, status, count)

	def db_find_and_modify_doc_reply(self, callback_id, status, doc):
		self.__normal_db_reply_with_result(callback_id, status, doc)

	def db_run_procedure_reply(self, callback_id, status, doc):
		self.__normal_db_reply_with_result(callback_id, status, doc)

	def db_oper_index_reply(self, callback_id, status):
		self.__normal_db_reply(callback_id, status)

	def db_create_collection_reply(self, callback_id, status):
		self.__normal_db_reply(callback_id, status)

	def __normal_db_reply_with_result(self, callback_id, status, result):
		callback = cb_mgr.pop_db_callback(callback_id)
		if callback is not None:
			try:
				callback(status, result)
			except:
				self.logger.error(traceback.format_exc())

	def __normal_db_reply(self, callback_id, status):
		callback = cb_mgr.pop_db_callback(callback_id)
		if callback is not None:
			try:
				callback(status)
			except:
				self.logger.error(traceback.format_exc())

	def call_db_method_reply(self, callback_id, args):
		self.logger.debug("call_db_method_reply %d: %s", callback_id, args)
