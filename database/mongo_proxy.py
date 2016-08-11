# -*- coding: utf-8 -*-

__author__ = 'return'


import logging
import cb_mgr
from etc import ads_const


'''
	用一层proxy进行封装，由proxy跟数据库driver进行交互
'''


class MongoDBProxy(object):

	def __init__(self, db_service, db_name):
		super(MongoDBProxy, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.dbstub = db_service
		self.connected = True
		self.db_name = db_name
		self.dbstatus = ads_const.STATUS_NOT_CONNECTED

	def db_ready_to_send(self):
		return self.dbstatus == ads_const.STATUS_DB_CONNECTED

	def db_find_doc(self, collection, query, fields = None, limit = 1,
					callback = None, seqflag = False, sort = None, seq_key = None, read_pref = None, hint=None, skip=None):
		bson_fields = {"f": fields} if fields is not None else ''
		bson_sort = {"s":sort} if sort is not None else ''
		real_seq_key = seq_key if seq_key is not None else ''
		real_read_pref = read_pref if read_pref is not None else 0
		real_hint = {"h": hint} if hint is not None else ''
		real_skip = {"skip": skip} if skip is not None else ''
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1

		self.dbstub.db_find_doc(callback_id, self.db_name, collection, query,
			bson_fields, limit, seqflag, bson_sort, real_seq_key, real_read_pref, real_hint, real_skip)

	def db_update_doc(self, collection, query, doc, callback= None, upset = True,
		multi = False, seqflag = False, seq_key = None):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1

		self.dbstub.db_update_doc(callback_id, self.db_name, collection, query,
			doc, upset, multi, seqflag, seq_key if seq_key is not None else '')

	def db_oper_index(self, collection, optype, index, desc, callback=None):
		query = {'i': index} if index is not None else ''
		desc = desc if desc is not None else ''
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1

		self.dbstub.db_oper_index(callback_id, self.db_name, collection,
			optype, query, desc)

	def db_delete_doc(self, collection, query, callback = None, seqflag = False, seq_key = None):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1
		
		self.dbstub.db_delete_doc(callback_id, self.db_name, collection,
			query, seqflag, seq_key if seq_key is not None else '')

	def db_insert_doc(self, collection, doc, callback = None, seqflag = False, seq_key = None):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1
		
		if isinstance(doc, list):
			self.dbstub.db_insert_doc(callback_id, self.db_name, collection,
				{"__batch__":doc}, seqflag, seq_key if seq_key is not None else '')
		else:
			self.dbstub.db_insert_doc(callback_id, self.db_name, collection,
				doc, seqflag, seq_key if seq_key is not None else '')

	def db_count_doc(self, collection, query={}, callback=None):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1
		self.dbstub.db_count_doc(callback_id, self.db_name, collection, query)

	def db_find_and_modify_doc(self, collection, query, update = None, fields = None, upsert = False,
		new = False, callback = None, seqflag = False, seq_key = None):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1
		bson_fields = {"f": fields} if fields is not None else ''
		bson_update = {"u":update} if update is not None else ''

		self.dbstub.db_find_and_modify_doc(callback_id, self.db_name, collection,
			query, bson_update, bson_fields, upsert, new, seqflag, seq_key if seq_key is not None else '')

	def db_run_procedure(self, procname, callback, *args, **kwargs):
		bson_args = {"a": args} if args else ''
		bson_kwargs = kwargs
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1

		self.dbstub.db_run_procedure(callback_id, self.db_name, procname, bson_args, bson_kwargs)

	def db_create_collection(self, collection_name, opts, callback):
		callback_id = cb_mgr.reg_db_callback(callback) if callback is not None else -1

		self.dbstub.db_create_collection(callback_id, self.db_name, collection_name, opts)

	def db_reload_proc(self):
		self.dbstub.db_reload_proc()

	def call_db_method(self, method, args, callback_id):
		self.dbstub.call_db_method(method, args, callback_id)
