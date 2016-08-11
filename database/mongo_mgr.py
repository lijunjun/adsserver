# -*- coding: utf-8 -*-

__author__ = 'return'

import os
import sys
import time
import random
import logging
import mongo_protocol
import traceback

from etc import ads_const
from common import timer
from collections import deque
from pymongo.mongo_client import MongoClient
from pymongo.errors import DuplicateKeyError, CollectionInvalid
from ThreadPool.threadpool import ThreadPool, WorkRequest, NoResultsPending
from pymongo.errors import OperationFailure
from pymongo.errors import AutoReconnect


'''
	mongodb操作，通过线程池异步执行db操作，根据回调id进行回调，并没有尝试使用Motor
	这里并没有做更加细致的优化, 如果说成为一个独立的module可能也需要增加些内容
'''


_FIND_DOC_OP = 1
_UPDATE_DOC_OP = 2
_DELETE_DOC_OP = 3
_INSERT_DOC_OP = 4
_COUNT_DOC_OP = 5
_FIND_AND_MODIFY_DOC_OP = 6
_RUN_PROCEDURE = 7
_ENSURE_INDEX = 8
_DROP_INDEX = 9
_RESET_INDEX = 10
_CREATE_COLLECTION = 11


class MongoClients(object):
	def __init__(self, mongoconfig, dbconfig):
		super(MongoClients, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)

		self.mongoconfig = mongoconfig
		self.connct_to_mongo = False
		self.mongoclient = None
		# self.connect_mongo()
		self.request_queue = deque()
		self.reply_queue = deque()

		self.requestthreadpool = ThreadPool(3)

		requesttime = dbconfig.get(ads_const.ADS_CONFIG_DATABASE_REQUEST_TIME, 0.02)
		timer.addRepeatTimer(requesttime, self.request_callback)

		# 游戏逻辑自定义的存储过程
		self.user_defined_proc_dict = {}
		self.db_except_msg = (
			'no master found',
			'error querying server',
			'transport error',
			'could not contact primary',
			'write results unavailable',
			'not master',
		)

	def request_callback(self):
		try:
			self.requestthreadpool.poll()
		except NoResultsPending:
			pass

	# test only
	def drop_collection(self, db, collection):
		self.mongoclient[db][collection].drop()

	def _init_proc(self):
		# CLEAR OLD MODULE
		for procname, module in self.user_defined_proc_dict.iteritems():
			if module.__name__ in sys.modules:
				del sys.modules[module.__name__]

		self.user_defined_proc_dict = {}

		procpath = self.mongoconfig.get('proc', None)
		moduleroot = self.mongoconfig.get('module_root', None)
		if not procpath or not moduleroot:
			return
		procpath = os.path.join(os.getcwd(), procpath)
		files = os.listdir(procpath)
		for filename in files:
			try:
				if filename.endswith('.py'):
					tail_count = 3
				elif filename.endswith('.pyc'):
					tail_count = 4
				else:
					continue
				if filename.startswith('__'):
					continue
				fullname = os.path.join(procpath, filename)
				if os.path.isdir(fullname):
					continue
				procname = filename[:-tail_count]
				proc = __import__('%s.%s' % (moduleroot, procname), fromlist = [''])
				self.user_defined_proc_dict[procname] = proc
			except:
				self.logger.warn('init proc error. procname: %s', procname)
				self.logger.error(traceback.format_exc())
		self.logger.info('available proc list:')
		for key, val in self.user_defined_proc_dict.iteritems():
			self.logger.info(' ** %s : %s', str(key), str(val))

	def connect_mongo(self):
		mongo_ip = self.mongoconfig["ip"]
		mongo_port = self.mongoconfig["port"]
		username = self.mongoconfig.get('user', None)
		password = self.mongoconfig.get('pwd', None)

		database = self.mongoconfig.get('db', None)
		ads_const.ADS_DB_NAME = database

		wmode = self.mongoconfig.get("wmode", 1)

		try:
			self.mongoclient = MongoClient(mongo_ip, mongo_port, w = wmode)
			if username:
				res = self.mongoclient.admin.authenticate(username, password, database)
				if not res:
					self.logger.error('mongodb authentication failed!')
					raise
			self.connct_to_mongo = True
			ads_const.IS_MONGO_DB_CONNECTED = True
			self.logger.info("successed connected to mongodb")
		except:
			self.logger.error(traceback.format_exc())
			self.logger.error("couldn't connected to mongodb, please check the db server as soon as possible")
			timer.addTimer(1, self.connect_mongo)

		try:
			self._init_proc()
			self.logger.info('successfully init proc')
		except:
			self.logger.warn('FAILED to init proc')
			self.logger.error(traceback.format_exc())

	def get_db_status(self):
		if self.connct_to_mongo:
			status = ads_const.STATUS_DB_CONNECTED
		else:
			status = ads_const.STATUS_CONNECTION_FAILED
		return status

	def _db_op_reply(self, request, result):
		pass

	def _db_op_except(self, request, exc_info):
		if not isinstance(exc_info, tuple):
			# Something is seriously wrong...
			print request
			print exc_info
			raise SystemExit
		print "**** Exception occured in request #%s: %s" % \
		  (request.requestID, exc_info)

	def _do_db_op_callback(self, request, result, opcallback):
		if result:
			try:
				opcallback(result[0], result[1])
			except:
				self.logger.warn("send_callback error")
				self.logger.error(traceback.format_exc())

	def do_db_op(self, optype, oprequest, opcallback):
		if self.connct_to_mongo:
			request = WorkRequest(self._do_db_op, (optype, oprequest),
								 callback = lambda request, result:self._do_db_op_callback(request, result, opcallback))
			self.requestthreadpool.putRequest(request)
			return True
		else:
			self.logger.warn("mongo client was not connected")
			self._do_db_op_callback(None, (False, None), opcallback)
			return False

	def _do_db_op(self, optype, oprequest):
		while 1:
			db_name = oprequest.db
			if optype != _RUN_PROCEDURE and optype != _CREATE_COLLECTION:
				collection_name = oprequest.collection
				collection = self.mongoclient[db_name][collection_name]

				if optype != _INSERT_DOC_OP and optype != _DROP_INDEX:
					#操作不为insert时，必须有query项
					query = oprequest.query
					if query == None:
						if optype != _COUNT_DOC_OP:
							self.logger.error("warning the request decode failed")
							return False, None

			if optype == _FIND_DOC_OP:
				fields = None
				if hasattr(oprequest, "fields") and oprequest.fields:
					fieldsdict = oprequest.fields
					fields = fieldsdict.get("f", None)
				sort = None
				if hasattr(oprequest, "sort") and oprequest.sort:
					sortdict = oprequest.sort
					sort = sortdict.get("s", None)
				kwargs = {}
				if hasattr(oprequest, 'read_pref') and oprequest.read_pref:
					kwargs.update({'read_preference':oprequest.read_pref})
				hint = None
				if hasattr(oprequest, 'hint') and oprequest.hint:
					hintdict = oprequest.hint
					hint = hintdict.get("h", None)
				skip = None
				if hasattr(oprequest, 'skip') and oprequest.skip:
					skipdict = oprequest.skip
					skip = skipdict.get("skip", None)
				try:
					if hint:
						if skip:
							findresult = list(collection.find( query, fields, 0, int(oprequest.limit), True, False, False, sort, **kwargs).skip(skip).hint(hint))
						else:
							findresult = list(collection.find( query, fields, 0, int(oprequest.limit), True, False, False, sort, **kwargs).hint(hint))
					else:
						if skip:
							findresult = list(collection.find( query, fields, 0, int(oprequest.limit), True, False, False, sort, **kwargs).skip(skip))
						else:
							findresult = list(collection.find( query, fields, 0, int(oprequest.limit), True, False, False, sort, **kwargs))
					return True, findresult
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _COUNT_DOC_OP:
				try:
					if query == None or len(query) == 0:
						count = collection.count()
					else:
						kwargs = {}	
						count = collection.find(query, **kwargs).count()
					return True, count
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _UPDATE_DOC_OP:
				doc = oprequest.doc
				upsert = oprequest.upset
				multi = oprequest.multi
				try:
					collection.update( query, doc, upsert = upsert, multi= multi)
					return True, None
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _DELETE_DOC_OP:
				try:
					collection.remove( query )
					return True, None
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _INSERT_DOC_OP:
				doc = oprequest.doc
				try:
					if len(doc) == 1 and doc.has_key("__batch__"):
						ret_id = collection.insert(doc["__batch__"])
					else:
						ret_id = collection.insert(doc)
					if ret_id:
						return True, ret_id
					else:
						return True, None
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except Exception, e:
					if isinstance(e, DuplicateKeyError):
						self.logger.warn("insert DuplicateKeyError with collection %s and id %s", collection_name, doc['_id'])
					else:
						self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _FIND_AND_MODIFY_DOC_OP:
				fields = None
				if hasattr(oprequest, "fields") and oprequest.fields:
					fieldsdict = oprequest.fields
					fields = fieldsdict.get("f", None)
				update = None
				if hasattr(oprequest, "update"):
					updatedict = oprequest.update
					update = updatedict.get("u", None)
				try:
					upsert = oprequest.upsert
					new = oprequest.new
					findresult = collection.find_and_modify( query, update = update, fields = fields, upsert = upsert, new = new)
					return True, findresult
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _RUN_PROCEDURE:
				args = []
				kwargs = {}
				if hasattr(oprequest, 'args') and oprequest.args:
					argsdict = oprequest.args
					args = argsdict.get("a", [])
				if hasattr(oprequest, 'kwargs') and oprequest.kwargs:
					kwargs = oprequest.kwargs

				procname = oprequest.proc
				if not procname in self.user_defined_proc_dict:
					return False, None
				try:
					proc = self.user_defined_proc_dict[procname]
					result = proc.run(self.mongoclient[db_name], *args, **kwargs)
					return True, result
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _CREATE_COLLECTION:
				collection_name = oprequest.collection
				try:
					if hasattr(oprequest, 'operations') and oprequest.operations:
						opts = oprequest.operations
					else:
						opts = {}
					db = self.mongoclient[db_name]
					db.create_collection(collection_name, **opts)
					status = ads_const.CREATE_COLLECTION_CREATE_SUCC
					return status, None
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except CollectionInvalid:
					status = ads_const.CREATE_COLLECTION_ALREADY_EXISTED
					return status, None
				except Exception, e:
					status = ads_const.CREATE_COLLECTION_CREATE_FAILED
					return status, None

			elif optype == _ENSURE_INDEX or optype == _RESET_INDEX:
				if optype == _RESET_INDEX:
					collection.drop_indexes()
				try:
					#c.test.blog.ensure_index([("lv", -1,)], unique=True, drop_dups=True)
					index = oprequest.query
					if isinstance(index['i'], basestring):
						index = index['i']
					else:
						index = map(tuple, index['i'])
					desc = oprequest.desc
					result = collection.ensure_index(index, **desc)
					return True, result
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None

			elif optype == _DROP_INDEX:
				try:
					result = collection.drop_indexes()
					return True, result
				except OperationFailure, e:
					if self.should_retry(e):
						self.logger.warn(e.message)
					else:
						self.logger.error(traceback.format_exc())
						return False, None
				except AutoReconnect, e:
					self.logger.warn('AutoReconnect')
				except:
					self.logger.error(traceback.format_exc())
					return False, None
			time.sleep(1)

	def should_retry(self, ex):
		msg = ex.message
		for exec_msg in self.db_except_msg:
			if exec_msg in msg:
				return True
		return False

	def report_failure(self, optype, oprequest, clientproxy):
		self.reply_queue.append( (optype, oprequest, False, clientproxy, None ) )

	def get_db_result(self):
		try:
			return self.reply_queue.popleft()
		except IndexError:
			return None

class MongoDBServiceManager(object):
	def __init__(self, mongoconfig, dbconfig):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.mongoconfig = mongoconfig

		self.db_op_sequence_map = {}

		self.mongoclients = MongoClients(self.mongoconfig, dbconfig)
		self.mongoclients.connect_mongo()

		self.last_db_status =  self.mongoclients.get_db_status()

	def create_db_service(self):
		return MongoDBService(self)

	def send_callback(self, optype, oprequest, clientproxy, opstatus, result):
		db_status = self.mongoclients.get_db_status()
		if db_status != self.last_db_status:
			self.last_db_status = db_status

		if optype == _FIND_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_find_doc_reply(oprequest.callback_id, opstatus, result)
		elif optype == _UPDATE_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_update_doc_reply(oprequest.callback_id, opstatus)
		elif optype == _DELETE_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_delete_doc_reply(oprequest.callback_id, opstatus)
		elif optype == _INSERT_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_insert_doc_reply(oprequest.callback_id, opstatus, result)
		elif optype == _COUNT_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_count_doc_reply(oprequest.callback_id, opstatus, result)
		elif optype == _FIND_AND_MODIFY_DOC_OP:
			if oprequest.callback_id > 0:
				clientproxy.db_find_and_modify_doc_reply(oprequest.callback_id, opstatus, result)
		elif optype == _RUN_PROCEDURE:
			if oprequest.callback_id > 0:
				clientproxy.db_run_procedure_reply(oprequest.callback_id, opstatus, result)
		elif optype == _ENSURE_INDEX or optype == _RESET_INDEX or optype == _DROP_INDEX:
			if oprequest.callback_id > 0:
				clientproxy.db_oper_index_reply(oprequest.callback_id, opstatus)
		elif optype == _CREATE_COLLECTION:
			if oprequest.callback_id > 0:
				clientproxy.db_create_collection_reply(oprequest.callback_id, opstatus)

		if getattr(oprequest, 'seqflag', False):
			db = oprequest.db
			if not db in self.db_op_sequence_map:
				return

			collection = oprequest.collection
			if not collection in self.db_op_sequence_map[db]:
				return

			seq_key = self._get_request_seq_key(oprequest)
			if seq_key is None:
				return

			query = oprequest.query
			seq_val = query[seq_key]
			seq_pair = (seq_key, seq_val)
			if not seq_pair in self.db_op_sequence_map[db][collection]:
				return
			q = self.db_op_sequence_map[db][collection][seq_pair]
			if len(q) > 0:
				q.popleft()
				if len(q) > 0:
					optype, request, clientproxy = q[0]
					self._do_mongoclient_op(optype, request, clientproxy)

	def _get_request_seq_key(self, request):
		if request.HasField("seq_key") and request.seq_key:
			seq_key = request.seq_key
		else:
			seq_key = '_id'
		return seq_key

	def _do_mongoclient_op(self,  optype, oprequest, clientproxy):
		if not self.mongoclients.do_db_op(optype, oprequest,
			lambda state, result:self.send_callback(optype, oprequest, clientproxy, state, result)):
			self.logger.warn("do_db_op failed with op %d ", optype)

	def _do_reload_proc(self):
		self.mongoclients._init_proc()


class MongoDBService(object):
	def __init__(self, manager):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.manager = manager
		self.__result_client = None

	def register_result_client(self, result_client):
		self.__result_client = result_client

	def db_create_collection(self, callback_id, db, collection, operations):

		request = mongo_protocol.CreateCollectionRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			operations=operations
		)

		self.manager._do_mongoclient_op(_CREATE_COLLECTION, request, self.__result_client)

	def db_find_doc(self, callback_id, db,
					collection, query, fields, limit, seqflag, seq_key, sort, read_pref, hint, skip):
		
		request = mongo_protocol.FindDocRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			query=query,
			fields=fields,
			limit=limit,
			seqflag=seqflag,
			sort=sort,
			seq_key=seq_key,
			read_pref=read_pref,
			hint=hint,
			skip=skip
		)

		self.manager._do_mongoclient_op(_FIND_DOC_OP, request, self.__result_client)

	# def db_oper_index(self, callback_id, db, collection, typ, query, desc):
	# 	request = dbmanager_pb2.OperIndexRequest()
	# 	request.callback_id = callback_id
	# 	request.db = db
	# 	request.collection = collection
	# 	request.type = typ
	# 	request.query = query
	# 	request.desc = desc
	#
	# 	clientproxy =  self.manager.clientproxies[self.server_info_holder]
	# 	if request.type == dbmanager_pb2.OperIndexRequest.ENSURE:
	# 		self.manager._do_mongoclient_op(_ENSURE_INDEX, request, clientproxy)
	# 	elif request.type == dbmanager_pb2.OperIndexRequest.DROP:
	# 		self.manager._do_mongoclient_op(_DROP_INDEX, request, clientproxy)
	# 	elif request.type == dbmanager_pb2.OperIndexRequest.RESET:
	# 		self.manager._do_mongoclient_op(_RESET_INDEX, request, clientproxy)
	#
	# def db_count_doc(self, callback_id, db, collection, query):
	# 	#self.logger.info("db_find_doc")
	# 	request = dbmanager_pb2.CountDocRequest()
	# 	request.callback_id = callback_id
	# 	request.db = db
	# 	request.collection = collection
	# 	request.query = query
	# 	clientproxy =  self.manager.clientproxies[self.server_info_holder]
	# 	self.manager._do_mongoclient_op(_COUNT_DOC_OP, request, clientproxy)
	#

	def db_update_doc(self, callback_id, db, collection, query, doc, upset, multi, seqflag, seq_key):
		request = mongo_protocol.UpdateDocRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			query=query,
			doc=doc,
			upset=upset,
			multi=multi,
			seqflag=seqflag,
			seq_key=seq_key,
		)

		self.manager._do_mongoclient_op(_UPDATE_DOC_OP, request, self.__result_client)

	def db_delete_doc(self, callback_id, db, collection, query, seqflag, seq_key):
		request = mongo_protocol.DeleteDocRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			query=query,
			seqflag=seqflag,
			seq_key=seq_key
		)

		self.manager._do_mongoclient_op(_DELETE_DOC_OP, request, self.__result_client)
	
	def db_insert_doc(self, callback_id, db, collection, doc, seqflag, seq_key):
		request = mongo_protocol.InsertDocRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			doc=doc,
			seqflag=seqflag,
			seq_key=seq_key
		)
		
		self.manager._do_mongoclient_op(_INSERT_DOC_OP, request, self.__result_client)

	def db_find_and_modify_doc(self, callback_id, db, collection, query,
							   update, fields, upsert, new, seqflag, seq_key):
		request = mongo_protocol.FindAndModifyDocRequest(
			callback_id=callback_id,
			db=db,
			collection=collection,
			query=query,
			update=update,
			fields=fields,
			upsert=upsert,
			new=new,
			seqflag=seqflag,
			seq_key=seq_key,
		)

		self.manager._do_mongoclient_op(_FIND_AND_MODIFY_DOC_OP, request, self.__result_client)

	# def db_run_procedure(self, callback_id, db, proc, args, kwargs):
	# 	request = dbmanager_pb2.RunProcedureRequest()
	# 	request.callback_id = callback_id
	# 	request.db = db
	# 	request.proc = proc
	# 	request.args = args
	# 	request.kwargs = kwargs
	# 	clientproxy =  self.manager.clientproxies[self.server_info_holder]
	# 	self.manager._do_mongoclient_op(_RUN_PROCEDURE, request, clientproxy)
	#
	# def db_reload_proc(self):
	# 	self.logger.debug("db_reload_proc...")
	# 	self.manager._do_reload_proc()
	#
	# def call_db_method(self, method, args, callback_id):
	# 	self.logger.debug("call_db_method %s:%s %d", method, args, callback_id)
	# 	self.dispatch_rpc("send_call_db_method_reply", callback_id, args)
