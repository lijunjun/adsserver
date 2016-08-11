__author__ = 'return'

from extendabletype import extendabletype


class ResourceMgr(object):
	__metaclass__ = extendabletype
	_instance = None

	def __init__(self):
		self.__resource_map = {}

	@classmethod
	def instance(cls):
		if cls._instance is None:
			cls._instance = ResourceMgr()
		return cls._instance

	def add_resource(self, type, obj):
		self.__resource_map[type] = obj

	def remove_resource(self, type):
		self.__resource_map.pop(type, None)

	def get_resource(self, type):
		if self.__resource_map.has_key(type):
			return self.__resource_map[type]
		return None

