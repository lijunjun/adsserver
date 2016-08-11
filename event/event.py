__author__ = 'return'


class BaseEvent(object):
	def __init__(self, type, content):
		self.__type = type
		self.__content = content
		self.__handled = False

	def get_type(self):
		return self.__type

	def get_content(self):
		return self.__content


