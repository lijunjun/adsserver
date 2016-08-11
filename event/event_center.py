# -*- coding: utf-8 -*-

__author__ = 'return'


import logging


'''
	中心事件调度器，本来想使用已有的框架来做，但是跟这里的需求不太契合比如blinker, gevent等等
	所以就简单的使用channel方法把event丢给相应的channel
'''


class EventCenter(object):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__sub_channel_map = {}

	def register_event_sub_channel(self, type, channel):
		if self.__sub_channel_map.has_key(type):
			self.logger.warning('type {0} has been added, it will be overwritten'.format(type))
		else:
			self.logger.info('add sub channel for event type {0}.'.format(type))
		self.__sub_channel_map[type] = channel

	def un_register_event_sub_channel(self, type):
		if not self.__sub_channel_map.has_key(type):
			self.logger.warning('type {0} is not registered !'.format(type))
		else:
			self.logger.info('un_register sub channel for event type {0}'.format(type))

		self.__sub_channel_map.pop(type, None)

	def dispatch_event(self, event):
		if event is None:
			return

		sub_channel = self.__sub_channel_map.get(event.get_type(), None)
		if sub_channel:
			sub_channel.produce_event(event)
		else:
			self.logger.error(
				'the event has no consuming channel ({0}: {1}). it will be discarded !'.format(event.get_type(), event.get_content()))
