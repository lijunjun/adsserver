# -*- coding: utf-8 -*-

__author__ = 'return'

from Queue import Queue


class BasicChannel(object):
	def __init__(self):
		self.__channel_queue = Queue()

	def produce_event(self, event):
		self.__channel_queue.put(event)

	def consume_one_event_with_timeout(self, timeout):
		return self.__channel_queue.get(timeout=timeout)

	def consume_one_event(self):
		return self.__channel_queue.get()

	def consume_one_event_without_wait(self):
		return self.__channel_queue.get_nowait()

	def clear_all(self):
		self.__channel_queue.empty()