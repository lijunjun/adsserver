# -*- coding: utf-8 -*-

__author__ = 'return'

'''
	异步callback的方法也是较为常见的一种方式
'''

basic_id = 7 << 20


class _RotatedIdGenerator(object):
	max_id = 2 ** 20

	def __init__(self):
		self._id = 1

	def gen_id(self):
		if self._id != _RotatedIdGenerator.max_id:
			self._id += 1
		else:
			self._id = 1
		return basic_id + self._id


_db_callbacks = {}
_db_id_gen = _RotatedIdGenerator()


def reg_db_callback(callback):
	callback_id = _db_id_gen.gen_id()
	_db_callbacks[callback_id] = callback
	return callback_id


def pop_db_callback(callback_id):
	return _db_callbacks.pop(callback_id, None)
