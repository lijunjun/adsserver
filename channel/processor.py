# -*- coding: utf-8 -*-

__author__ = 'return'


'''
	processor的作用是类似于plugin模式的一种接口定义，用于支撑多processor的扩展
'''


class BasicProcessor(object):

	def run(self, task_count):
		raise BaseException('sub class should rewrite run')

	def stop(self):
		raise BaseException('sub class should rewrite stop')
