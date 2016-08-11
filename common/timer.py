# -*- coding: utf-8 -*-

__author__ = 'return'

from libs import asyncore_with_timer


def addTimer(delay, func, *args, **kwargs):
		return asyncore_with_timer.CallLater(delay, func, *args, **kwargs)


def addRepeatTimer(delay, func, *args, **kwargs):
	return asyncore_with_timer.CallEvery(delay, func, *args, **kwargs)


def tickTimer():
	pass


'''
	进入定时器循环
'''


def loops(sleep=0.02, count=None):
	asyncore_with_timer.loop_task(sleep, count)

