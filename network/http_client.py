# -*- coding: utf-8 -*-

__author__ = 'return'


import requests
import os


'''
	简单的一个http client，如果网络比较复杂，实现一下异步http请求
'''


def http_get(url):
	return requests.get(url)


def http_get_async(url, callback=None):
	pass