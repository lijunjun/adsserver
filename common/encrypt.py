# -*- coding: utf-8 -*-

__author__ = 'return'


import hashlib

'''
	常用加密解密
'''


def md5_encode(str):
	mdf = hashlib.md5()
	mdf.update(str)
	return mdf.hexdigest()


