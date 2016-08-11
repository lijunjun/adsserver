# -*- coding: utf-8 -*-

__author__ = 'return'

from collections import namedtuple

'''
	Mongo数据库操作协议定义
'''

CreateCollectionRequest = namedtuple(
	'CreateCollectionRequest',
	'callback_id db collection operations'
)

FindDocRequest = namedtuple(
	'FindDocRequest',
	'callback_id db collection query fields limit seqflag sort seq_key read_pref hint skip'
)

InsertDocRequest = namedtuple(
	'InsertDocRequest',
	'callback_id db collection doc seqflag seq_key'
)

UpdateDocRequest = namedtuple(
	'UpdateDocRequest',
	'callback_id db collection query doc upset multi seqflag seq_key'
)


FindAndModifyDocRequest = namedtuple(
	'FindAndModifyDocRequest',
	'callback_id db collection query update fields upsert new seqflag seq_key'
)

DeleteDocRequest = namedtuple(
	'DeleteDocRequest',
	'callback_id db collection query seqflag seq_key'
)
