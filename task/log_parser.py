# -*- coding: utf-8 -*-

__author__ = 'return'

import logging
import traceback
import re
import datetime

from urlparse import urlparse
from urlparse import parse_qs
from etc import ads_const
from event.event import BaseEvent


'''
	URL处理，这里其实需要比较高效的匹配算法，暂时先用最土的方式
'''


class LogParser(object):
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__ads_url_handler = {
			ads_const.ADS_BASE_URL: self.__handle_click_ads_url,
			# ads_const.ADS_ACTIVATE_URL: self.__handle_activate_ads_url
		}
		self.__log_parse_chain = [
			self._parse_chain_url_filter,
			self._parse_chain_ip_filter,
			self._parse_chain_time_filter
		]

		self.__ip_pattern = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
		self.__ip_regex = re.compile(self.__ip_pattern, re.VERBOSE)

	def __handle_click_ads_url(self, url):
		url_paras = urlparse(url)
		query = url_paras.query
		if query:
			adsage_callback = query.find(ads_const.ADS_SOURCE_ADSAGE_CALLBACK)
			if adsage_callback >= 0:
				# adsage: callback=callback_url
				callback_url = [query[adsage_callback+9:]]
				other_paras = parse_qs(query[:adsage_callback], keep_blank_values=True)
				other_paras[ads_const.ADS_SOURCE_ADSAGE_CALLBACK] = callback_url
				paras = other_paras
			else:
				paras = parse_qs(url_paras.query, keep_blank_values=True)

			if paras.get(ads_const.ADS_SOURCE_ACTIVATE_TYPE, None):
				return BaseEvent(ads_const.EVENT_TYPE_ACTIVATE_EVENT, paras)
			else:
				return BaseEvent(ads_const.EVENT_TYPE_GAME_USER_EVENT, paras)

		return None

	@staticmethod
	def __handle_activate_ads_url(url):
		url_paras = urlparse(url)
		return BaseEvent(
			ads_const.EVENT_TYPE_ACTIVATE_EVENT, parse_qs(url_paras.query, keep_blank_values=True))

	def _parse_chain_ip_filter(self, line, event):
		if event is None:
			self.logger.error('there is something wrong in previous filters')
			return event
		if line:
			ip_list = self.__ip_regex.findall(line)
			if ip_list and ip_list[0]:
				event_content = event.get_content()
				if event_content:
					event_content[ads_const.ADS_SOURCE_PARA_IP] = ip_list[0]

		return event

	def _parse_chain_url_filter(self, line, event):
		try:
			for pattern, handler in self.__ads_url_handler.iteritems():
				pos = line.find(pattern)
				if pos >= 0:
					event = handler(line[pos:].split(' ')[0])
		except Exception as e:
			self.logger.error(traceback.format_exc())

		return event

	def _parse_chain_time_filter(self, line, event):
		if event is None:
			self.logger.error('there is something wrong in previous filters')
			return event
		try:
			time_str = line.split(' ')[0]
			if time_str and len(time_str) > 6:
				# 删除 +08:00
				time_str = time_str[:-6]
				if time_str:
					event_content = event.get_content()
					if event_content:
						event_content[ads_const.ADS_SOURCE_PARA_TIME] = time_str
		except Exception as e:
			self.logger.error(traceback.format_exc())

		return event

	def parse(self, line):
		event = None

		for filter_fun in self.__log_parse_chain:
			try:
				event = filter_fun(line, event)
			except:
				self.logger.error(traceback.format_exc())

		# if event is None:
			# self.logger.debug('current line is not matched {0}',format(line))

		return event
