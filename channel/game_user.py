# -*- coding: utf-8 -*-

__author__ = 'return'

import traceback
import time
import logging


from task.task import BaseTask
from etc import ads_const
from database.db_proxy_factory import DBProxyFactory


'''
	用户点击广告，广告商传回来的用户信息
'''


class GameUser(BaseTask):
	def __init__(self, processor_channel):
		super(GameUser, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__processor_channel = processor_channel
		self.__stop_game_user = False
		self.__db_client = DBProxyFactory.instance().create_mongo_db_proxy()

	def get_channel(self):
		return self.__event_channel

	def __handle_source_duomeng(self, query_paras):
		mac = query_paras.get(ads_const.ADS_SOURCE_PARA_MAC, None)
		idfa = query_paras.get(ads_const.ADS_SOURCE_DUOMENG_PARA_IDFA, None)
		udid = query_paras.get(ads_const.ADS_SOURCE_DUOMENG_PARA_UDID, None)
		appId = query_paras.get(ads_const.ADS_SOURCE_COLLECTION_DUOMENG_APPID, None)
		click_ip = query_paras.get(ads_const.ADS_SOURCE_PARA_IP, None)

		if (mac is None and idfa is None) or \
				(mac and mac[0] is None and idfa and idfa[0] is None):
			self.logger.warning('mac and idfa are all none for duomeng ! ({0})'.format(query_paras))
			return

		if mac and mac[0] == ads_const.IOS_DEFAULT_MAC_ADDRESS and \
			(idfa is None or idfa[0] is None):
			self.logger.error(
				'mac address is default one ({0}) and idfa is none !'.format(ads_const.IOS_DEFAULT_MAC_ADDRESS))
			return

		self.__db_client.db_insert_doc(
			ads_const.ADS_SOURCE_COLLECTION_ADS,
			{
				ads_const.ADS_SOURCE_COLLECTION_SOURCE: ads_const.ADS_SOURCE_DUOMENG,
				ads_const.ADS_SOURCE_PARA_MAC: mac[0] if mac else None,
				ads_const.ADS_SOURCE_PARA_IDFA: idfa[0] if idfa else None,
				ads_const.ADS_SOURCE_DUOMENG_PARA_UDID: udid[0] if udid else None,
				ads_const.ADS_SOURCE_COLLECTION_IS_ACTIVATED: False,
				ads_const.ADS_SOURCE_COLLECTION_DUOMENG_APPID: appId[0] if appId else None,
				ads_const.ADS_SOURCE_COLLECTION_DUOMENG_CLICK_IP: click_ip,
				ads_const.ADS_SOURCE_COLLECTION_TIME: int(time.time())
			},
			lambda status: self.logger.info('insert duomeng: {0}'.format(status))
		)

	def __handle_source_adsage(self, query_paras):
		mac = query_paras.get(ads_const.ADS_SOURCE_PARA_MAC, None)
		idfa = query_paras.get(ads_const.ADS_SOURCE_PARA_IDFA, None)
		callback = query_paras.get(ads_const.ADS_SOURCE_ADSAGE_CALLBACK, None)

		if callback is None or \
				(mac is None and idfa is None) or \
				(callback and callback[0] is None) or \
				(mac and mac[0] is None and idfa and idfa[0] is None):

			self.logger.warning('query is wrong for adsage ! ({0})'.format(query_paras))
			return

		mac_address = mac[0] if mac else None
		if not mac_address:
			mac_address = ads_const.IOS_DEFAULT_MAC_ADDRESS

		self.__db_client.db_insert_doc(
			ads_const.ADS_SOURCE_COLLECTION_ADS,
			{
				ads_const.ADS_SOURCE_COLLECTION_SOURCE: ads_const.ADS_SOURCE_ADSAGE,
				ads_const.ADS_SOURCE_PARA_MAC: mac_address,
				ads_const.ADS_SOURCE_PARA_IDFA: idfa[0] if idfa else None,
				ads_const.ADS_SOURCE_COLLECTION_IS_ACTIVATED: False,
				ads_const.ADS_SOURCE_COLLECTION_ADSAGE_CALLBACK: callback[0] if callback else None,
				ads_const.ADS_SOURCE_COLLECTION_TIME: int(time.time())
			},
			lambda status: self.logger.info('insert adsage: {0}'.format(status))
		)

	def _run_task(self):

		while not self.__stop_game_user:
			try:
				event = self.__processor_channel.consume_one_event()
				query_paras = event.get_content()
				if query_paras:
					if query_paras.get(ads_const.ADS_SOURCE_DUOMENG_PARA_SOURCE, None):
						if query_paras[ads_const.ADS_SOURCE_DUOMENG_PARA_SOURCE][0] == \
							ads_const.ADS_SOURCE_DUOMENG:
							self.__handle_source_duomeng(query_paras)
						else:
							# 其他广告商
							pass
					elif query_paras.get(ads_const.ADS_SOURCE_ADSAGE_CALLBACK, None):
						# adsage
						self.__handle_source_adsage(query_paras)

					else:
						self.logger.warning(
							'some of parameters are not supported or missed: {0}'.format(query_paras))
				else:
					self.logger.warning(
						'current event is not well formed: {0}'.format(query_paras))

			except Exception as e:
				self.logger.error(traceback.format_exc())

	def stop(self):
		self.logger.info('stop game user task')
		self.__stop_game_user = True
