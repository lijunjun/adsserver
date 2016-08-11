# -*- coding: utf-8 -*-

__author__ = 'return'


import logging
import traceback
import time

from common import encrypt
from network import http_client
from etc import ads_const
from task.task import BaseTask
from database.db_proxy_factory import DBProxyFactory


'''
	用户首次激活任务
'''


class ActivateGame(BaseTask):
	def __init__(self, processor_channel):
		super(ActivateGame, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.__processor_channel = processor_channel
		self.__stop_activate_game = False
		self.__max_try_times = 10
		self.__db_client = DBProxyFactory.instance().create_mongo_db_proxy()

	def __send_info_to_ads(self, request):
		try:

			for i in xrange(self.__max_try_times):
				# self.logger.debug('request url: {0}'.format(request))

				res = http_client.http_get(request)
				if res and res.status_code == 200:
					self.logger.info('activated url: {0}, return: {1}.'.format(request, res.content))
					res.close()
					return
				elif res:
					res.close()
					self.logger.warning('http result: {0}, {1}'.format(res.status_code, res.content))
		except Exception as e:
			self.logger.error(e)
			return

	@staticmethod
	def get_domob_sign(appid, udid, ma, ifa, oid, key):
		import hashlib
		return hashlib.md5('%s,%s,%s,%s,%s,%s' % (appid, udid, ma, ifa, oid, key)).hexdigest()

	def __search_result(self, status, doc):
		if status and doc:
			source = doc.get(ads_const.ADS_SOURCE_COLLECTION_SOURCE, None)
			if source == ads_const.ADS_SOURCE_DUOMENG:

				# duomeng
				mac = doc.get(ads_const.ADS_SOURCE_PARA_MAC, '')
				idfa = doc.get(ads_const.ADS_SOURCE_PARA_IDFA, '')
				appid = doc.get(ads_const.ADS_SOURCE_COLLECTION_DUOMENG_APPID, '')
				activate_time = doc.get(ads_const.ADS_SOURCE_COLLECTION_ACTIVATED_TIME, '')
				click_time = doc.get(ads_const.ADS_SOURCE_COLLECTION_TIME, '')
				activate_ip = doc.get(ads_const.ADS_SOURCE_COLLECTION_DUOMENG_ACTIVATE_IP, '')
				app_version = doc.get(ads_const.ADS_SOURCE_COLLECTION_DUOMENG_APP_VERSION, '')

				sign_key = doc.get(
					ads_const.ADS_SOURCE_COLLECTION_DUOMENG_SIGN_KEY, ads_const.ADS_DOUMENG_SIGN_KEY)

				if app_version == '':
					self.logger.error('app version should not be none for duomeng')
					return

				if activate_ip == '':
					self.logger.error('activate ip should not be none for duomeng')
					return

				# duomeng 4.6.5
				# http://e.domob.cn/track/ow/api/postback?
				# appId=531266294&
				# udid=7C:AB:A3:D6:E7:81&
				# ifa=511F7987-6E2F-423A-BFED-E4C52CB5A6DC&
				# acttime=1391502359&
				# returnFormat=1&
				# sign=87c3574ff875eaf36353fc48638bb580&
				# ip= 115.183.152.45&
				# appVersion=2.0.1&
				# userid=4124bc0a9335c27f086f24ba207a4912&
				# clktime=1391501359&
				# clkip=119.255.14.220

				duomeng_activate_uri = \
					'http://e.domob.cn/track/ow/api/postback?appId={0}&' \
					'udid={1}&' \
					'ifa={2}&' \
					'acttime={3}&' \
					'sign={4}&' \
					'ip={5}&' \
					'appVersion={6}&' \
					'clktime={7}'.format(
						appid or '',
						mac or '',
						idfa or '',
						activate_time or '',
						self.get_domob_sign(appid, mac, '', idfa, '', sign_key),
						activate_ip or '',
						app_version or '',
						click_time or ''
					)

				self.__send_info_to_ads(duomeng_activate_uri)

			elif source == ads_const.ADS_SOURCE_ADSAGE:

				# adsage
				callback = doc.get(ads_const.ADS_SOURCE_COLLECTION_ADSAGE_CALLBACK, None)
				if callback:
					self.__send_info_to_ads(callback)

		else:
			self.logger.info('activate user item is not found or activated !')
		pass

	def __check_activate_request(self, query_paras):
		if query_paras:
			active_part_string = query_paras.get(ads_const.ADS_SOURCE_ACTIVATE_TYPE, None)
			active_sign = query_paras.get(ads_const.ADS_SOURCE_ACTIVATE_SIGN_TYPE, None)
			active_sign_key = ads_const.ADS_SOURCE_ACTIVATE_SIGN_KEY

			if active_part_string and active_sign:
				active_string = active_part_string[0] + active_sign_key
				if active_string and active_sign[0] == encrypt.md5_encode(active_string).upper():
					return True

		return False

	def _run_task(self):

		while not self.__stop_activate_game:
			try:
				event = self.__processor_channel.consume_one_event()
				query_paras = event.get_content()

				if not self.__check_activate_request(query_paras):
					self.logger.error('failed to check sign {0}'.format(query_paras))
					continue

				if query_paras:
					mac = query_paras.get(ads_const.ADS_SOURCE_PARA_MAC, None)
					idfa = query_paras.get(ads_const.ADS_SOURCE_PARA_IDFA, None)
					udid = query_paras.get(ads_const.ADS_SOURCE_DUOMENG_PARA_UDID, None)
					activate_ip = query_paras.get(
						ads_const.ADS_SOURCE_PARA_IP, None)
					app_version = query_paras.get(
						ads_const.ADS_SOURCE_PARA_APP_VERSION, [ads_const.ADS_APP_VRESION])

					if mac is None and idfa is None and udid is None:
						return

					record_limit_time = int(time.time()) - ads_const.ADS_CLEANUP_TIME_LIMIT
					if mac == ads_const.IOS_DEFAULT_MAC_ADDRESS or \
								mac == ads_const.IOS_DEFAULT_MAC_ADDRESS_MD5:
						query = {
							ads_const.ADS_SOURCE_PARA_IDFA: idfa[0] if idfa else None,
							ads_const.ADS_SOURCE_COLLECTION_IS_ACTIVATED: False,
							ads_const.ADS_SOURCE_COLLECTION_TIME:
								{
									"$gt": record_limit_time
								}
						}
					else:
						query = {
							ads_const.ADS_SOURCE_PARA_MAC: mac[0] if mac else None,
							ads_const.ADS_SOURCE_PARA_IDFA: idfa[0] if idfa else None,
							ads_const.ADS_SOURCE_COLLECTION_IS_ACTIVATED: False,
							ads_const.ADS_SOURCE_COLLECTION_TIME:
								{
									"$gt": record_limit_time
								}
						}

					self.__db_client.db_find_and_modify_doc(
						ads_const.ADS_SOURCE_COLLECTION_ADS,
						query,
						{
							"$set":
							{
								ads_const.ADS_SOURCE_COLLECTION_IS_ACTIVATED: True,
								ads_const.ADS_SOURCE_COLLECTION_ACTIVATED_TIME: int(time.time()),
								ads_const.ADS_SOURCE_COLLECTION_DUOMENG_ACTIVATE_IP: activate_ip,
								ads_const.ADS_SOURCE_COLLECTION_DUOMENG_SIGN_KEY:
									ads_const.ADS_DOUMENG_SIGN_KEY,
								ads_const.ADS_SOURCE_COLLECTION_DUOMENG_APP_VERSION:
									app_version[0] if app_version else None
							}
						},
						upsert=False,
						new=True,
						callback=self.__search_result
					)

			except Exception as e:
				self.logger.error(traceback.format_exc())

	def stop(self):
		self.logger.info('stop activating game task')
		self.__stop_activate_game = True
