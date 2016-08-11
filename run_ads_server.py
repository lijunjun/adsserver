# -*- coding: utf-8 -*-

__author__ = 'return'


import signal
import logging
import os

from etc import ads_env
ads_env.init_env()
ads_env.init_log()
logger = logging.getLogger('ads_server')


from etc import ads_const
from task.task_center import TaskCenter
from database.mongo_mgr import MongoDBServiceManager
from common.resource_mgr import ResourceMgr
from common import timer
from common import jsonconfig
from common.ads_console import AdsConsole


def init_basic_signal():

	def signal_handler(signal_v, frame):
		try:
			logger.info('ads server will be exited in signal {0} !'.format(signal_v))
			task_center = ResourceMgr.instance().get_resource(ads_const.RESOURCE_TASK_CENTER)
			if task_center:
				task_center.stop_all_ads_task()
		finally:
			os._exit(1)

	signal.signal(signal.SIGINT, signal_handler)


def init_mongo_db(db_config):
	if db_config is None:
		raise BaseException('db config should not be none')

	mongo_config = db_config.get(ads_const.ADS_CONFIG_DATABASE_MONGO, None)
	if mongo_config is None:
		raise BaseException('mongo config should not be none')

	logger.info('init mongodb service')
	mongodb_service_mgr = MongoDBServiceManager(mongo_config, db_config)
	ResourceMgr.instance().add_resource(ads_const.RESOURCE_MONGO_DB_MANAGER, mongodb_service_mgr)


def init_ads_env(config):
	ads_config = jsonconfig.parse(config)

	ads_base_url = ads_config.get(ads_const.ADS_CONFIG_BASE_URL, None)
	# ads_activate_url = ads_config.get(ads_const.ADS_CONFIG_ACTIVATE_URL, None)
	ads_syslog_path = ads_config.get(ads_const.ADS_CONFIG_SYSLOG_PATH, None)

	ads_activate_processor_count = \
		ads_config.get(ads_const.ADS_CONFIG_ACTIVATE_PROCESSOR_COUNT, 1)
	ads_game_user_processor_count = \
		ads_config.get(ads_const.ADS_CONFIG_GAME_USER_PROCESSOR_COUNT, 1)

	if ads_base_url is None or ads_syslog_path is None:
		logger.error('config should not be null')
		raise BaseException('config should not be null')

	ads_global_monitor_interval = \
		ads_config.get(ads_const.ADS_CONFIG_DATABASE_GLOBAL_MONITOR_INTERVAL, 15)
	ads_const.ADS_GLOBAL_MONITOR_INTERVAL = ads_global_monitor_interval

	ads_sign_pwd = ads_config.get(ads_const.ADS_CONFIG_SIGN_PWD, None)
	if ads_sign_pwd:
		ads_const.ADS_SOURCE_ACTIVATE_SIGN_KEY = ads_sign_pwd

	ads_duomeng_sign_key = \
		ads_config.get(ads_const.ADS_CONFIG_ADS_DUOMENG_SIGN_KEY, None)
	if ads_duomeng_sign_key:
		ads_const.ADS_DOUMENG_SIGN_KEY = ads_duomeng_sign_key

	ads_collect_user_idfa = ads_config.get(ads_const.ADS_CONFIG_COLLECT_USER_IDFA, 0)
	ads_const.ADS_COLLECT_USER_IDFA = ads_collect_user_idfa

	ads_const.ADS_BASE_URL = ads_base_url
	# ads_const.ADS_ACTIVATE_URL = ads_activate_url
	ads_const.ADS_SYSLOG_PATH = ads_syslog_path
	ads_const.ADS_ACTIVATE_PROCESSOR_TASK_COUNT = ads_activate_processor_count
	ads_const.ADS_GAME_USER_PROCESSOR_TASK_COUNT = ads_game_user_processor_count

	ads_clean_interval = ads_config.get(ads_const.ADS_CONFIG_CLEAN_IN_HOUR, None)
	if ads_clean_interval:
		ads_const.ADS_CLEANUP_TIME_INTERVAL = ads_clean_interval

	ads_clean_time_limit = ads_config.get(ads_const.ADS_CONFIG_CLEAN_LIMIT_IN_SECOND, None)
	if ads_clean_time_limit:
		ads_const.ADS_CLEANUP_TIME_LIMIT = ads_clean_time_limit

	init_mongo_db(ads_config.get(ads_const.ADS_CONFIG_DATABASE, None))


if __name__ == '__main__':
	logger.info('run ads server')
	init_ads_env('etc/ads.config')

	task_center = TaskCenter()
	task_center.init_ads_tasks()
	ResourceMgr.instance().add_resource(ads_const.RESOURCE_TASK_CENTER, task_center)

	# console = AdsConsole()
	# console.cmdloop()

	init_basic_signal()

	logger.info('go into loops')
	timer.loops()
