__author__ = 'return'

import os
import sys
import ads_const
import logging
from logging import config

ADS_ENV_FILE_PATH = os.path.dirname(os.path.abspath(__file__))
ADS_ROOT_PATH = os.path.dirname(ADS_ENV_FILE_PATH)

ADS_ETC_PATH = os.path.join(ADS_ROOT_PATH, 'etc')
ADS_LIBS_PATH = os.path.join(ADS_ROOT_PATH, 'libs')


def init_env():
	search_path_list = [ADS_ROOT_PATH, ADS_LIBS_PATH]

	for search_path in search_path_list:
		if search_path not in sys.path:
			sys.path.insert(0, search_path)


def init_log():

	log_configfile = None
	try:
		cur_pid = str(os.getpid())
		config_filename = ADS_ETC_PATH + '/log-' + cur_pid + '.config'
		log_configfile = open(config_filename, 'w+')

		template_config = open(ads_const.LOG_CONFIG_FILE, 'r')
		template = template_config.read()

		new_log_config = template.replace(ads_const.LOG_CONFIG_FILE_NAME_PATTERN, cur_pid)
		log_configfile.write(new_log_config)
		log_configfile.flush()
		log_configfile.close()

		print 'log configure file name:' + config_filename
		config.fileConfig(config_filename)

	except Exception as e:
		print e
	finally:
		if log_configfile is not None:
			os.remove(config_filename)
