__author__ = 'return'

import cmd

from etc import ads_const
from resource_mgr import ResourceMgr

class AdsConsole(cmd.Cmd):

	def emptyline(self):
		pass

	def do_stop(self, para):
		"""
			stop ads server
		"""
		task_center = ResourceMgr.instance().get_resource(ads_const.RESOURCE_TASK_CENTER)
		if task_center:
			task_center.stop_all_ads_task()

