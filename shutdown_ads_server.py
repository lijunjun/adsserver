# -*- coding: utf-8 -*-

__author__ = 'return'


import os
import psutil


'''
	我把这个文件名字用shutdown_ads_server主要是给看不到代码的人一种shutdown的错觉，太坏了～
	至于真正的shutdown，console已经支持了，但是没有支持信号或者侦听端口的方式来优雅的解决
'''


def kill_server():
	# psutil version 2.1.3 has bugs on windows: https://github.com/giampaolo/psutil/issues/549
	kill_list = {'run_ads_server'}

	kill_pid_set = set()
	for p in psutil.process_iter():
		cmdline = p.cmdline()
		if p.pid != os.getpid() and len(cmdline) > 1:
			for kill_name in kill_list:
				if kill_name in cmdline[1]:
					kill_pid_set.add(p.pid)
					break

	for kill_pid in kill_pid_set:
		p = psutil.Process(kill_pid)
		if p.is_running():
			for c in p.children(recursive=True):
				print('kill process %s %s' % (c.name(), c.cmdline()))
				c.terminate()
		print('kill process %s %s' % (p.name(), p.cmdline()))
		p.terminate()


if __name__ == '__main__':
	kill_server()

