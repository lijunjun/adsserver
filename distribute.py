# -*- coding: utf-8 -*-

__author__ = 'return'


import fnmatch
import compileall
import os
import re
import zipfile


'''
	编译py，然后打包
'''


def compile_python(ads_dir):
	print '[ads_server] compile py to pyc in dir: {0}'.format(ads_dir)
	compileall.compile_dir(ads_dir, maxlevels=50, force=True)


def package_dir(ads_dir, package_name):
	print '[ads_server] package pyc from {0} to {1}'.format(ads_dir, package_name)

	includes = []
	excludes = ['*.py', '*.log', '*.zip', '*.tar']
	includes = r'|'.join([fnmatch.translate(x) for x in includes])
	excludes = r'|'.join([fnmatch.translate(x) for x in excludes])

	package = zipfile.ZipFile(package_name, 'w', zipfile.ZIP_DEFLATED)

	for dir_path, dir_names, file_names in os.walk(ads_dir):

		dir_names[:] = [d for d in dir_names if not d.startswith('.')]

		for filename in file_names:
			if not re.match(excludes, filename):
				package.write(os.path.join(dir_path, filename))

	package.close()


if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(
		description='build and package ads server',
		formatter_class=argparse.RawTextHelpFormatter)
	parser.add_argument('-ads_dir', type=str, help='the ads server source root (. dir)', default='.')
	parser.add_argument('-pack_name', type=str, help='package name (ads_server_dist.zip)', default='ads_server_dist.zip')

	args = parser.parse_args()
	ads_dir = os.path.realpath(args.ads_dir)
	pack_name = args.pack_name

	# 编译
	compile_python(ads_dir)

	# 打包
	package_dir(ads_dir, pack_name)
