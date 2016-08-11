__author__ = 'return'

import json


def parse( filename ):
	configfile = open(filename)
	jsonconfig = json.load(configfile)
	configfile.close()
	return jsonconfig


def save( filename, jsonconfig):
	configFile = open(filename, "w")
	json.dump(jsonconfig, configFile)
	configFile.close()


def save2(filename, jsonconfig):
	configFile = open(filename, "w+")
	json.dump(jsonconfig, configFile)
	configFile.close()

