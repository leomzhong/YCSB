from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.contrib.files import exists
from ConfigParser import ConfigParser
from pprint import pprint
import json
import logging
import re
import sys

env.hosts = []
env.password = ""

# Root directory on all hosts
root = ""

# Files needed
neededFiles = ["ycsb-0.1.4.tar.gz"]

# YCSB argument
ycsb_arguments = ""

# additional YCSB arguments in the config file
additional_arguments = ""

# Whether to force update the ycsb code on all hosts
forceupdate = False

# Whether multi-client testing is enabled
multiclient = False

# zookeeper related
zkAddress = ""
zkRoot = ""
groupSize = -1

# Final output file name
finalLog = "test.output"

DEBUG = True

def debug(output):
	global DEBUG
	if DEBUG:
		print("*************DEBUG*****************" + output)

def loadConfigFile(filename):
	config = ConfigParser()
	config.read(filename)
	loadHostList(config)
	loadNeededFiles(config)
	loadYCSBArguments(config)
	return config

def loadHostList(config):
	global root
	hostlist = config.items("Host")
	for key, content in hostlist:
		if key.startswith("host"):
			env.hosts.append(content)
		elif key == "password":
			env.password = content
		elif key == "root":
			root = content
	debug("hosts:" + '\t'.join(env.hosts))
	debug("password:" + env.password)
	debug("root:" + root)

def loadNeededFiles(config):
	global neededFiles
	global forceupdate
	filelist = config.items("NeededFiles")
	for key, content in filelist:
		if key.startswith("file"):
			neededFiles.append(content)
		if key == "forceupdate" and content == "yes":
			forceupdate = True
	debug("neededFiles:" + '\t'.join(neededFiles))
	debug("forceupdate:" + str(forceupdate))
	
def loadYCSBArguments(config):
	global additional_arguments
	global multiclient
	global zkAddress
	global zkRoot
	global groupSize
	global finalLog
	arguments = config.items("YCSB")
	for key, content in arguments:
		if key == "arguments":
			additional_arguments = content
		elif key == "multi-clients" and content == "yes":
			multiclient = True
			zkAddress = config.get("YCSB", "zkAddress")
			zkRoot = config.get("YCSB", "zkRoot")
			groupSize = len(env.hosts)
		elif key == "logname" and content:
			finalLog = content
	debug("YCSB argument" + additional_arguments)
	debug("multiclient:" + str(multiclient))
	debug("zkAddress:" + zkAddress)
	debug("zkRoot:" + zkRoot)
	debug("groupSize:" + str(groupSize))
	debug("logname:" + finalLog)

def getRunCommand(command=""):
	global additional_arguments
	global multiclient
	global zkAddress
	global zkRoot
	global groupSize
	if additional_arguments:
		command += " " + additional_arguments	
	if multiclient:
		command += " -p " + "clientsCoordination=true"
		command += " -p " + "zkAddress=" + zkAddress
		command += " -p " + "zkRoot=" + zkRoot
		command += " -p " + "groupSize=" + str(groupSize)
	debug("command is:" + command)
	return command

def test(arguments=""):
	global ycsb_arguments
	ycsb_arguments = arguments
	config = loadConfigFile("fab_config.cfg")
	execute(deploy)
	execute(runTest)
	execute(getLog)
	mergeLog()

#
# This function is only used when you need to rebuild the project
#
@runs_once
def build():
	local('mvn clean package -DskipTests')

#
# Copy all necessary files to all testing machines
#
def deploy():
	global root
	global neededFiles
	global forceupdate

	debug("deploy: forceupdate is:" + str(forceupdate))

	# Create the root directory if not exist
    	with cd("~/"):
		if not exists(root):	
			run('mkdir ' + root)
	# copy all the files
	for pathname in neededFiles:
		filenames = pathname.split('/')
		filename = filenames[-1]
		target_filename = root + "/" + filename
		debug("target_file is:" + target_filename)
		if forceupdate or (not forceupdate and not exists(target_filename)):
			put(pathname, target_filename) 
			if filename == "ycsb-0.1.4.tar.gz":
				with cd(root):
					run('tar xzvf ycsb-0.1.4.tar.gz')
#		if forceupdate and not exists(target_filename):
#			put(pathname, target_filename) 
#			if filename == "ycsb-0.1.4.tar.gz":
#				with cd(root):
#					run('tar xzvf ycsb-0.1.4.tar.gz') 
		else:
			print(filename + " already exists, skip updating")
@parallel
def runTest():
	global root
	global ycsb_arguments
	runCommand = getRunCommand(ycsb_arguments)
	with cd(root):
#		run("((" + runCommand + " > test.output) 3>&1 1>&2 2>&3 > test.err)") 
		run(runCommand + " 1> test.output 2> test.err")

@parallel
def getLog():
	global root
	counter = 0
	get(root + "/test.output")

def mergeLog():
	overall_throughput = 0.0
	record = {}
	for hostname in env.hosts:
		f = open(hostname + "/test.output", "r")	
		overall_throughput += parseResultFile(hostname, f, record)	
	print "overal_throught is:" + str(overall_throughput)
	dumpFinalLog(overall_throughput, record)

def parseResultFile(hostname, f, record):
	header_list = ["[UPDATE]", "[READ]", "[WRITE]", "[CLEANUP]"]
	throughtput = 0.0
	if not record.has_key(hostname):
		record[hostname] = {}
	myrecord = record[hostname]
	for line in f:
		words = re.sub(r'\s', '', line).split(',')
		if line.startswith("[OVERALL]"):
			if line.find("RunTime") != -1:
				myrecord[words[1]] = float(words[2])		
			elif line.find("Throughput") != -1:
				throughtput = float(words[2])
		elif any(line.startswith(header) for header in header_list):
			update_record(myrecord, words)	
	return throughtput

def update_record(myrecord, words):
	if not myrecord.has_key(words[0]):
		myrecord[words[0]] = {}
	subrecord = myrecord[words[0]]
	subrecord[words[1]] = words[2]

def dumpFinalLog(overall_throughput, record):
	global finalLog
	global ycsb_arguments
	header_list = ["[UPDATE]", "[READ]", "[WRITE]", "[CLEANUP]"]
	f = open(finalLog, "w+")
	f.write("YCSB Testing result with run command:\n")
	f.write(getRunCommand(ycsb_arguments) + "\n")
	f.write("[OVERALL], Throughput(ops/sec), " + str(overall_throughput) + "\n")
	for header in header_list:
		dumpLogHelper(f, record, header)

def dumpLogHelper(f, record, target):
	targetmap = {}
	total_operations = 0
	total_latency = 0.0
	max_latency = 0
	min_latency = sys.maxint
	for hostname in env.hosts:
		tmp = record[hostname]
		if tmp.has_key(target):
			hostmap = tmp[target]
			operation_counter = 0
			average_latency = 0.0
			for key in hostmap:
				if key == "Operations":
					operation_counter = int(hostmap[key])
				elif key == "AverageLatency(us)":
					average_latency = float(hostmap[key])
				elif key == "MinLatency(us)":
					min_latency = min(min_latency, int(hostmap[key]))
				elif key == "MaxLatency(us)":
					max_latency = max(max_latency, int(hostmap[key]))
				elif not targetmap.has_key(key):
					targetmap[key] = int(hostmap[key])	
				else:
					targetmap[key] += int(hostmap[key])
			total_operations += operation_counter
			total_latency += operation_counter * average_latency

	# Only output the result if target operation exists
	if total_operations:
		f.write(", ".join([target, "Operations", str(total_operations)]) + "\n")
		f.write(", ".join([target, "AverageLatency(us)", str(total_latency / total_operations)]) + "\n")
		f.write(", ".join([target, "MinLatency(us)", str(min_latency)]) + "\n")
		f.write(", ".join([target, "MaxLatency(us)", str(max_latency)]) + "\n")
		for key in range(0, 1000):
			result = targetmap[str(key)]	
			f.write(", ".join([target, str(key), str(result)]) + "\n")
			
if __name__ == "__main__":
	ycsb_arguments = " ".join(sys.argv[1:])
	execute(test, ycsb_arguments)
