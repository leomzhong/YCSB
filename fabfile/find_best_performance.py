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
import multiprocessing
from threading import Thread
import signal
import os
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

DEBUG = False


def signal_handler(signal, frame):
    print 'Stopping benchmark ...... \n'
    #sys.exit(0)
    run("ps -ef | grep 'ycsb'|awk '{print $2}'|xargs kill -9 > /dev/null 2>&1" )
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
        global thread_ct 
	if additional_arguments:
		command += " " + additional_arguments	
	if multiclient:
		command += " -p " + "clientsCoordination=true"
		command += " -p " + "zkAddress=" + zkAddress
		command += " -p " + "zkRoot=" + zkRoot
		command += " -p " + "groupSize=" + str(groupSize)
	#debug("command is:" + command)
        print "[REPORT]: now running YCSB with "+str(thread_ct)+" threads\n"
	return command

def get_org_YCSB_arg(arguments=""):
    global ycsb_arguments
    ycsb_arguments = ""
    if "threadcount=" in arguments:
        splits = arguments.split("threadcount=")
        for s in splits:
            s = s.strip()
            s = s.rstrip("-p") 
            tmp = s.split(" ")
            if tmp != []:
                fl = tmp[0].strip()
                if(fl.isdigit()):
                    continue
                else:
                    ycsb_arguments += (s+" ")

def test(arguments=""):
	global ycsb_arguments
        global thread_ct
	config = loadConfigFile("fab_config.cfg")
	execute(deploy)
        thread_ct = 1
        drop_threshould = 1
        if "operationcount" in arguments:
            splits = arguments.split("operationcount")
            opcts = splits[1].split(" ")[0]
            opcts = opcts.replace(" ", "")
            opcts = opcts.replace("=", "")
            opcts = opcts.strip()
            drop_threshould = int(opcts)/100
        if drop_threshould == 0:
            drop_threshould = 1
        throughputs = []
        threadcts = []
        cpu_num = multiprocessing.cpu_count()
        step = cpu_num
        final_res = []
        in_testing_stage = True
        go_next_stage = False
        upper_bound = 0
        f = open("overall_throughput", "w")

        while True:
            get_org_YCSB_arg(arguments)
            ycsb_arguments += " -p "+ "threadcount="+str(thread_ct) 
            avg_throughput = 0
            for i in range(1,2):
	        execute(runTest)
	        execute(getLog)
	        overall_throughput = mergeLog(f)
                avg_throughput += overall_throughput
            #avg_throughput = avg_throughput / 3.0
            final_res.append({thread_ct:avg_throughput})
            print "[REPORT]: average throughput "+str(avg_throughput)+ "\n"
            throughputs.append(avg_throughput)
            threadcts.append(thread_ct)
            if len(throughputs) > 2:
                if avg_throughput >= max(throughputs):
                    if in_testing_stage:
                        thread_ct += step
                    else:
                        thread_ct = (upper_bound - thread_ct)/2 + thread_ct
                        upper_bound = thread_ct
                        if thread_ct in threadcts:
                            break
                else:
                    if in_testing_stage:
                        if max(throughputs) - avg_throughput > drop_threshould:
                            in_testing_stage = False
                            step = 200*cpu_num
                            upper_bound = step
                            go_next_stage = True
                        else:
                            thread_ct += step
                            continue
                    old_thread_ct = threadcts[throughputs.index(max(throughputs))]
                    if go_next_stage:
                        thread_ct = thread_ct + step
                        go_next_stage = False
                        continue
                    thread_ct = (thread_ct + old_thread_ct)/2
                    if thread_ct <= old_thread_ct or thread_ct in threadcts:
                        break
            else:
                thread_ct += step
        final_res.sort()
        for item in final_res:
            f.write(str(item.keys()[0]) + "\t" + str(item.values()[0]) + "\n")
        f.close()
        print "[CONLUSION] max throughput this database can reach: "+str(max(throughputs))+" thread count per machine: "+ str(threadcts[throughputs.index(max(throughputs))])+ " num of test machine: "+ str(len(env.hosts)) 
#
# This function is only used when you need to rebuild the project
#
@runs_once
def build():
	local('mvn clean package -DskipTests')

#
# Copy all necessary files to all testing machines
#
@parallel
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
#       run("((" + runCommand + " > test.output) 3>&1 1>&2 2>&3 > test.err)") 
        run(runCommand + " 1> test.output 2> test.err")

@parallel
def getLog():
	global root
	counter = 0
	get(root + "/test.output")

def mergeLog(f):
	overall_throughput = 0.0
	record = {}
	for hostname in env.hosts:
		f = open(hostname + "/test.output", "r")	
		overall_throughput += parseResultFile(hostname, f, record)	
	print "\n*********overal_throught is:" + str(overall_throughput)
        #f.write(str(overall_throughput)+"\n")
	dumpFinalLog(overall_throughput, record)
        return overall_throughput
    

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

def monitor():
    print "  ------------    MONITOR ---------------   \n"

			
if __name__ == "__main__":
	ycsb_arguments = " ".join(sys.argv[1:])
	#execute(test, ycsb_arguments)
        signal.signal(signal.SIGINT, signal_handler)
        t_thread = Thread(target=execute, args=(test, ycsb_arguments,))
        t_thread.start()
        m_thread = Thread(target=monitor)
        m_thread.start()
        t_thread.join()
        m_thread.join()
