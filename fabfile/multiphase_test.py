from ConfigParser import ConfigParser
import os

def hasPhase(config, counter):
	try:
		phase = config.items('PHASE' + str(counter))	
	except:
		return False
	return True 

def loadArgumentForPhase(config, counter):
	section_name = 'PHASE' + str(counter)
	if not config.has_option(section_name, 'argument'):
		print "Warning: there should be argument under " + section_name
		return None
	else:
		argument = config.get(section_name, 'argument')
		if argument.startswith('\"'):
			return argument[1: -1]
		else:
			return argument

def runPhase(config, counter):
	print "String Phase " + str(counter)
	argument_phase = loadArgumentForPhase(config, counter)
	if not argument_phase:
		return
	#print "DEBUGGING:******* Argument for Phase " + str(counter) + " is:" + argument_phase
	os.system('python multiclient_test.py ' + argument_phase)
	os.system('mv test.output multiphase_output/phase' + str(counter) + '.output')

def test():
	config = ConfigParser()
	config.read('multiphase_config.cfg')
	counter = 1
	if not os.path.exists('./multiphase_output'):
		os.system('mkdir multiphase_output')
	if os.path.exist('./throughput.output'):
		os.system('rm throughput.output')
	while True:
		if hasPhase(config, counter):
			runPhase(config, counter)
		else:
			break
		counter += 1

if __name__ == "__main__":
	test()
