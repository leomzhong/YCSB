Prerequisite:
In order to run multi-client YCSB, you need to have:
1. ZooKeeper running on a server
2. Several testing nodes that you want to use. You should be able to ssh to these machines using the same password.
3. In order to use this script, you have to have python fabric package installed. This is only needed on one machine where you want to run this script.


Getting started with this auto-deploy script for running YCSB on multiple servers:

1. Get the latest version for YCSB, or compile the dev version of YCSB
2. Put the fabfile.py and fab_config.cfg file into the same folder as the ycsb distribution tar file
3. Change the fab_config file according to your testing environment
4. Run "python fabfile.py [your normal YCSB commands]"
Example: python fabfile.py ycsb-0.1.4/bin/ycsb run redis -P ycsb-0.1.4/workloads/workloada -p redis.host=localhost -p redis.port=6379

The default result file is "test.output" 
