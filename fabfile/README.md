Getting started with this auto-deploy script for running YCSB on multiple servers:

1. Make sure you have fab package installed on your machine.(Will later remove this)
2. Get the latest version for YCSB, or compile the dev version of YCSB
3. Put the fabfile.py and fab_config.cfg file into the same folder as the ycsb distribution tar file
4. Change the fab_config file according to your testing environment
5. Run "python fabfile.py [your normal YCSB commands]"

The default result file is "test.output" 
