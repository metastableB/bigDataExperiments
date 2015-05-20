#!/bin/bash

# Don K Dennis (metastableB)
# donkdennis [at] gmail.com
# 15 May, 2015

# This script attempts to login the IPs specified under HOST with USER one by one and
# runs the jps program. This is helpful to see if all the hadoop daemons have started 
# correctly on all the nodes

# For adaptation please make sure to change USERNAME , HOSTS , SCRIPT according to your usecase 

USERNAME=iiitd
HOSTS="192.168.1.241 192.168.1.242 192.168.1.243 192.168.1.244 192.168.1.245 192.168.1.246 192.168.1.247 192.168.1.248 192.168.1.249 192.168.1.250"
SCRIPT="/usr/local/jdk1.8.0_45/bin/jps;"
for HOSTNAME in ${HOSTS} ; do
	echo "In " $HOSTNAME
	ssh -l ${USERNAME} ${HOSTNAME} "${SCRIPT}"
	echo "--------------------------"
done