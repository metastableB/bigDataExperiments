#!/bin/bash

# Don K Dennis (metastableB)
# donkdennis [at] gmail.com
# 26 May, 2015

# This script is for printing the required grep outputs on all nodes level wise
# For adaptation please make sure to change source path and number of inputs pass as required

numberOfPass=15
level=1

while [ $numberOfPass -gt 0 ] ; do
	count=$(grep -e "GRAY" ./f1_r1_${level}/part* | grep -e "GRAY" -c )
	echo "Level $level  : $count"
	let	level=level+1
	let numberOfPass=numberOfPass-1
done