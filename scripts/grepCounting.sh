#!/bin/bash

# Don K Dennis (metastableB)
# donkdennis [at] gmail.com
# 26 May, 2015

# This script is for printing the required grep outputs on all nodes level wise
# For adaptation please make sure to change source path and number of inputs pass as required

numberOfPass=9
level=1
numberOfNodes=81306
total=1 # counting the source node

while [ $numberOfPass -gt 0 ] ; do
	count=$(grep -e "GRAY" ./f1_r1_${level}/part* | grep -e "GRAY" -c )
	echo "Level $level  : $count"
	let	level=level+1
	let numberOfPass=numberOfPass-1
	let total=total+${count}
done
difference=$((numberOfNodes-total))
echo "total of nodes on each level = " $total
echo "difference with the total no of nodes = " $difference
echo "Total Black Nodes at the end = " $(grep -P 'BLACK' ./f1_r1_$((level-1))/part* | grep -P 'BLACK' -c)
echo "Total WHITE nodes at the end = " $(grep -P 'WHITE' ./f1_r1_$((level-1))/part* | grep -P 'WHITE' -c)
