#!/bin/bash

# Don K Dennis (metastableB)
# donkdennis [at] gmail.com
# 16 May, 2015

# This is how you sort files in unix, this script is not intended
# to run as such but as a reminer for me to remember how to sort
# incase I need to sort.
# command line arguements <source_file> <outputFile> <colomn to sort indexed from 1>
# -n : sort numerically
# -k : sort by this colon
# -k2,3 : sort by colon two three
# -k2,2 -k3,3 : sort by colon 2 till 2, then by 3 till 3

SOURCE=$1
DESTINATION=$2
COLOMN=$3
sort -n -k$COLOMN,$COLOMN $SOURCE >$DESTINATION