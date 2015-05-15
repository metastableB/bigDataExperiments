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

SOURCE=$1
DESTINATION=$2
COLOMN=$3
sort -n -k$COLOMN $SOURCE >$DESTINATION