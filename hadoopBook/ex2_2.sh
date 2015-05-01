#!/bin/bash

#extract the file names to year variable
for year in /home/don/Work/Internship2015/hadoop-book/input/ncdc/all/* 
do 	# we dont need a semicolon on the previous line since do is not on it
	echo -ne `basename $year .gz`"\t"
	# -ne : -n donot output the trailing newline
	#		-e enable backslash interpretations as escape for \t
	# basename is a standard UNIX computer program. When basename is given a pathname, 
	# it will delete any prefix up to the last slash ('/') character and return the result
	# We are extracting thr year from the file name
	gunzip -c $year | \
	awk '{ temp = substr($0, 88, 5) + 0; # addition turns this into an intiger
	q = substr($0, 93, 1);
	if (temp !=9999 && q ~ /[01459]/ && temp > max) max = temp } # 9999 implies a missing entry in the NCDC database
	END { print max }'
done
# q : quality code
# awk resource here : http://www.grymoire.com/Unix/Awk.html