#!/bin/bash
export HADOOP_CLASSPATH=$(pwd)/
hadoop MaxTemperature  $(pwd)/input/ $(pwd)/output/