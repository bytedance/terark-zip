#!/bin/bash

set -e # exit on error

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

make test -o -j$cpuNum
