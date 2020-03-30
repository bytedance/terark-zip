#!/bin/bash
set -ex

BASE_DIR=`pwd`
if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

# Build terark-core libraries
cd ../ && ./build.sh

# Build test cases under gtests
rm -rf $BASE_DIR/build && mkdir -p $BASE_DIR/build
cd $BASE_DIR/build && cmake ../ && make -j $cpuNum