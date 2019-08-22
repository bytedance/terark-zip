#!/usr/bin/env bash

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

# make the project
git submodule update --init

sudo apt-get update
sudo apg-get install libaio-dev

rm -rf pkg

make pkg -j $cpuNum PKG_WITH_STATIC=1 PKG_WITH_DBG=1

# move all binaries to output/ dir for next CICD steps
WITH_BMI2=`./cpu_has_bmi2.sh`
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc tools/configure/compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

echo $PLATFORM_DIR

rm -rf output

mkdir output

if [ `uname` == Darwin ]; then
	cp -r pkg/terark-fsa_all-$PLATFORM_DIR/* output
else
	cp -lrP pkg/terark-fsa_all-$PLATFORM_DIR/* output
fi

#mv 'pkg/terark-fsa_all-'$PLATFORM_DIR output
