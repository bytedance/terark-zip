#!/usr/bin/env bash
#
# Usage:
#   GTEST_INC=... \     # googletest include dir
#   GTEST_LIB_DIR=... \     # googletest static library dir (if not set, will use submodule denpendency to complie on the fly)
#   BOOST_INC=... \
#   BOOST_LIB_DIR=... \
#   ./test.sh  

set -e

BASE_DIR=$PWD
GTEST_INC=
GTEST_LIB_DIR=
BOOST_INC=
BOOST_LIB_DIR=

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

# build boost
if [ -z "$BOOST_LIB_DIR" ];then
  echo "build from submodule if BOOST_LIB_DIR is not set"
  # TODO
  BOOST_INC=$BASE_DIR/boost-include
  BOOST_LIB_DIR=$BOOST_INC/stage/lib
else
  echo "use prebuild boost"
  # TODO
fi


echo $BOOST_INC
echo $BOOST_LIB_DIR


# build gtest for unit testing
if [ -z "$GTEST_LIB_DIR" ]; then
  echo "build google test on the fly if GTEST_LIB_DIR is not set"
  cd $BASE_DIR/3rdparty/googletest
  cmake . && make -j $cpuNum
  GTEST_INC=$BASE_DIR/3rdparty/googletest/googletest/include
  GTEST_LIB_DIR=$BASE_DIR/3rdparty/googletest/lib
  cd $BASE_DIR
else
  echo "use prebuild google test, GTEST_LIB_DIR = " $GTEST_LIB_DIR
fi

# build all test cases
cd $BASE_DIR/gtests
rm -rf build && mkdir build && cd build
cmake ../ -DGTEST_INC=$GTEST_INC -DGTEST_LIB_DIR=$GTEST_LIB_DIR \
          -DBOOST_INC=$BOOST_INC -DBOOST_LIB_DIR=$BOOST_LIB_DIR
make -j $cpuNum
cd $BASE_DIR

# run all test cases
cd $BASE_DIR/gtests/build && make -j $cpuNum test
