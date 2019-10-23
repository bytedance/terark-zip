#!/usr/bin/env bash
#
# Usage:
#
#   GTEST_INC=... \     # googletest include dir
#   GTEST_LIB=... \     # googletest static library dir (if not set, will use submodule denpendency to complie on the fly)
#   BOOST_INC=... \
#   BOOST_LIB=... \
#   ./build.sh  
#

BASE_DIR=$PWD
GTEST_INC=
GTEST_LIB=
BOOST_INC=
BOOST_LIB=

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

echo Current BUILD_BRANCH = $BUILD_BRANCH
echo Current BUILD_REPO_BRANCH = $BUILD_REPO_BRANCH

if test -n "$BUILD_BRANCH"; then
    git checkout "$BUILD_BRANCH"
fi

git submodule update --init

# build boost
if [ -z "${BOOST_LIB}" ];then
  echo "build from submodule"
  # TODO
  BOOST_INC=${BASE_DIR}/boost-include
  BOOST_LIB=${BASE_DIR}/$BOOST_LIB/stage/lib
else
  echo "use prebuild boost"
  # TODO
fi

# build core
BRANCH_NAME=`git rev-parse --abbrev-ref HEAD`
echo Current BRANCH_NAME = $BRANCH_NAME

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    sudo apt-get update
    sudo apt-get install libaio-dev
else
    echo you must ensure libaio-dev have been installed
fi

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

# build gtest for unit testing
if [ -z "$GTEST_LIB" ]; then
  echo "build google test on the fly"
  cd $BASE_DIR/3rdparty/googletest
  cmake . && make -j $cpuNum
  GTEST_INC=$BASE_DIR/3rdparty/googletest/googletest/include
  GTEST_LIB=$BASE_DIR/3rdparty/googletest/lib
  cd $BASE_DIR
else
  echo "use prebuild google test, GTEST_LIB = " $GTEST_LIB
fi

cd $BASE_DIR/tests
rm -rf build && mkdir build && cd build
cmake ../ -DGTEST_INC=$GTEST_INC -DGTEST_LIB=$GTEST_LIB \
          -DBOOST_INC=$BOOST_INC -DBOOST_LIB=$BOOST_LIB
make -j $cpuNum
cd $BASE_DIR
