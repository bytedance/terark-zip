#!/bin/bash

#set -x

BASE=`cd ../../..;pwd`
export PKG_TERARK_HOME=$BASE/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-4.8-bmi2-0
export LD_LIBRARY_PATH=$PKG_TERARK_HOME/lib:$LD_LIBRARY_PATH

cp $BASE/terark/src/terark/succinct/rank_select_fewzero.hpp .
cp $BASE/terark/src/terark/succinct/rank_select_fewzero.cpp .

make clean
make -j4

./dbg/rs_fewzero_ut.exe
./dbg/rank_select_few_test.exe
