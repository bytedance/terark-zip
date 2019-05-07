#!/usr/bin/env bash

set -e

dir=`dirname $0`
dir=`cd $dir; pwd`

make -C $dir/tries -j8
make -C $dir/succinct -j8

LIB_PATH=$dir/../build/Darwin-x86_64-clang-10.0-bmi2-0/lib

pushd $dir/tries
if [ `uname` == Darwin ]; then
    DYLD_LIBRARY_PATH=$LIB_PATH dbg/test_adfa_iter.exe -n    < fab-data.txt
    DYLD_LIBRARY_PATH=$LIB_PATH dbg/nest_louds_trie_test.exe < fab-data.txt
    DYLD_LIBRARY_PATH=$LIB_PATH dbg/test_dict_order_gen.exe  < fab-data.txt
else
      LD_LIBRARY_PATH=$LIB_PATH dbg/test_adfa_iter.exe -n    < fab-data.txt
      LD_LIBRARY_PATH=$LIB_PATH dbg/nest_louds_trie_test.exe < fab-data.txt
      LD_LIBRARY_PATH=$LIB_PATH dbg/test_dict_order_gen.exe  < fab-data.txt
fi
popd

pushd $dir/succinct
if [ `uname` == Darwin ]; then
    DYLD_LIBRARY_PATH=$LIB_PATH dbg/rank_select_test.exe 10000
    DYLD_LIBRARY_PATH=$LIB_PATH dbg/sorted_uint_vec_test.exe 10000
else
      LD_LIBRARY_PATH=$LIB_PATH dbg/rank_select_test.exe 10000
      LD_LIBRARY_PATH=$LIB_PATH dbg/sorted_uint_vec_test.exe 10000
fi
popd
