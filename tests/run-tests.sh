#!/usr/bin/env bash

set -e

dir=`dirname $0`
dir=`cd $dir; pwd`

make -C $dir/tries -j8 test &
make -C $dir/succinct -j8 test

wait
