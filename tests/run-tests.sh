#!/usr/bin/env bash

dir=`dirname $0`
dir=`cd $dir; pwd`

make -C $dir/tries -j8
make -C $dir/succinct -j8