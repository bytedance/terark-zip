#!/bin/bash

set -x
set -e

cd /home/leipeng/terark-mysql/storage/rocksdb/terarkdb/terark-core/tools/zbs
export LD_LIBRARY_PATH=../../build/Linux-x86_64-g++-4.9-bmi2-1/lib_shared

for ifile in /data01/hdfs_data/raw.json.*; do
    for sampleRatio in 030 040 045; do
	ofile=/data00/leipeng/bmq-${ifile#/data01/hdfs_data/raw.json.}
        time env DictZipBlobStore_zipThreads=0 rls/zbs_build.exe -ZEBp -j128 -S 0.$sampleRatio -o ${ofile}.zbs.${sampleRatio} $ifile
	rls/zbs_unzip.exe    -t -b 1 -T 10 ${ofile}.zbs.${sampleRatio}
	rls/zbs_unzip.exe -r -t -b 1 -T 10 ${ofile}.zbs.${sampleRatio}
        time env DictZipBlobStore_zipThreads=0 rls/zbs_build.exe -ZEBp -j128 -S 0.$sampleRatio -o ${ofile}.zbs.${sampleRatio}.huf -e h $ifile
	rls/zbs_unzip.exe    -t -b 1 -T 10 ${ofile}.zbs.${sampleRatio}.huf
	rls/zbs_unzip.exe -r -t -b 1 -T 10 ${ofile}.zbs.${sampleRatio}.huf
    done
    time rls/zbs_build.exe -j128 -z 6 -T o -o ${ofile}.zstd.rec -B $ifile
    rls/zbs_unzip.exe    -t -b 1 -T 10 ${ofile}.zstd.rec
    rls/zbs_unzip.exe -r -t -b 1 -T 10 ${ofile}.zstd.rec
    time zstd -o   ${ofile}.zstd.all $ifile
    time zstd -d < ${ofile}.zstd.all > /dev/null
done

