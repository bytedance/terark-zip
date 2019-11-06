all:
	cd ../../ && make -j 12 && cd tools/zbs && make clean && make

test: all
	cd ../../build/*/lib && TERARK_DYLIB_DIR=`pwd` && cd ../../../tests/zbs && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T d -V -C -c 2 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T m -V -C -c 2 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T p -V -C -c 2 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T o -V -C -c 2 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T e -V -C -c 2 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T d -V -C -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T m -V -C -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T p -V -C -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T o -V -C -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T e -V -C -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T d -V -C -c 2 -t 1 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T m -V -C -c 2 -t 1 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T p -V -C -c 2 -t 1 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T o -V -C -c 2 -t 1 -j 64 -o /tmp/zbs_test sample.txt && \
	env DYLD_LIBRARY_PATH=$$TERARK_DYLIB_DIR ../../tools/zbs/dbg/zbs_build.exe -T e -V -C -c 2 -t 1 -j 64 -o /tmp/zbs_test sample.txt && \
	echo "all zbs tests passed"
