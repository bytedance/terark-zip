//
// Created by leipeng on 2019-10-20.
//
#include <stdio.h>
#include <terark/lcast.hpp>
#include <terark/util/linebuf.hpp>

using namespace terark;

int main() {
	LineBuf line;
	valvec<byte_t> obuf;
	while (line.getline(stdin) > 0) {
		line.chomp();
		obuf.resize_no_init(line.size()/2 + 1 + 4);
		size_t hexstrlen = hex_decode(line.p, line.n, obuf.data() + 4, obuf.capacity() - 4);
		// ignore (hexstrlen % 2 == 1)
		size_t datalen = hexstrlen/2;
		size_t bsonlen = 4 + datalen;
		*(uint32_t*)obuf.data() = bsonlen;
		size_t written = fwrite(obuf.data(), 1, bsonlen, stdout);
		if (written != bsonlen) {
			perror("fwrite(stdout) failed");
			exit(1);
		}
	}
	return 0;
}
