//
// Created by leipeng on 2019-10-20.
//

#include <getopt.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#ifdef _MSC_VER
   #include <io.h>
#endif

#include <terark/util/sortable_strvec.hpp>

void usage(const char* prog) {
	fprintf(stderr,
R"EOS(Usage: %s Options

  Options:

    -h
       Show this help information

    -i
       ignore error(incomplete unit), when ignore error,
       last incomplete unit of that bson record is keep untouched

    -u integer
       integer number for unit length

)EOS", prog);
	exit(1);
}

int main(int argc, char* argv[]) {
	bool ignore_error = false;
	size_t unitlen = size_t(-1);
	for (;;) {
		int opt = getopt(argc, argv, "hiu:");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'u':
			unitlen = atoi(optarg);
			break;
		case 'i':
			ignore_error = true;
			break;
		case '?':
		case 'h':
		default:
			usage(argv[0]);
		}
	}
GetoptDone:
	if (unitlen >= 65536) {
		fprintf(stderr, "ERROR: invalid unit length = %zd\n", unitlen);
		return 1;
	}
#ifdef _MSC_VER
	_setmode(_fileno(stdin), _O_BINARY);
	_setmode(_fileno(stdout), _O_BINARY);
#endif
	terark::valvec<unsigned char> buf;
	size_t rno = 0;
	while (!feof(stdin)) {
		rno++;
		int32_t bsonlen = 0;
		if (4 != fread(&bsonlen, 1, 4, stdin)) {
			fprintf(stderr, "ERROR: rno:%zd: fread(4, stdin) = %s\n",
					rno, strerror(errno));
			return 1;
		}
		if (bsonlen < 4) {
			fprintf(stderr, "ERROR: rno:%zd: bad bsonlen = %d\n", rno, bsonlen);
			return 1;
		}
		size_t datalen = bsonlen-4;
		if (datalen % unitlen != 0 && !ignore_error) {
			fprintf(stderr,
					"ERROR: rno:%zd: (datalen = %zd) mod (unitlen = %zd) == %zd\n",
					rno, datalen, unitlen, datalen % unitlen);
			return 1;
		}
		buf.resize_no_init(bsonlen);
		*(int32_t*)buf.data() = bsonlen;
		size_t rlen = fread(buf.data()+4, 1, datalen, stdin);
		if (rlen != datalen) {
			fprintf(stderr, "ERROR: rno:%zd: fread(datalen = %zd, stdin) = %s\n",
					rno, datalen, strerror(errno));
			return 1;
		}
		size_t unitnum = datalen / unitlen;
		terark::FixedLenStrVec::sort_raw(buf.data()+4, unitnum, unitlen);
		size_t written = fwrite(buf.data(), 1, bsonlen, stdout);
		if (written != size_t(bsonlen)) {
			fprintf(stderr, "ERROR: rno:%zd: fwrite(bsonlen = %d, stdout) = %s\n",
					rno, bsonlen, strerror(errno));
			return 1;
		}
	}
	return 0;
}
