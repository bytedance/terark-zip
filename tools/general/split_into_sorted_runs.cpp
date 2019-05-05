#include <terark/util/linebuf.hpp>
#include <terark/valvec.hpp>

// write sorted run filename to stdout
// write sorted run content  to filename

using namespace terark;

int main(int argc, char* argv[]) {
	if (argc < 2) {
		fprintf(stderr, "usage: %s fnamePrefix\n", argv[0]);
		return 1;
	}
	const char* fnamePrefix = argv[1];
	if (strlen(fnamePrefix) > 100) {
		fprintf(stderr, "ERROR: fnamePrefix = %s is too long(max 100)\n", fnamePrefix);
		return 1;
	}
	char fname[128];
	LineBuf line;
	valvec<byte_t> prev;
	FILE* fo = NULL;
	int fileIdx = 0;
	while (line.getline(stdin) > 0) {
		line.chomp();
		if (NULL == fo || prev > fstring(line)) {
			if (fo) {
				fclose(fo);
			}
			sprintf(fname, "%s%06d", fnamePrefix, fileIdx++);
			fo = fopen(fname, "w");
			if (NULL == fo) {
				fprintf(stderr, "ERROR: fopen(%s, w) = %s\n", fname, strerror(errno));
				return 2;
			}
			printf("%s\n", fname);
			fflush(stdout);
		}
		prev.assign(line.p, line.n);
		line.push_back('\n');
		size_t wn = fwrite(line.p, 1, line.n, fo);
		if (wn != line.n) {
			fprintf(stderr, "ERROR: fwrite(%s, %zd) = %s\n", fname, line.n, strerror(errno));
			return 3;
		}
	}
	if (fo) {
		fclose(fo);
	}
	return 0;
}

