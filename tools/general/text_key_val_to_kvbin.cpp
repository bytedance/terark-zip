#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <algorithm>
#include <terark/util/linebuf.hpp>
#include <terark/util/throw.hpp>
#include <boost/static_assert.hpp>
#ifdef _MSC_VER
	#include <fcntl.h>
	#include <io.h>
#endif

int main(int argc, char* argv[]) {
	BOOST_STATIC_ASSERT(sizeof(int) == 4);
	int kvlen[2]; // int32
	int lineno = 0;
#ifdef _MSC_VER
	if (_setmode(_fileno(stdout), _O_BINARY) < 0) {
		THROW_STD(invalid_argument, "set stdout as binary mode failed");
	}
#endif
	terark::LineBuf line;
	while (line.getline(stdin) > 0) {
		lineno++;
		line.chomp();
		if (line.empty()) {
			fprintf(stderr, "line:%d is empty\n", lineno);
			continue;
		}
		const char* beg = line.begin();
		const char* end = line.end();
		const char* tab = std::find(beg, end, '\t');
		if (tab == end) {
			kvlen[1] = 0;
		} else {
			kvlen[1] = end - tab - 1;
		}
		kvlen[0] = tab - beg;
		fwrite(kvlen, 1, sizeof(kvlen), stdout);
		fwrite(beg+0, 1, kvlen[0], stdout);
		fwrite(tab+1, 1, kvlen[1], stdout);
	}
	return 0;
}

