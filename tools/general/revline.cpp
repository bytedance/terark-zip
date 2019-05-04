#include <stdio.h>
#include <algorithm>
#include <terark/util/linebuf.hpp>

int main(int argc, char* argv[]) {
	terark::LineBuf line;
	FILE* fp = stdin;
	while (line.getline(fp) >= 0) {
		line.chomp();
		std::reverse(line.begin(), line.end());
		printf("%s\n", line.p);
	}
	return 0;
}
