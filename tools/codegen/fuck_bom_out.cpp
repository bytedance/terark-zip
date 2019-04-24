#include <stdio.h>
#include <string.h>

int main() {
	char buf[4096];
	size_t n = fread(buf, 1, 3, stdin);
	if (3==n) {
		if (memcmp("\xEF\xBB\xBF", buf, 3) != 0) {
			fwrite(buf, 1, 3, stdout);
		}
	}
	while ((n = fread(buf, 1, sizeof(buf), stdin)) > 0) {
		fwrite(buf, 1, n, stdout);
	}
	return 0;
}

