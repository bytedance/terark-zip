#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <terark/util/autoclose.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/fstring.hpp>
#include <terark/bitmap.hpp>
#include <terark/valvec.hpp>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr,
R"EOS(Usage: %s Options output-keys output-values
  Options:
    -d delimeter
    -k fields: such as 0,1,2 or 4,2,3,0,1 ...
    -h Show this help information
    -v Show verbose info
  Read input from stdin
)EOS", prog);
	exit(1);
}

int main(int argc, char* argv[]) {
//	bool verbose = false;
	febitvec isKey;
	fstring delim = "\t";
	valvec<size_t> keyFields;
	for (;;) {
		int opt = getopt(argc, argv, "hd:k:v");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'd':
			delim = optarg;
			break;
		case 'k':
			for(char* p = optarg; *p;) {
				char* endp = NULL;
				long fieldIndex = strtol(p, &endp, 10);
				if (fieldIndex < 0 || endp == p) {
					fprintf(stderr, "ERROR: invalid arg: -k '%s'; @pos %zd\n", optarg, endp-optarg);
					return 1;
				}
				isKey.ensure_set(fieldIndex, 1);
				keyFields.push_back(fieldIndex);
				if ('\0' == *endp)
					break;
				p = endp + 1;
			}
			break;
		case 'v':
		//	verbose = true;
			break;
		case '?':
		case 'h':
		default:
			usage(argv[0]);
		}
	}
GetoptDone:
	if (keyFields.empty()) {
		fprintf(stderr, "ERROR: key fields is empty\n");
		return 1;
	}
	const char* fnameKeys = argv[optind + 0];
	const char* fnameVals = argv[optind + 1];
	Auto_close_fp ofkeys(fopen(fnameKeys, "w"));
	Auto_close_fp ofvals(fopen(fnameVals, "w"));
	if (!ofkeys) {
		fprintf(stderr, "FATAL: fopen(%s, wb) = %s\n", fnameKeys, strerror(errno));
		return 1;
	}
	if (!ofvals) {
		fprintf(stderr, "FATAL: fopen(%s, wb) = %s\n", fnameVals, strerror(errno));
		return 1;
	}
	size_t lineno = 0;
	terark::LineBuf line;
	valvec<fstring> F;
	valvec<char> key, val;
	while (line.getline(stdin) > 0) {
		lineno++;
		line.chomp();
		line.split(delim, &F, isKey.size());
		key.erase_all();
		val.erase_all();
		// keyFields can be reordered, such as: 4,2,3,0,1
		for(size_t i = 0; i < keyFields.size(); ++i) {
			size_t j = keyFields[i];
			if (j < F.size()) {
				key.append(F[j]);
				key.append(delim);
			}
			else {
				fprintf(stderr, "WARN: line: %zd: key field index = %zd out of range, insert an empty key field\n", lineno, j);
				key.append(delim);
			}
		}
		for (size_t i = 0; i < F.size(); ++i) {
			if (i >= isKey.size() || !isKey[i]) {
				val.append(F[i]);
				val.append(delim);
			}
		}
		if (!key.empty()) key.pop_n(delim.size());  key.push_back('\n');
		if (!val.empty()) val.pop_n(delim.size());  val.push_back('\n');
		size_t n = fwrite(key.data(), 1, key.size(), ofkeys);
		if (key.size() != n) {
			fprintf(stderr, "ERROR: lineno: %zd: fwrite(key, %zd) = %s\n", lineno, key.size(), strerror(errno));
			return 1;
		}
		n = fwrite(val.data(), 1, val.size(), ofvals);
		if (val.size() != n) {
			fprintf(stderr, "ERROR: lineno: %zd: fwrite(val, %zd) = %s\n", lineno, val.size(), strerror(errno));
			return 1;
		}
	}
	return 0;
}

