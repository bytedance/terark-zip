#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <terark/util/autoclose.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>
#include <terark/fstring.hpp>
#include <terark/io/FileStream.hpp>
#include <getopt.h>
#if defined(_MSC_VER)
#include <io.h>
#else
#include <unistd.h>
#endif
#include <fcntl.h>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr,
R"EOS(Usage: %s Options [Input-File-List]
  Options:
    -h Show this help information
    -o Output-File
    -v Show verbose info
    -k Save file name as key, default DO NOT save file name
  If Input-File-List is empty, read file names from stdin
)EOS", prog);
	exit(1);
}

int main(int argc, char* argv[]) {
//	bool saveFilename = false;
	bool verbose = false;
//	bool isBson = true; // size including uint32 size itself
	const char* ofname = NULL;
	llong maxFileSize = UINT32_MAX;
	for (;;) {
		int opt = getopt(argc, argv, "hkm:o:v");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'k':
		//	saveFilename = true;
			break;
		case 'm':
			maxFileSize = strtoll(optarg, NULL, 0);
			maxFileSize = std::max<llong>(maxFileSize, 1*1024);
			maxFileSize = std::min<llong>(maxFileSize, UINT32_MAX);
			break;
		case 'o':
			ofname = optarg;
			break;
		case 'v':
			verbose = true;
			break;
		case '?':
		case 'h':
		default:
			usage(argv[0]);
		}
	}
GetoptDone:
	Auto_close_fp ofp;
	if (NULL == ofname) {
		fprintf(stderr, "%s: not specified output file, will write to stdout\n", argv[0]);
	}
	else {
		ofp = fopen(ofname, "wb");
		if (!ofp) {
			fprintf(stderr, "FATAL: fopen(%s, wb) = %s\n", ofname, strerror(errno));
			return 1;
		}
	}
	NonOwnerFileStream ofs(ofp.self_or(stdout));
	terark::fstrvec fnameList;
	terark::profiling pf;
	if (optind < argc) {
		for (int i = optind; i < argc; ++i) {
		//	fprintf(stderr, "%s\n", argv[i]);
			fnameList.push_back(fstring(argv[i]));
			fnameList.back_append('\0');
		}
	//	fprintf(stderr, "%s: %zd file from command line\n", argv[0], fnameList.size());
	}
	else {
		fprintf(stderr
			, "%s: not specified input file list, reading file list from stdin...\n"
			, argv[0]);
		LineBuf line;
		while (line.getline(stdin) > 0) {
			line.chomp();
			fnameList.emplace_back(line.p, line.n+1);
		}
	}
	for (size_t i = 0; i < fnameList.size(); ++i) {
		const char* fname = fnameList.c_str(i);
		FileStream fp;
		if (!fp.xopen(fname, "rb")) {
			fprintf(stderr, "ERROR: fopen(%s, rb) = %s, skip...\n", fname, strerror(errno));
			continue;
		}
		llong fsize = llong(fp.fsize());
		if (verbose) {
			fprintf(stderr, "INFO: FileLen =%8lld  Path = %s\n", fsize, fname);
		}
		fp.disbuf();
		if (fsize > maxFileSize) {
			fprintf(stderr, "ERROR: fsize(%s) = %lld exceeds limit: %lld, skip...\n",
				fname, fsize, maxFileSize);
			continue;
		}
		uint32_t fsize32 = uint32_t(fsize) + 4; // including the size itself
		ofs.ensureWrite(&fsize32, 4);
		ofs.cat(fp);
	}
	return 0;
}
