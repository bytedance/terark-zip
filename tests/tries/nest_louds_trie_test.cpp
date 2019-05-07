#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <terark/fsa/nest_louds_trie.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <getopt.h>

using namespace terark;

int main(int argc, char* argv[]) {
	bool b_write_dot_file = false;
	NestLoudsTrieConfig conf;
	conf.nestLevel = 5;
	for (;;) {
		int opt = getopt(argc, argv, "n:g");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'n':
			conf.nestLevel = atoi(optarg);
			break;
		case 'g':
			b_write_dot_file = true;
			break;
		case '?':
			fprintf(stderr, "usage: %s options text_file\n", argv[0]);
			return 1;
		}
	}
GetoptDone:
	const char* fname = argv[optind];
	Auto_fclose fp;
	if (fname) {
		fp = fopen(fname, "r");
		if (NULL == fp) {
			fprintf(stderr, "FATAL: fopen(\"%s\", \"r\") = %s\n", fname, strerror(errno));
			return 1;
		}
	}
	else {
		fprintf(stderr, "Reading from stdin...\n");
	}
	SortableStrVec strVec1;
	LineBuf line;
	while (line.getline(fp.self_or(stdin)) > 0) {
		line.chomp();
		strVec1.push_back(line);
	}
	NestLoudsTrie_SE trie;
	SortableStrVec strVec2 = strVec1;
	valvec<uint32_t> linkVec(strVec2.size(), UINT32_MAX);
	trie.build_strpool(strVec2, linkVec, conf);
	if (b_write_dot_file)
		trie.write_dot_file((std::string(fname) + ".dot").c_str());
	printf("---------------------\n");
#if !defined(NDEBUG)
	valvec<byte_t> strbuf2;
	for (size_t i = 0; i < linkVec.size(); ++i) {
		trie.restore_string(linkVec[i], &strbuf2);
		fstring s1(strVec1[i]);
		fstring s2(strbuf2);
	//	printf("[%03d]: %-40.*s | %.*s\n",
	//		int(i), s1.ilen(), s1.data(), s2.ilen(), s2.data());
		assert(s1 == s2);
	}
#endif
	printf("sizeof(NestLoudsTrie_SE) = %d\n", (int)sizeof(NestLoudsTrie_SE));
	printf("mem_size=%lld core_mem_size=%lld\n"
		, (long long)trie.mem_size()
		, (long long)trie.core_mem_size()
		);
	return 0;
}

