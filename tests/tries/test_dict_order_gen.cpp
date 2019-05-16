#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/autoclose.hpp>
#include <getopt.h>

using namespace terark;

int main(int argc, char* argv[]) {
    const char* fname = "fab-data.txt";
    if (argc >= 2) {
        fname = argv[1];
    }
    Auto_fclose fp(fopen(fname, "r"));
    if (!fp) {
        fprintf(stderr, "fopen(%s) = %s\n", fname, strerror(errno));
        return 1;
    }
	hash_strmap<> strSet;
	LineBuf line;
	size_t lineno = 0;
	printf("reading file %s ...\n", fname);
	while (line.getline(fp) > 0) {
		line.trim();
		strSet.insert_i(line);
		lineno++;
		if (lineno % TERARK_IF_DEBUG(1000, 10000) == 0) {
			printf("lineno=%zd\n", lineno);
		}
	}
	printf("done, lines=%zd...\n", lineno);

	printf("strSet.sort_slow()...\n");
	strSet.sort_slow();
	if (strSet.size() > 0)
		printf("strSet.key(0).size() = %zd\n", strSet.key(0).size());

	SortableStrVec strVec;
	for (size_t i = 0; i < strSet.size(); ++i) {
		strVec.push_back(strSet.key(i));
	}
	NestLoudsTrieConfig conf;
	NestLoudsTrieDAWG_SE_512 trie;
	trie.build_from(strVec, conf);
	valvec<byte_t> trieKey;
	NonRecursiveDictionaryOrderToStateMapGenerator gen;
	gen(trie, [&](size_t byteLexNth, size_t state) {
		size_t trieIdx = trie.state_to_word_id(state);
		trie.nth_word(trieIdx, &trieKey);
		fstring hashKey = strSet.key(byteLexNth);
		TERARK_RT_assert(hashKey == trieKey, std::logic_error);
	//	printf("%zd %zd\n", byteLexNth, trieIdx);
	//	printf("%s\n", hashKey.c_str());
	});
	return 0;
}

