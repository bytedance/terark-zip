#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <terark/util/function.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/cspptrie.inl>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <getopt.h>
#include <random>

using namespace terark;

template<class T> void Unused(const T&) {}

template<class DFA>
void test_empty_dfa(const DFA& dfa) {
	std::unique_ptr<ADFA_LexIterator> iterU(dfa.adfa_make_iter(initial_state));
	auto iter = iterU.get();
	assert(!iter->seek_begin());
	assert(!iter->seek_end());
	assert(!iter->seek_lower_bound(""));
	assert(!iter->seek_lower_bound("1"));
	assert(!iter->seek_lower_bound("2"));
	assert(!iter->seek_lower_bound("9"));
	assert(!iter->seek_lower_bound("\xFF"));
    Unused(iter);
}
template<class Trie, class DawgType>
void test_empty_dfa(const NestTrieDAWG<Trie, DawgType>& dfa) {}

template<class DFA, class Inserter>
void unit_test_run(Inserter insert);
void unit_test();
void run_benchmark();
void test_run_impl(const MatchingDFA& dfa, const hash_strmap<>& strVec);

void usage(const char* prog) {
    fprintf(stderr, R"EOS(usage: %s Options

Options:
    -a
       test Automata<State32>
    -n
       test NLT
    -b
       run benchmark
    -f dfa_file
       load dfa_file for test
    -I
       with benchmark for iterator create
    -?
)EOS", prog);
}

bool test_nlt = false;
bool bench_iter_create = false;
const char* dfa_file = NULL;
const char* fname = "fab-data.txt";
FILE* fp = NULL;

int main(int argc, char* argv[]) {
    bool benchmark = false;
    for (int opt = 0; (opt = getopt(argc, argv, "bnIhf:")) != -1; ) {
        switch (opt) {
        case '?': usage(argv[0]); return 3;
        case 'b':
            benchmark = true;
            break;
        case 'n':
            test_nlt = true;
            break;
        case 'I':
            bench_iter_create = true;
            break;
        case 'f':
            dfa_file = optarg;
            break;
        }
    }
    if (optind < argc) {
        fname = argv[optind];
    }
    fp = fopen(fname, "r");
    if (!fp) {
        fprintf(stderr, "fatal: fopen(%s) = %s\n", fname, strerror(errno));
    }
    if (benchmark) {
        run_benchmark();
    }
    else {
        unit_test();
    }
    return 0;
}

void run_benchmark() {
    MainPatricia trie(sizeof(uint32_t), 1<<20, Patricia::SingleThreadShared);
    hash_strmap<uint32_t> strVec;
    LineBuf line;
    size_t lineno = 0;
    size_t bytes = 0;
    printf("reading file %s ...\n", fname);
    profiling pf;
    auto t0 = pf.now();
    while (line.getline(fp) > 0) {
        line.trim();
        strVec[line]++;
        bytes += line.size();
        lineno++;
        if (lineno % TERARK_IF_DEBUG(1000, 10000) == 0) {
            //printf("lineno=%zd\n", lineno);
            printf("."); fflush(stdout);
        }
    }
    if (strVec.end_i() == 0) {
        printf("input is empty\n");
        return;
    }
    printf("\n");
    auto t1 = pf.now();
    printf("read  input: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, lines=%zd\n",
            pf.sf(t0,t1), lineno / pf.uf(t0,t1), bytes / pf.uf(t0,t1), lineno);

    bytes = 0;
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        bytes += strVec.key_len(i);
    }

    Patricia::WriterToken wtoken(&trie);
    t0 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        fstring key = strVec.key(i);
        auto  & val = strVec.val(i);
        wtoken.update();
        bool ok = wtoken.insert(key, &val);
        assert(ok);
        Unused(ok);
        assert(wtoken.value_of<uint32_t>() == val);
    }
    trie.set_readonly();
    t1 = pf.now();
    printf("trie insert: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec\n"
        , pf.sf(t0,t1), strVec.end_i()/pf.uf(t0,t1), bytes/pf.uf(t0,t1));

    t0 = pf.now();
    strVec.sort_slow();
    t1 = pf.now();
    printf("svec   sort: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec\n"
        , pf.sf(t0,t1), strVec.end_i()/pf.uf(t0,t1), bytes/pf.uf(t0,t1));

    t0 = pf.now();
    valvec<size_t> shuf(strVec.size(), valvec_no_init());
    for (size_t i = 0; i < shuf.size(); ++i) shuf[i] = i;
    std::shuffle(shuf.begin(), shuf.end(), std::mt19937());
    t1 = pf.now();
    printf("shuf  index: time = %10.3f\n", pf.sf(t0,t1));

    char iter_mem[Patricia::ITER_SIZE];
    trie.construct_iter(iter_mem);
    Patricia::Iterator& iter = *reinterpret_cast<Patricia::Iterator*>(&iter_mem);
    t0 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        fstring key = strVec.key(i);
        bool ok = iter.seek_lower_bound(key);
        Unused(ok);
        assert(ok);
    }
    t1 = pf.now();
    printf("lower_bound: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec\n"
        , pf.sf(t0,t1), strVec.end_i()/pf.uf(t0,t1), bytes/pf.uf(t0,t1));

    auto t2 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        fstring key = strVec.key(i);
        auto    val = strVec.val(i);
        bool ok = iter.lookup(key);
        Unused(ok);
        assert(ok);
        assert(iter.value_of<uint32_t>() == val);
    }
    auto t3 = pf.now();
    printf("trie lookup: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));

  if (bench_iter_create) {
    t2 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        char iter_mem2[Patricia::ITER_SIZE];
        trie.construct_iter(iter_mem2);
        Patricia::Iterator& iter2 = *reinterpret_cast<Patricia::Iterator*>(&iter_mem2);
        fstring key = strVec.key(i);
        bool ok = iter2.seek_lower_bound(key);
        Unused(ok);
        assert(ok);
#if !defined(NDEBUG)
        auto va1 = strVec.val(i);
        auto va2 = iter2.value_of<uint32_t>();
        assert(va1 == va2);
#endif
        iter2.~Iterator();
    }
    t3 = pf.now();
    printf("NewIter low: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));
  }
    t2 = pf.now();
    bool hasData = iter.seek_begin();
    assert(hasData);
    for (size_t i = 0; i < strVec.end_i() - 1; ++i) {
        fstring key = strVec.key(i);
        fstring ke2 = iter.word();
#if !defined(NDEBUG)
        auto    va1 = strVec.val(i);
        auto    va2 = iter.value_of<uint32_t>();
        assert(va1 == va2);
        assert(key == ke2);
#endif
        bool ok = iter.incr();
        assert(ok);
        Unused(ok);
    }
    hasData = iter.incr();
    assert(!hasData);
    t3 = pf.now();
    printf("trie   walk: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));

    printf("\n------ random ----------------------\n\n");
    fstrvec fstrVec;
    fstrVec.reserve(strVec.end_i());
    fstrVec.reserve_strpool(strVec.total_key_size());
    t2 = pf.now();
    for(size_t i = 0; i < strVec.end_i(); ++i) {
        size_t j = shuf[i];
        fstring key = strVec.key(j);
        fstrVec.push_back(key);
    }
    t3 = pf.now();
    printf("WrtStr Rand: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound seq\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));

    auto t4 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        fstring key = fstrVec[i];
        bool ok = iter.seek_lower_bound(key);
        assert(ok);
        Unused(ok);
    }
    auto t5 = pf.now();
    printf("lower_bound: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X TIME  of lower_bound seq\n"
        , pf.sf(t4,t5), strVec.end_i()/pf.uf(t4,t5), bytes/pf.uf(t4,t5)
        , pf.nf(t4,t5)/pf.nf(t0,t1));

    t0 = t4;
    t1 = t5;

    t2 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        fstring key = fstrVec[i];
        bool ok = iter.lookup(key);
        assert(ok);
        Unused(ok);
    }
    t3 = pf.now();
    printf("trie lookup: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));

  if (bench_iter_create) {
    t2 = pf.now();
    for (size_t i = 0; i < strVec.end_i(); ++i) {
        char iter_mem2[Patricia::ITER_SIZE];
        trie.construct_iter(iter_mem2);
        Patricia::Iterator& iter2 = *reinterpret_cast<Patricia::Iterator*>(&iter_mem2);
        fstring key = fstrVec[i];
        bool ok = iter2.seek_lower_bound(key);
        assert(ok);
        Unused(ok);
    }
    t3 = pf.now();
    printf("NewIter low: time = %10.3f, QPS = %10.3f M, TP = %10.3f MB/sec, %8.3f X speed of lower_bound\n"
        , pf.sf(t2,t3), strVec.end_i()/pf.uf(t2,t3), bytes/pf.uf(t2,t3)
        , pf.nf(t0,t1)/pf.nf(t2,t3));
  }
    iter.~Iterator();
}

template<class NLT>
void unit_nlt() {
    printf("unit_test_run: %s\n\n", BOOST_CURRENT_FUNCTION);
    {
        auto insert = [](NLT& nlt, const hash_strmap<>& strVec) {
            SortableStrVec sVec;
            sVec.reserve(strVec.size(), strVec.total_key_size());
            for (size_t i = 0, n = strVec.end_i(); i < n; i++) {
                fstring key = strVec.key(i);
                sVec.push_back(key);
            }
            NestLoudsTrieConfig conf;
            nlt.build_from(sVec, conf);
        };
        unit_test_run<NLT>(insert);
    }
    rewind(stdin);
    printf("\n");
}

void unit_test() {
    if (dfa_file) {
        std::unique_ptr<MatchingDFA> dfa(MatchingDFA::load_from(dfa_file));
        hash_strmap<> strVec;
        strVec.i_know_data_is_sorted();
        printf("readed dfa file, keys = %zd\n", strVec.size());
        test_run_impl(*dfa, strVec);
        if (auto pt = dynamic_cast<const MainPatricia*>(dfa.get())) {
            MainPatricia dyna(pt->get_valsize(), pt->mem_size() * 9/8);
            MainPatricia::WriterToken token(&dyna);
            for (size_t i = 0; i < strVec.end_i(); ++i) {
                fstring key = strVec.key(i);
                size_t v = i;
                dyna.insert(key, &v, &token);
            }
            printf("inserted to MainPatricia, keys = %zd\n", strVec.size());
            test_run_impl(dyna, strVec);
        }
        return;
    }
    if (test_nlt) {
        unit_nlt<NestLoudsTrieDAWG_IL_256_32_FL>();
        unit_nlt<NestLoudsTrieDAWG_IL_256      >();
    }
    {
        printf("unit_test_run: MainPatricia\n\n");
        {
            MainPatricia trie(sizeof(uint32_t));
            MainPatricia::WriterToken token(&trie);
            for (uint32_t i = 0; i < 256; ++i) {
                char strkey[2] = { char(i), '\0' };
                token.insert(fstring(strkey, 2), &i);
            }
            {
                uint32_t val = UINT32_MAX;
                token.insert(fstring(""), &val);
            }
            char iter_mem[Patricia::ITER_SIZE];
            trie.construct_iter(iter_mem);
            Patricia::Iterator& iter = *reinterpret_cast<Patricia::Iterator*>(&iter_mem);
            printf("MainPatricia iter incr basic...\n");
            {
                bool ok = iter.seek_begin();
                assert(ok);
                assert(iter.value_of<uint32_t>() == UINT32_MAX);
                for (uint32_t i = 0; i < 256; ++i) {
                    ok = iter.incr();
                    assert(ok);
                    assert(iter.value_of<uint32_t>() == i);
                }
                ok = iter.incr();
                assert(!ok);
            }
            printf("MainPatricia iter incr basic... passed\n");

            printf("MainPatricia iter decr basic...\n");
            {
                bool ok = iter.seek_end();
                assert(ok);
                uint32_t i = 256;
                while (i) {
                    --i;
                    assert(iter.value_of<uint32_t>() == i);
                    ok = iter.decr();
                    assert(ok);
                }
                assert(iter.value_of<uint32_t>() == UINT32_MAX);
                ok = iter.decr();
                assert(!ok);
            }
            iter.~Iterator();
            printf("MainPatricia iter decr basic... passed\n");
        }
        auto insert = [](MainPatricia& trie, const hash_strmap<>& strVec) {
            MainPatricia::WriterToken token(&trie);
            for (size_t i = 0, n = strVec.end_i(); i < n; i++) {
                fstring key = strVec.key(i);
                token.update();
                trie.insert(key, NULL, &token);
            }
        };
        unit_test_run<MainPatricia>(insert);
    }
}
/*
// for Automata<State>
template<class DFA>
void minimize_and_path_zip(DFA& dfa) {
    printf("dfa.adfa_minimize()...\n");
    dfa.adfa_minimize();
    printf("dfa.path_zip(\"DFS\")...\n");
    dfa.path_zip("DFS");
}
*/
template<class Trie, class DawgType>
void minimize_and_path_zip(NestTrieDAWG<Trie, DawgType>&) {}
void minimize_and_path_zip(MainPatricia&) {}

template<class DFA, class Inserter>
void unit_test_run(Inserter insert) {
	DFA dfa;
	test_empty_dfa(dfa);
	hash_strmap<> strVec;
	LineBuf line;
	size_t lineno = 0;
	printf("reading file %s ...\n", fname);
	while (line.getline(fp) > 0) {
		line.trim();
		strVec.insert_i(line);
		lineno++;
		if (lineno % TERARK_IF_DEBUG(1000, 10000) == 0) {
			//printf("lineno=%zd\n", lineno);
            printf("."); fflush(stdout);
		}
	}
    printf("\n");
	printf("done, lines=%zd...\n", lineno);
	if (strVec.size() > 0) {
        insert(dfa, strVec);
		minimize_and_path_zip(dfa);
		printf("strVec.sort_slow()...\n");
		strVec.sort_slow();
		printf("strVec.key(0).size() = %zd\n", strVec.key(0).size());
        test_run_impl(dfa, strVec);
	}
}

void test_run_impl(const MatchingDFA& dfa, const hash_strmap<>& strVec) {
	std::unique_ptr<ADFA_LexIterator> iterU(dfa.adfa_make_iter(initial_state));
	auto iter = iterU.get();

	printf("test incr...\n");
	bool hasData = iter->seek_begin();
    Unused(hasData);
	assert(hasData);
	for (size_t i = 0; i < strVec.end_i() - 1; ++i) {
		fstring iw = iter->word();
		fstring vw = strVec.key(i);
		if (iw != vw) {
			printf("iw:%zd: len=%zd: %.*s\n", i, iw.size(), iw.ilen(), iw.data());
			printf("vw:%zd: len=%zd: %.*s\n", i, vw.size(), vw.ilen(), vw.data());
		}
		assert(iw == vw);
		hasData = iter->incr();
		assert(hasData);
	}
	assert(!iter->incr());
	printf("test incr passed\n\n");

	printf("test decr...\n");
	hasData = iter->seek_end();
	for(size_t i = strVec.end_i(); i-- > 1; ) {
		fstring iw = iter->word();
		fstring vw = strVec.key(i);
		if (iw != vw) {
			printf("iw:%zd: %.*s\n", i, iw.ilen(), iw.data());
			printf("vw:%zd: %.*s\n", i, vw.ilen(), vw.data());
		}
		assert(iw == vw);
		hasData = iter->decr();
		assert(hasData);
        Unused(hasData);
	}
	assert(!iter->decr());
	printf("test decr passed\n\n");

	printf("test lower_bound + incr...\n");
	valvec<size_t> shuf(strVec.size(), valvec_no_init());
	for (size_t i = 0; i < shuf.size(); ++i) shuf[i] = i;
	std::shuffle(shuf.begin(), shuf.end(), std::mt19937());
	size_t cnt = 10;
	for(size_t j = 0; j < strVec.end_i()/cnt; ++j) {
		size_t k = shuf[j];
		fstring key = strVec.key(k);
		hasData = iter->seek_lower_bound(key);
		assert(hasData);
		for (size_t i = k; i < std::min(k + cnt, strVec.end_i()-1); ++i) {
			fstring iw = iter->word();
			fstring vw = strVec.key(i);
			if (iw != vw) {
				printf("iw:%zd: len=%zd: %.*s\n", i, iw.size(), iw.ilen(), iw.data());
				printf("vw:%zd: len=%zd: %.*s\n", i, vw.size(), vw.ilen(), vw.data());
			}
			assert(iw == vw);
			hasData = iter->incr();
			assert(hasData);
            Unused(hasData);
		}
	}
	printf("test lower_bound + incr passed\n\n");

	printf("test lower_bound out of range key...\n");
	std::string key = strVec.key(strVec.size()-1).str();
	hasData = iter->seek_lower_bound(key);
	assert(hasData);
	hasData = iter->incr();
	assert(!hasData);
	for (size_t i = 0; i < key.size(); ++i) {
		std::string key2 = key;
		if ((unsigned char)key2[i] < 255) {
			key2[i]++;
			hasData = iter->seek_lower_bound(key2);
			assert(!hasData);
		}
	}
    Unused(hasData);
	key.push_back('1');
	hasData = iter->seek_lower_bound(key);
	assert(!hasData);
	printf("test lower_bound out of range key passed\n\n");

	printf("test lower_bound minimum key...\n");
	key = strVec.key(0).str();
	hasData = iter->seek_lower_bound(key);
	assert(hasData);
	assert(iter->word() == key);
	hasData = iter->decr();
	assert(!hasData);
	for (size_t i = 0; i < key.size(); ++i) {
		std::string key2 = key;
		if ((unsigned char)key2[i] > 0) {
			key2[i]--;
			hasData = iter->seek_lower_bound(key2);
			assert(hasData);
			assert(key2 < iter->word());
			hasData = iter->decr();
			assert(!hasData);
		}
	}
	if (key.size()) {
		std::string key2 = key;
		key2.pop_back();
		hasData = iter->seek_lower_bound(key2);
		assert(hasData);
		assert(key2 < iter->word());
		hasData = iter->decr();
		assert(!hasData);
        Unused(hasData);
	}
	printf("test lower_bound minimum key passed\n\n");

	printf("test lower_bound median key...\n");
	key = strVec.key(0).str();
	key.push_back('\1');
	hasData = iter->seek_lower_bound(key);
	assert(hasData || strVec.size() == 1);
	printf("test lower_bound median key passed...\n\n");

	printf("test lower_bound + decr...\n");
	for(size_t j = 0; j < strVec.end_i()/cnt; ++j) {
		size_t k = shuf[j];
		k = std::max(k, cnt);
		fstring randKey = strVec.key(k);
		iter->seek_lower_bound(randKey);
		for (size_t i = k; i > k - cnt; ) {
			--i;
			hasData = iter->decr();
			assert(hasData);
			fstring iw = iter->word();
			fstring vw = strVec.key(i);
			if (iw != vw) {
				printf("--------------------------\n");
				printf("iw:%zd: %.*s\n", strVec.find_i(iw), iw.ilen(), iw.data());
				printf("vw:%zd: %.*s\n", i, vw.ilen(), vw.data());
			}
			assert(iw == vw);
		}
		for (size_t i = k - cnt; i < k; ++i) {
			fstring iw = iter->word();
			fstring vw = strVec.key(i);
			if (iw != vw) {
				printf("--------------------------\n");
				printf("iw:%zd: %.*s\n", strVec.find_i(iw), iw.ilen(), iw.data());
				printf("vw:%zd: %.*s\n", i, vw.ilen(), vw.data());
			}
			assert(iw == vw);
			hasData = iter->incr();
			assert(hasData);
		}
	}
	printf("test lower_bound + decr passed\n\n");

	printf("test lower_bound as upper_bound + decr...\n");
	for(size_t j = 0; j < strVec.end_i()/cnt; ++j) {
		size_t k = shuf[j];
		fstring rk = strVec.key(k);
        if (rk.size() == 0)
            continue;
		std::string randKey = rk.substr(0, rand() % rk.size())
							+ rk.substr(rand() % rk.size(), 1);
		k = strVec.lower_bound(randKey);
		if (k < cnt)
			continue; // did not meet test condition
		if (iter->seek_lower_bound(randKey)) {
            {
                fstring iw = iter->word();
                fstring vw = strVec.key(k);
                if (iw != vw) {
                    printf("RandKey: len=%zd: %s------------------\n", randKey.size(), randKey.c_str());
                    printf("iw:%zd: len=%zd: %.*s\n", k, iw.size(), iw.ilen(), iw.data());
                    printf("vw:%zd: len=%zd: %.*s\n", k, vw.size(), vw.ilen(), vw.data());
                }
                assert(iw == vw);
            }
		    for (size_t i = k; i > k - cnt; ) {
			    --i;
			    hasData = iter->decr();
			    assert(hasData);
			    fstring iw = iter->word();
			    fstring vw = strVec.key(i);
			    if (iw != vw) {
				    printf("--------------------------\n");
				    printf("iw:%zd: %.*s\n", strVec.find_i(iw), iw.ilen(), iw.data());
				    printf("vw:%zd: %.*s\n", i, vw.ilen(), vw.data());
			    }
			    assert(iw == vw);
		    }
		    for (size_t i = k - cnt; i < k; ++i) {
			    fstring iw = iter->word();
			    fstring vw = strVec.key(i);
			    if (iw != vw) {
				    printf("--------------------------\n");
				    printf("iw:%zd: %.*s\n", strVec.find_i(iw), iw.ilen(), iw.data());
				    printf("vw:%zd: %.*s\n", i, vw.ilen(), vw.data());
			    }
			    assert(iw == vw);
			    hasData = iter->incr();
			    assert(hasData);
		    }
        }
        else {
            fstring maxKey = strVec.key(strVec.end_i()-1);
            assert(maxKey < randKey);
            Unused(maxKey);
        }
	}
	printf("test lower_bound as upper_bound + decr passed\n\n");

    printf("test seek_max_prefix\n");
    // TODO: Add more case
    for(size_t j = 0; j < strVec.end_i(); ++j) {
        size_t k = shuf[j];
        fstring rk = strVec.key(k);
        if (rk.size() == 0)
            continue;
        size_t r1 = rand() % rk.size();
        size_t r2 = rand() % rk.size();
        std::string randKey = rk.substr(0, r1) + rk.substr(r2, rk.size() - r2);
        size_t cplen = rk.commonPrefixLen(randKey);
        size_t partial_len  = iter->seek_max_prefix(randKey);
    //    printf("randKey = len=%zd: %s\n", randKey.size(), randKey.c_str());
    //    printf("cplen = %zd, partial_len = %zd, iter->word() = len=%zd: \"%s\"\n"
    //            , cplen, partial_len, iter->word().size(), iter->word().c_str());
        assert(partial_len <= randKey.size());
        assert(partial_len >= iter->word().size());
        assert(partial_len >= cplen);
        assert(memcmp(randKey.data(), iter->word().data(), iter->word().size()) == 0);
        k = strVec.lower_bound(randKey);
        assert(k == strVec.end_i() || iter->word() <= strVec.key(k));
        if (k < strVec.end_i() && strVec.key(k).startsWith(iter->word())) {
            assert(strVec.key(k).startsWith(iter->word()));
        }
        else if (k > 0) {
            assert(strVec.key(k-1).startsWith(iter->word()));
        }
        if (iter->incr()) {
            if (!(randKey < iter->word())) {
            //    printf("fail: randKey  = %s\n", randKey.c_str());
            //    printf("fail: iterWord = %s\n", iter->word().c_str());
            }
            //assert(randKey < iter->word());
        }
        iter->decr();
        if (iter->decr()) {
            //assert(randKey > iter->word());
        }
        iter->incr();
        iter->incr();
    }
    printf("test seek_max_prefix passed\n\n");
	TERARK_UNUSED_VAR(hasData);
    Unused(hasData);
}

