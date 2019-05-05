#ifdef _MSC_VER
	#define _CRT_SECURE_NO_WARNINGS
	#define _SCL_SECURE_NO_WARNINGS
    #include <io.h>
    #include <fcntl.h>
#else
	#include <sys/mman.h>
#endif

#include <terark/fsa/nest_louds_trie.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/fstrvec.hpp>
#include <getopt.h>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr, R"EOS(Usage: %s Options Input-TXT-File
Options:
    -h Show this help information
    -d DAWG-File
    -H use huge page
    -b BenchmarkLoop : Run benchmark
    -B input is bson binary keys
    -c cache ratio
    -m read all keys into memory for benchmark
If Input-TXT-File is omitted, use stdin
)EOS", prog);
	exit(1);
}

int main(int argc, char* argv[]) {
	size_t benchmarkLoop = 1;
	double cacheRatio = 0;
	const char* cacheWalkMethod = NULL;
	const char* dawg_fname = NULL;
	bool hugepage = false;
	bool verbose = false;
    bool mem_key = false;
    bool isBson = false;
	for (;;) {
		int opt = getopt(argc, argv, "b:c:d:HvhmB");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'b':
			benchmarkLoop = atoi(optarg);
			break;
		case 'B':
			isBson = true;
			break;
		case 'c':
			cacheRatio = atof(optarg);
			cacheWalkMethod = strchr(optarg, '@');
			if (cacheWalkMethod)
				cacheWalkMethod++;
			break;
		case 'd':
			dawg_fname = optarg;
			break;
		case 'H':
			hugepage = true;
			break;
        case 'm':
            mem_key = true;
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
	if (NULL == dawg_fname) {
		fprintf(stderr, "-d DAWG-File is required\n\n");
		usage(argv[0]);
	}
	terark::profiling pf;
	const char* input_fname = argv[optind];
	Auto_fclose fp;
	if (input_fname) {
		fp = fopen(input_fname, isBson?"rb":"r");
		if (NULL == fp) {
			fprintf(stderr, "FATAL: fopen(\"%s\", \"r\") = %s\n", input_fname, strerror(errno));
			return 1;
		}
	}
	else {
		fprintf(stderr, "Reading from stdin...\n");
	}
	LineBuf fileData;
	std::unique_ptr<BaseDFA> dfa;
try {
	if (hugepage) {
		fileData.read_all(dawg_fname, size_t(4)<<20);
		dfa.reset(BaseDFA::load_mmap_user_mem(fileData.data(), fileData.size()));
#if defined(MADV_HUGEPAGE) && 0 // madvise(hugepage) was called in read_all
		if (madvise(fileData.data(), fileData.size(), MADV_HUGEPAGE) != 0) {
			fprintf(stderr, "FAIL: madivise(MADV_HUGEPAGE) = %s\n", strerror(errno));
		}
		else {
			fprintf(stderr, "INFO: madivise(MADV_HUGEPAGE) success\n");
		}
#endif
	}
	else {
		dfa.reset(BaseDFA::load_from(dawg_fname));
	}
}
catch (const std::exception& ex) {
    fprintf(stderr, "load nlt error: %s\n", ex.what());
    return 1;
}
	auto dawg = dfa->get_dawg();
	if (!dawg) {
		fprintf(stderr, "ERROR: dfa is not a DAWG, file: %s\n", dawg_fname);
		return 1;
	}
	if (auto cache = dynamic_cast<FSA_Cache*>(dfa.get())) {
		cache->build_fsa_cache(cacheRatio, cacheWalkMethod);
		cache->print_fsa_cache_stat(stderr);
	}
#ifdef _MSC_VER
    if (isBson) {
        _setmode(_fileno(fp.self_or(stdin)), _O_BINARY);
    }
#endif
	long long t0 = pf.now();
	size_t numWords = dawg->num_words();
	size_t lines = 0;
	size_t bytesTotal = 0;
	size_t bytesFound = 0, bytesMiss = 0;
	size_t keysFound = 0, keysMiss = 0;
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define read_onekey()                                  \
    if (isBson) {                                      \
        if (buf.getbson(fp.self_or(stdin)) < 0) break; \
    } else {                                           \
        if (buf.getline(fp.self_or(stdin)) < 0) break; \
		buf.chomp();                                   \
    }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if (mem_key) {
        fstrvecl svec;
        {
        	LineBuf buf;
	        while (true) {
                read_onekey();
                svec.push_back(buf);
	        }
            svec.shrink_to_fit();
        }
        t0 = pf.now();
		for(size_t j = 0; j < benchmarkLoop; ++j) {
            for(size_t i = 0; i < svec.size(); ++i) {
                fstring key = svec[i];
			    size_t idx = dawg->index(key);
			    if (idx < numWords) {
				    bytesFound += key.size();
				    keysFound++;
			    } else {
				    bytesMiss += key.size();
				    keysMiss++;
				    if (0 == j && verbose)
					    printf("NotFound: %.*s\n", key.ilen(), key.p);
			    }
            }
		}
		bytesTotal = svec.strpool.size();
		lines = svec.size();
    }
    else {
    	LineBuf buf;
	    while (true) {
            read_onekey();
		    for(size_t j = 0; j < benchmarkLoop; ++j) {
			    size_t idx = dawg->index(buf);
			    if (idx < numWords) {
				    bytesFound += buf.size();
				    keysFound++;
			    } else {
				    bytesMiss += buf.size();
				    keysMiss++;
				    if (0 == j && verbose)
					    printf("NotFound: %s\n", buf.p);
			    }
		    }
		    bytesTotal += buf.size();
		    lines++;
	    }
    }
	long long t1 = pf.now();
	fprintf(stderr, "Time  : %f'seconds   lines = %zd   bytes = %zd\n", pf.sf(t0,t1), lines, bytesTotal);
	fprintf(stderr, "Found : Count = %10zd   QPS = %8.3f'K/sec   ThroughPut: %8.4f'MB/sec\n", keysFound, keysFound/pf.mf(t0,t1), bytesFound/pf.uf(t0,t1));
	fprintf(stderr, "Miss  : Count = %10zd   QPS = %8.3f'K/sec   ThroughPut: %8.4f'MB/sec\n", keysMiss , keysMiss /pf.mf(t0,t1), bytesMiss /pf.uf(t0,t1));

	return 0;
}
