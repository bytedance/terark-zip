#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif

#include <terark/zbs/nest_louds_trie_blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/mixed_len_blob_store.hpp>
#include <terark/zbs/plain_blob_store.hpp>
#include <terark/zbs/zip_offset_blob_store.hpp>
#include <terark/zbs/entropy_zip_blob_store.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
#include <terark/entropy/entropy_base.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/io/FileStream.hpp>
#include <getopt.h>
#include <fcntl.h>
#include <random>
#include <terark/util/stat.hpp>
#include <terark/histogram.hpp>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr,
R"EOS(Usage: %s Options Input-TXT-File
Options:
  -h Show this help information
  -c checksumLevel: default 1, can be 0, 1, 2, 3
     0: checksum disabled
     1: checksum file header
     2: checksum each record(needs extra 4 bytes per record)
     3: checksum zip data area, and do not checksum each record
  -t checksumType: default 0(CRC32C), can be 0, 1(CRC16C)
  -C Check for correctness
  -d Use Dawg String Pool
  -e EntropyAlgo: Use EntropyAlgo for entropy zip, default none
     h: huffman
     f: FSE (Finite State Entropy)
  -n Nest Level
  -r Random get benchmark
  -o Output-Trie-File
  -g Output-Graphviz-Dot-File
  -b BenchmarkLoop : Run benchmark
  -B Input is binary(bson) data
  -S FloatNumber : Sample ratio of dictionary compression, usually < 0.1, or --
     FloatNumber@<file> : sample from <file>, with sample ratio, >= 1 means dict:<file>
     dict:<file> : <file> is the predefined sample dictionary
     Default is 0.03, <= 0 indicate DO NOT use PA-Zip
  -L local_match_opt when using dictionary compression
     h: Local Match by hashing, this is the default
     s: Local Match by suffix array
  -U [optional(0 or 1)] use new Ultra ref encoding, default 1
  -Z compress global dictionary
  -E embedded global dictionary
  -T a: auto select     DictZipBlobStore or NestLoudsTrieBlobStore, default
     d: force use       DictZipBlobStore
     n: force use NestLoudsTrieBlobStore, compatible to BaseDFA
     m: force use      MixedLenBlobStore
     p: force use         PlainBlobStore
     o: force use     ZipOffsetBlobStore
     e: force use    EntropyZipBlobStore
  -R integer: test reorder times
  -j [BlockUnits of Zipped Offset Array]
     This option is only for DictZipBlobStore and ZipOffsetBlobStore.
     This option takes an optional argument, must be one of {0,64,128}.
     Default argument is 128.
     0 to disable offset array compression.
  -V verify zbs
  -p print progress

If Input-TXT-File is omitted, use stdin
Note:
   If -S SampleRatio is specified, Input-TXT-File must be a regular file,
   because in this case DictZipBlobStore need to read input file two passes,
   stdin can be redirected from a regular file, but CAN NOT pass through pipes,
   such as `cat Input-TXT-File | ...`

)EOS", prog);
	exit(1);
}

static terark::profiling pf;
static int g_print_progress = 0;
static long long g_speed_bytes = 0;
static long long g_speed_time = -1;
static long long g_last_time = -1;
static long long g_dot_num = 0;
static long long g_rec_bytes_last = 0;
static long long g_rec_bytes = 0;

bool readoneRecord(FILE* fp, LineBuf* rec, size_t recno, bool isBson) {
	if (isBson) {
		int32_t reclen;
		if (fread(&reclen, 1, 4, fp) != 4) {
			return false;
		}
		if (reclen < 4) {
			fprintf(stderr
				, "read binary data error: nth=%zd reclen=%ld too small\n"
				, recno, long(reclen));
			return false;
		}
		if (rec->capacity < size_t(reclen-4+1)) {
			size_t cap = std::max<size_t>(reclen-4+1, 2 * rec->capacity);
			char* q = (char*)realloc(rec->p, cap);
			if (NULL == q) {
				fprintf(stderr
					, "readoneRecord: realloc failed: reclen-4 = %ld, cap = %zd"
					, long(reclen-4), cap);
				exit(3);
			}
			rec->p = q;
			rec->capacity = cap;
		}
		rec->n = reclen-4;
		size_t datalen = fread(rec->p, 1, reclen-4, fp);
		rec->p[reclen-4] = '\0'; // append an extra '\0'
		if (datalen != size_t(reclen-4)) {
			fprintf(stderr
				, "read binary data error: nth=%zd requested=%ld returned=%zd\n"
				, recno, long(reclen-4), datalen);
			return false;
		}
	}
	else {
		if (rec->getline(fp) > 0) {
			rec->chomp();
		} else {
			return false;
		}
	}
	g_rec_bytes += rec->size();
    if (g_print_progress) {
        if (terark_unlikely(-1 == g_last_time)) {
            g_dot_num = 0;
            g_rec_bytes = 0;
            g_speed_bytes = 0;
            g_rec_bytes_last = 0;
            g_speed_time = g_last_time = pf.now();
        }
        if (g_rec_bytes - g_rec_bytes_last > 8192) { // pf.now() is slow
            long long curr_time = pf.now();
            double us = pf.uf(g_last_time, curr_time);
            if (us > 2e5) {
                g_last_time = curr_time;
                g_dot_num++;
                if (g_dot_num % 32 != 0)
                    printf("."), fflush(stdout);
                else {
                    long long bytes = g_rec_bytes - g_speed_bytes;
                    us = pf.uf(g_speed_time, curr_time);
                    g_speed_time = curr_time;
                    g_speed_bytes = g_rec_bytes;
                    if (g_rec_bytes < 1e9)
                        printf("%8.3f MB/s, total %7.3f MB, records %8.3f K\n", bytes / us, g_rec_bytes / 1e6, recno / 1e3);
                    else if (g_rec_bytes < 1e12)
                        printf("%8.3f MB/s, total %7.3f GB, records %8.3f M\n", bytes / us, g_rec_bytes / 1e9, recno / 1e6);
                    else
                        printf("%8.3f MB/s, total %7.3f TB, records %8.3f G\n", bytes / us, g_rec_bytes / 1e12, recno / 1e9);
                }
            }
            g_rec_bytes_last = g_rec_bytes;
        }
	}
	return true;
}

int main(int argc, char* argv[])
TERARK_IF_DEBUG(,try) {
	size_t benchmarkLoop = 0;
	int  checksumLevel = 1;
	int  checksumType = 0;
	bool checkForCorrect = false;
	bool b_write_dot_file = false;
	bool isBson = false;
    bool verify = false;
	char local_match_opt = 'h';
	char entropy_algo = '?'; // NO entropy
    char select_store = 'a';
    int reorder_test = 0;
	bool randomUnzipBench = false;
	const char* nlt_fname = NULL;
	const char* sampleFile = NULL;
	double dictZipSampleRatio = 0.03;
	LineBuf rec, preSample;
	DictZipBlobStore::Options dzopt;
	NestLoudsTrieConfig conf;
//	conf.saFragMinFreq = 2;
//	conf.bzMinLen = 8;
	conf.flags.set0(conf.optUseDawgStrPool);
	conf.initFromEnv();
	for (;;) {
		int opt = getopt(argc, argv, "Bb:c:t:Ce:ghdn:o:M:F:S:L:rU::ZET:R:j::pV");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'B':
			isBson = true;
			break;
		case 'b':
			benchmarkLoop = atoi(optarg);
			break;
		case 'c':
			checksumLevel = atoi(optarg);
			checksumLevel = std::min(3, checksumLevel);
			checksumLevel = std::max(0, checksumLevel);
			break;
        case 't':
            checksumType = atoi(optarg);
            checksumType = std::min(1, checksumType);
            checksumType = std::max(0, checksumType);
            break;
		case 'C':
			checkForCorrect = true;
			break;
		case 'd':
			conf.flags.set1(conf.optUseDawgStrPool);
			break;
		case 'e':
			entropy_algo = optarg[0];
			break;
		case 'M':
			conf.maxFragLen = atoi(optarg);
			break;
		case 'n':
			conf.nestLevel = atoi(optarg);
			break;
		case 'F':
			conf.saFragMinFreq = atoi(optarg);
			break;
		case 'g':
			b_write_dot_file = true;
			break;
		case 'o':
			nlt_fname = optarg;
			break;
		case 'S':
		{
			const char* dictFile = NULL;
			if (fstring(optarg).startsWith("dict:")) {
				dictFile = optarg + 5;
		PredefineDict:
				ullong fsize = FileStream(dictFile, "rb").fsize();
				if (fsize < 100) {
					fprintf(stderr
						, "ERROR: prefdefined dictionary: %s is too small\n"
						, dictFile);
					return 1;
				}
				if (fsize > INT32_MAX) {
					fprintf(stderr
						, "ERROR: prefdefined dictionary: %s is too large\n"
						, dictFile);
					return 1;
				}
				preSample.read_all(dictFile);
				dictZipSampleRatio = 0;
			}
			else {
				char* endptr = NULL;
				dictZipSampleRatio = strtof(optarg, &endptr);
				if ('@' == *endptr) {
					if (dictZipSampleRatio >= 1) {
						dictFile = endptr + 1;
						goto PredefineDict;
					}
					sampleFile = endptr + 1;
					if (dictZipSampleRatio <= 0) {
						fprintf(stderr
							, "ERROR: invalid SampleRatio=%f@filename=%s\n"
							, dictZipSampleRatio, sampleFile);
						return 1;
					}
				}
			}
			break;
		}
		case 'L':
			local_match_opt = optarg[0];
			if (strchr("hs", local_match_opt) == NULL) {
				fprintf(stderr, "-L local_match_opt must be 'h' or 's'\n");
				usage(argv[0]);
			}
			break;
		case 'r':
			randomUnzipBench = true;
			benchmarkLoop = std::max<size_t>(1, benchmarkLoop);
			break;
		case 'U':
			if (optarg && '0' == optarg[0]) {
				dzopt.useNewRefEncoding = false;
			} else {
				dzopt.useNewRefEncoding = true;
			}
			break;
		case 'Z':
			dzopt.compressGlobalDict = true;
			break;
        case 'E':
            dzopt.embeddedDict = true;
            break;
        case 'T':
            select_store = optarg[0];
            if (strchr("adnmpoe", select_store) == NULL) {
                fprintf(stderr, "ERROR: -T store type must be 'a', 'd', 'n', 'm', 'p', 'o' or 'e'\n\n");
                usage(argv[0]);
            }
            break;
        case 'R':
            reorder_test = atoi(optarg);
            break;
        case 'V':
            verify = true;
            break;
		case 'j':
			if (optarg) {
				dzopt.offsetArrayBlockUnits = atoi(optarg);
				if (true
					&&   0 != dzopt.offsetArrayBlockUnits // disable compression
					&&  64 != dzopt.offsetArrayBlockUnits
					&& 128 != dzopt.offsetArrayBlockUnits
				   ) {
					fprintf(stderr,
						"ERROR: Argument for -U must be 64 or 128, (default is 128)\n\n");
					usage(argv[0]);
				}
			}
			else {
				dzopt.offsetArrayBlockUnits = 128;
			}
			break;
		case 'p':
			g_print_progress = 1;
			break;
        case '?':
        case 'h':
        default:
            usage(argv[0]);
        }
    }
GetoptDone:
	if (NULL == nlt_fname) {
		fprintf(stderr, "-o Output-Trie-File is required\n\n");
		usage(argv[0]);
	}
	if (select_store == 'd' && dictZipSampleRatio <= 0 && preSample.empty()) {
		fprintf(stderr, "force use DictZipBlobStore, but bad -S option\n\n");
		return 1;
	}
	if (dictZipSampleRatio > 0 && !preSample.empty()) {
		fprintf(stderr, "At most one of -S dict:dict_file or -S sampleRatio, not both\n\n");
		usage(argv[0]);
	}
	const char* input_fname = argv[optind];
	Auto_fclose afp;
	if (input_fname) {
#ifdef _MSC_VER
		afp = fopen(input_fname, "rb");
#else
		afp = fopen(input_fname, isBson ? "rb" : "r");
#endif
		if (NULL == afp) {
			fprintf(stderr, "FATAL: fopen(\"%s\", \"r\") = %s\n", input_fname, strerror(errno));
			return 1;
		}
	}
	else {
		fprintf(stderr, "Reading from stdin...\n");
	}
	FILE* fp = afp.self_or(stdin);
	long long inputFileSize = 0;
	{
		struct ll_stat st;
		int err = ::ll_fstat(fileno(fp), &st);
		if (err) {
			fprintf(stderr, "ERROR: fstat failed = %s\n", strerror(errno));
			return 1;
		}
		if (!S_ISREG(st.st_mode) && dictZipSampleRatio > 0 && !sampleFile) {
			fprintf(stderr, "ERROR: input must be a regular file\n");
			return 1;
		}
		inputFileSize = st.st_size; // compute one by one
	}
    std::unique_ptr<freq_hist_o1> freq;
	SortableStrVec strVec;
	std::unique_ptr<AbstractBlobStore> store;
	std::unique_ptr<DictZipBlobStore::ZipBuilder> dzb;
    Uint32Histogram histogram;
	std::mt19937_64 randomGen;
	uint64_t randomUpperBound = 0;
	dzopt.checksumLevel = checksumLevel;
	dzopt.useSuffixArrayLocalMatch = 's' == local_match_opt;
	dzopt.entropyAlgo
		= 'h' == entropy_algo ? dzopt.kHuffmanO1
		: 'f' == entropy_algo ? dzopt.kFSE
		: dzopt.kNoEntropy;
    if (select_store == 'a' || select_store == 'd') {
        if (dictZipSampleRatio > 0) {
            dzb.reset(DictZipBlobStore::createZipBuilder(dzopt));
            randomUpperBound = uint64_t(randomGen.max() * dictZipSampleRatio);
        }
        else if (!preSample.empty()) {
            dzb.reset(DictZipBlobStore::createZipBuilder(dzopt));
            dzb->addSample(preSample);
            preSample.clear();
            dzb->finishSample();
            dzb->prepare(0, nlt_fname);
        }
    }
	size_t allstrlen = 0;
	size_t allstrnum = 0;
	long long t0 = pf.now();
    if (select_store == 'e') {
        freq.reset(new freq_hist_o1);
        size_t recno = 0;
        for (; readoneRecord(fp, &rec, recno, isBson); recno++) {
            freq->add_record(rec);
			strVec.push_back(rec);
			allstrlen += rec.size();
			allstrnum += 1;
        }
        freq->finish();
    }
	if (sampleFile) {
		Auto_fclose sfp(fopen(sampleFile, isBson ? "rb" : "r"));
		if (!sfp) {
			fprintf(stderr, "ERROR: fopen(%s) = %s\n", sampleFile, strerror(errno));
			return 1;
		}
		while (readoneRecord(sfp, &rec, allstrnum, isBson)) {
			if (randomGen() < randomUpperBound) {
				dzb->addSample(rec);
			}
			allstrlen += rec.size();
			allstrnum += 1;
		}
	}
	else {
		while (readoneRecord(fp, &rec, allstrnum, isBson)) {
			if (dzb) {
				if (dictZipSampleRatio > 0) {
					if (randomGen() < randomUpperBound) {
						dzb->addSample(rec);
					}
				}
				else {
					dzb->addRecord(rec);
				}
				if (checkForCorrect) {
					strVec.push_back(rec);
				}
			}
			else {
				strVec.push_back(rec);
                ++histogram[rec.size()];
			}
			allstrlen += rec.size();
			allstrnum += 1;
		}
	}
    histogram.finish();
	if (0 == allstrnum) {
		fprintf(stderr, "Input is empty\n");
		return 1;
	}
	if (0 == inputFileSize) { // not a regular file
		inputFileSize = isBson
						? allstrlen + 4*allstrnum
						: allstrlen + 1*allstrnum;
	}
	if (g_print_progress) {
		g_speed_time = g_last_time = -1;
		printf("\n");
	}
	long long t1 = pf.now();
	NestLoudsTrieBlobStore_SE_512* dfa = nullptr;
	if (dzb) {
		if (dictZipSampleRatio > 0) {
			fprintf(stderr, "DictZip: sample completed, start compressing...\n");
			dzb->finishSample();
			dzb->prepare(allstrnum, nlt_fname);
			size_t recno = 0;
			::rewind(fp);
			for (; readoneRecord(fp, &rec, recno, isBson); recno++) {
				dzb->addRecord(rec);
			}
		}
        dzb->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict
            | DictZipBlobStore::ZipBuilder::FinishWriteDictFile);
        store.reset(AbstractBlobStore::load_from_mmap(nlt_fname, false));
    }
    else if (select_store == 'm') {
        size_t fixedLen = histogram.m_max_cnt_key;
        size_t varLenSize = histogram.m_total_key_len - histogram.m_cnt_of_max_cnt_key * fixedLen;
		size_t varLenCnt = histogram.m_cnt_sum - histogram.m_cnt_of_max_cnt_key;
        MixedLenBlobStore::MyBuilder mlbuilder(fixedLen, varLenSize, varLenCnt, nlt_fname, 0, checksumLevel, checksumType);
        for (size_t i = 0, ei = strVec.size(); i < ei; ++i) {
            mlbuilder.addRecord(strVec[i]);
        }
        mlbuilder.finish();
        store.reset(AbstractBlobStore::load_from_mmap(nlt_fname, false));
    }
    else if (select_store == 'p') {
        PlainBlobStore::MyBuilder pbuilder(strVec.str_size(), strVec.size(), nlt_fname, 0, checksumLevel, checksumType);
        for (size_t i = 0, ei = strVec.size(); i < ei; ++i) {
            pbuilder.addRecord(strVec[i]);
        }
        pbuilder.finish();
        store.reset(AbstractBlobStore::load_from_mmap(nlt_fname, false));
    }
    else if (select_store == 'o') {
        ZipOffsetBlobStore::MyBuilder zobuilder(dzopt.offsetArrayBlockUnits, nlt_fname, 0, checksumLevel, checksumType);
        for (size_t i = 0, ei = strVec.size(); i < ei; ++i) {
            zobuilder.addRecord(strVec[i]);
        }
        zobuilder.finish();
        store.reset(AbstractBlobStore::load_from_mmap(nlt_fname, false));
    }
    else if (select_store == 'e') {
        EntropyZipBlobStore::MyBuilder ezbuilder(*freq.get(), dzopt.offsetArrayBlockUnits, nlt_fname, 0, checksumLevel, checksumType);
        for (size_t i = 0, ei = strVec.size(); i < ei; ++i) {
            ezbuilder.addRecord(strVec[i]);
        }
        ezbuilder.finish();
        store.reset(AbstractBlobStore::load_from_mmap(nlt_fname, false));
    }
	else {
		dfa = new NestLoudsTrieBlobStore_SE_512();
		store.reset(dfa);
		dfa->build_from(strVec, conf);
		dfa->set_is_dag(true);
	}
	long long t2 = pf.now();
	if (b_write_dot_file && dfa) {
		dfa->write_dot_file((std::string(input_fname) + ".dot").c_str());
	}
	long long t3 = pf.now();
	long long zipFileSize = 0;
	DictZipBlobStore::ZipStat zstat;
	if (dzb) {
	// don't need to save, files have saved when dzb->finish()
	//	static_cast<DictZipBlobStore&>(*store).save_mmap(nlt_fname);
		struct ll_stat st;
		::ll_stat((fstring(nlt_fname) + "-dict").c_str(), &st);
		zipFileSize += st.st_size;
		zstat = dzb->getZipStat();
		dzb.reset();
	}
	if (dfa) {
		store->save_mmap(nlt_fname);
	}
	{
		struct ll_stat st;
		::ll_stat(nlt_fname, &st);
		zipFileSize += st.st_size;
	}
	long long t4 = pf.now();

    if (verify) {
        for (size_t i = 0; i < strVec.size(); ++i) {
            if (store->get_record(i) != strVec[i]) {
                fprintf(stderr, "build mismatch at %zd\n", i);
                exit(-1);
            }
        }
    }

	if (benchmarkLoop) {
		if (g_print_progress) {
			printf("compressing finished, benchmarking...\n");
		}
		long long t5 = t4;
		if (randomUnzipBench) {
			valvec<uint32_t> idvec(allstrnum, valvec_no_init());
			for (size_t i = 0; i < idvec.size(); ++i) idvec[i] = uint32_t(i);
			std::mt19937_64 random;
			std::shuffle(idvec.begin(), idvec.end(), random);
			t5 = pf.now();
			valvec<byte_t> recData;
			for (size_t k = 0; k < benchmarkLoop; ++k) {
				for(size_t i = 0; i < idvec.size(); ++i) {
					if (g_print_progress && i % (1ull << 19) == 0) {
						printf("."); fflush(stdout);
					}
					store->get_record(idvec[i], &recData);
				}
			}
		}
		else if (checkForCorrect) {
			valvec<byte_t> recData;
			for (size_t i = 0; i < allstrnum; ++i) {
				if (g_print_progress && i % (1ull << 19) == 0) {
					printf("."); fflush(stdout);
				}
				store->get_record(i, &recData);
				fstring saved = strVec[i];
				if (saved != recData) {
					fprintf(stderr, "%08zd: not correct\n", i);
				}
			}
		}
		else {
			valvec<byte_t> recData;
			for (size_t k = 0; k < benchmarkLoop; ++k) {
				for (size_t i = 0; i < allstrnum; ++i) {
					if (g_print_progress && i % (1ull << 19) == 0) {
						printf("."); fflush(stdout);
					}
					store->get_record(i, &recData);
				}
			}
		}
		long long t6 = pf.now();
		bool write_html_table = false;
		if (const char* env = getenv("write_html_table")) {
			write_html_table = atoi(env) ? true : false;
		}
		fprintf(stderr, "\n");
		if (write_html_table) {
			fprintf(stderr,
				"<table><tbody>\n"
				"<tr>"
					"<th></th>"
					"<th>Time</th>"
					"<th>mQPS</th>"
					"<th>ns/Q</th>"
					"<th>MB/s</th>"
				"</tr>\n"
				);
			fprintf(stderr,
				"<tr>"
					"<th align='left'>Read text file</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t0, t1)
				, allstrnum / pf.uf(t0, t1)
				, pf.nf(t0, t1) / allstrnum
				, allstrlen / pf.uf(t0, t1)
				);
			fprintf(stderr,
				"<tr>"
					"<th align='left'>Compress  data</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t1, t2)
				, allstrnum / pf.uf(t1, t2)
				, pf.nf(t1, t2) / allstrnum
				, allstrlen / pf.uf(t1, t2)
				);
		  if (!dynamic_cast<DictZipBlobStore*>(&*store)) {
			fprintf(stderr,
				"<tr>"
					"<th align='left'>Save mmap dfa</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t3, t4)
				, allstrnum / pf.uf(t3, t4)
				, pf.nf(t3, t4) / allstrnum
				, allstrlen / pf.uf(t3, t4)
				);
		  }
			fprintf(stderr,
				"<tr>"
					"<th align='left'>extract/unzip</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t5, t6)
				, (allstrnum * benchmarkLoop) / pf.mf(t5, t6)
				, pf.nf(t5, t6) / (allstrnum * benchmarkLoop)
				, (allstrlen * benchmarkLoop) / pf.uf(t5, t6)
				);
			fprintf(stderr, "</tbody></table>\n");
		}
		else {
			fprintf(stderr
				, "Read text file:  Time:%8.3f     QPS:%8.1f K      %8.1f ns/Q     %9.3f MB/s\n"
				, pf.sf(t0, t1)
				, allstrnum / pf.mf(t0, t1)
				, pf.nf(t0, t1) / allstrnum
				, allstrlen / pf.uf(t0, t1)
				);
			fprintf(stderr
				, "Compress  data:  Time:%8.3f     QPS:%8.1f K      %8.1f ns/Q     %9.3f MB/s\n"
				, pf.sf(t1, t2)
				, allstrnum / pf.mf(t1, t2)
				, pf.nf(t1, t2) / allstrnum
				, allstrlen / pf.uf(t1, t2)
				);
		  if (!dynamic_cast<DictZipBlobStore*>(&*store)) {
			fprintf(stderr
				, "Save mmap dfa :  Time:%8.3f     QPS:%8.1f K      %8.1f ns/Q     %9.3f MB/s\n"
				, pf.sf(t3, t4)
				, allstrnum / pf.mf(t3, t4)
				, pf.nf(t3, t4) / allstrnum
				, allstrlen / pf.uf(t3, t4)
				);
		  }
			fprintf(stderr
				, "extract/unzip :  Time:%8.3f     QPS:%8.1f K      %8.1f ns/Q     %9.3f MB/s\n"
				, pf.sf(t5, t6)
				, (allstrnum * benchmarkLoop) / pf.mf(t5, t6)
				, pf.nf(t5, t6) / (allstrnum * benchmarkLoop)
				, (allstrlen * benchmarkLoop) / pf.uf(t5, t6)
				);
		}
		fprintf(stderr
			, "CompressRatio : input:%11lld  zip: %10lld   input/zip: %5.3f   zip/input: %5.3f\n"
			, inputFileSize, zipFileSize
			, 1.0*inputFileSize/zipFileSize
			, 1.0*zipFileSize/inputFileSize
			);
	}
    if (dynamic_cast<DictZipBlobStore*>(&*store)) zstat.print(stderr);
    if (reorder_test) {
        UintVecMin0 ids(store->num_records(), store->num_records());
        std::mt19937 mt;
        for (size_t i = 0, e = store->num_records(); i < e; ++i)
            ids.set_wire(i, i);
        std::string reorder_name = nlt_fname + std::string(".reorder");
        if (!dzopt.embeddedDict && dynamic_cast<DictZipBlobStore*>(&*store)) {
            FileStream(nlt_fname + std::string(".reorder-dict"), "wb").cat(nlt_fname + std::string("-dict"));
        }
        for (int reorder_test_i = 0; reorder_test_i < reorder_test; ++reorder_test_i) {
            std::string reorder_map = nlt_fname + std::string(".reorder-map");
            ZReorderMap::Builder builder(ids.size(), 1, reorder_map, "wb");
            if (ids.size() >= 2) {
                for (size_t i = 0, e = ids.size() - 1; i != e; ++i) {
                    size_t off = std::uniform_int_distribution<size_t>(i, e)(mt);
                    size_t val = ids[off];
                    ids.set_wire(off, ids[i]);
                    ids.set_wire(i, val);
                    builder.push_back(val);
                }
            }
            builder.push_back(ids.back());
            builder.finish();
            ZReorderMap reorder(reorder_map);
            FileStream fp(reorder_name, "wb");
            store->reorder_zip_data(reorder, [&](const void*d, size_t l) {
                fp.ensureWrite(d, l);
            }, nlt_fname + std::string(".reorder-tmp"));
            fp.close();
            std::unique_ptr<BlobStore> r(BlobStore::load_from_mmap(reorder_name, false));
            valvec<byte_t> v1, v2;
            for (size_t i = 0; i < store->num_records(); ++i) {
                store->get_record_append(ids[i], &v1);
                r->get_record_append(i, &v2);
                if (v1 != v2) {
                    fprintf(stderr, "reorder mismatch at %zd -> %zd\n", i, size_t(ids[i]));
                    exit(-1);
                }
                v1.risk_set_size(0);
                v2.risk_set_size(0);
            }
            fprintf(stderr, "reorder test ok (%d / %d)\n", reorder_test_i + 1, reorder_test);
        }
    }
	return 0;
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
	fprintf(stderr, "ERROR: Exception: %s\n", ex.what());
	exit(1);
}
#endif
