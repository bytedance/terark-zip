#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif

#include <terark/fsa/nest_louds_trie.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/base64.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <getopt.h>
#include <fcntl.h>
#include <terark/util/mmap.hpp>
#include <terark/util/stat.hpp>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr, R"EOS(Usage: %s Options Input-TXT-File
Options:
    -h Show this help information
    -M maxFragLen
    -m mmap input file
    -n Nest Level
    -o Output-Trie-File
    -g Output-Graphviz-Dot-File
    -b BenchmarkLoop : Run benchmark
    -s indicate that input is sorted, the top level sort will be omitted
    -B Input is binary(bson) data
    -6 Input is base64 encoded data
    -p CommonPrefix
    -U StrVecType, can be one of:
         t: SortThinStrVec, this is the default
         x: SortableStrVec,
         s: VoSortedStrVec, -s must also be specified, for double check
         z: ZoSortedStrVec, -s must also be specified, for double check
         f: FixedLenStrVec
         d: DoSortedStrVec, mmap is used for string content
		      -s must also be specified, for double check
         q: QoSortedStrVec, mmap is used for string content
		      -s must also be specified, for double check
         +--------------+---------+---------+---------------+---------+
         |              |Mem Usage|VarKeyLen|CanBe UnSorted?|With Mmap|
		 +--------------+---------+---------+---------------+---------+
         |SortThinStrVec|  High   |   Yes   |     Yes       | Trim LF |
         |SortableStrVec|  High   |   Yes   |     Yes       | Trim LF |
         |VoSortedStrVec| Medium  |   Yes   |Must Be Sorted | Keep LF |
         |ZoSortedStrVec|  Low    |   Yes   |Must Be Sorted | Keep LF |
         |FixedLenStrVec|  Lowest |   No    |     Yes       | Keep LF |
         |DoSortedStrVec|  Lowest |   Yes   |     Yes       | Trim LF |
         |QoSortedStrVec|  Lowest |   Yes   |     Yes       | Trim LF |
         +--------------+---------+---------+---------------+---------+
         * ZoSortedStrVec is slower than SortedStrVec(20%% ~ 40%% slower).
           When using ZoSortedStrVec, you should also use -T 4@/path/to/tmpdir,
           otherwise warning will be issued.
         * When using mmap input, there are more notes:
           Keep LF: the LF char('\n') of each line is kept   in the output NLT
           Trim LF: the LF char('\n') of each line is trimed in the output NLT
    -T TmpDir, if specified, will use less memory
       TmpLevel@TmpDir, TmpLevel is 0-9
    -F bool(0 or 1), default = 1
       Use FastLabel, this will greatly improve search performance and
       reduce a little compression ratio
    -R RankSelect implementation, can be:
         se-256
         se-512
         il-256
         m-se-512
         m-il-256
         m-xl-256, this is the default
         m-il-256-41
         m-xl-256-41
    -w File
       Write words to File in NLT order
       Because NLT order is not dictionary/bytewise/strcmp/sort order.
If Input-TXT-File is omitted, use stdin
)EOS", prog);
	exit(1);
}

size_t benchmarkLoop = 0;
bool b_write_dot_file = false;
bool is_binary_input = false;
bool is_base64_input = false;
bool mmap_input_file = false;
const char* rlouds_trie_fname = NULL;
const char* bench_input_fname = NULL;
const char* nlt_order_fname = NULL;
auchar_t kv_delim = '\t';
unsigned char strVecTypeChar = 't';
NestLoudsTrieConfig conf;

template<class NestTrieDAWG>
int build(int argc, char* argv[]);

int main(int argc, char* argv[]) {
	bool useFastLabel = true;
	conf.initFromEnv();
	const char* rank_select_impl = "m-xl-256";
	for (;;) {
		int opt = getopt(argc, argv, "Bb:w:ghd:M:mn:o:R:T:U:s6F:p:");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'B':
			is_binary_input = true;
			break;
		case 'b':
			{
				char* endpos = NULL;
				benchmarkLoop = strtoul(optarg, &endpos, 10);
				if ('@' == *endpos) {
					bench_input_fname = endpos + 1;
				}
				break;
			}
		case '6':
			is_base64_input = true;
			break;
		case 'd':
			kv_delim = optarg[0];
			break;
		case 'M':
			conf.maxFragLen = atoi(optarg);
			break;
		case 'm':
			mmap_input_file = true;
			break;
		case 'n':
			conf.nestLevel = atoi(optarg);
			break;
		case 'g':
			b_write_dot_file = true;
			break;
		case 'o':
			rlouds_trie_fname = optarg;
			break;
        case 'p':
            conf.commonPrefix = optarg;
            break;
		case 'R':
			rank_select_impl = optarg;
			break;
		case 'F':
			useFastLabel = !!atoi(optarg);
			break;
        case 'w':
            nlt_order_fname = optarg;
            break;
		case 'T':
			if (isdigit(optarg[0]) && '@' == optarg[1]) {
				conf.tmpLevel = optarg[0] - '0';
				conf.tmpDir = optarg + 2;
			}
			else {
				conf.tmpDir = optarg;
			}
			break;
		case 's':
			conf.isInputSorted = true;
			break;
        case 'U':
            strVecTypeChar = optarg[0];
            if (strchr("zxsftdq", strVecTypeChar) == NULL) {
                fprintf(stderr, "ERROR: invalid option: -U %c\n", strVecTypeChar);
                usage(argv[0]);
            }
            break;
		case '?':
		case 'h':
		default:
			usage(argv[0]);
		}
	}
GetoptDone:
    if (strchr("zsdq", strVecTypeChar) && !conf.isInputSorted) {
        fprintf(stderr,
            "ERROR: When using option (-U s) or (-U z), option (-s) is required\n");
        usage(argv[0]);
    }
    if ('z' == strVecTypeChar && (conf.tmpDir.empty() || conf.tmpLevel < 4)) {
        fprintf(stderr,
            "WARN: When using option (-U z), it should be low memory, option (-T 4@/path/to/tmpdir) should also be used\n");
    }
	if (useFastLabel) {
		if (strcasecmp(rank_select_impl, "se-256") == 0)
			return build<NestLoudsTrieDAWG_SE_256_32_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "se-512") == 0)
			return build<NestLoudsTrieDAWG_SE_512_32_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "se-512-64") == 0)
			return build<NestLoudsTrieDAWG_SE_512_64_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "il-256") == 0)
			return build<NestLoudsTrieDAWG_IL_256_32_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-se-512") == 0)
			return build<NestLoudsTrieDAWG_Mixed_SE_512_32_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-il-256") == 0)
			return build<NestLoudsTrieDAWG_Mixed_IL_256_32_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-xl-256") == 0)
			return build<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>(argc, argv);
		/*
		if (strcasecmp(rank_select_impl, "m-il-256-41") == 0)
			return build<NestLoudsTrieDAWG_Mixed_IL_256_32_41_FL>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-xl-256-41") == 0)
			return build<NestLoudsTrieDAWG_Mixed_XL_256_32_41_FL>(argc, argv);
		*/
	}
	else {
		if (strcasecmp(rank_select_impl, "se-256") == 0)
			return build<NestLoudsTrieDAWG_SE_256>(argc, argv);
		if (strcasecmp(rank_select_impl, "se-512") == 0)
			return build<NestLoudsTrieDAWG_SE_512>(argc, argv);
		if (strcasecmp(rank_select_impl, "se-512-64") == 0)
			return build<NestLoudsTrieDAWG_SE_512_64>(argc, argv);
		if (strcasecmp(rank_select_impl, "il-256") == 0)
			return build<NestLoudsTrieDAWG_IL_256>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-se-512") == 0)
			return build<NestLoudsTrieDAWG_Mixed_SE_512>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-il-256") == 0)
			return build<NestLoudsTrieDAWG_Mixed_IL_256>(argc, argv);
		if (strcasecmp(rank_select_impl, "m-xl-256") == 0)
			return build<NestLoudsTrieDAWG_Mixed_XL_256>(argc, argv);
		/*
		if (strcasecmp(rank_select_impl, "m-il-256-41") == 0) {
            fprintf(stderr, "ERROR: -R m-il-256-41 must with -F1\n");
			return 1;
        }
        if (strcasecmp(rank_select_impl, "m-xl-256-41") == 0) {
            fprintf(stderr, "ERROR: -R m-xl-256-41 must with -F1\n");
			return 1;
        }
		*/
	}
	fprintf(stderr, "ERROR: invalid arg: -R %s\n", rank_select_impl);
	usage(argv[0]);
}

template<class NestTrieDAWG, class StrVecType>
int build_impl(int argc, char* argv[]);

template<class NestTrieDAWG>
int build(int argc, char* argv[]) {
    switch (strVecTypeChar) {
    default:
        fprintf(stderr, "ERROR: %s: bad strVecTypeChar = %c(0x%02X)\n"
            , BOOST_CURRENT_FUNCTION, strVecTypeChar, strVecTypeChar);
        usage(argv[0]);
        break;
    case 'z':
        return build_impl<NestTrieDAWG, ZoSortedStrVecWithBuilder>(argc, argv);
    case 'x':
        return build_impl<NestTrieDAWG, SortableStrVec>(argc, argv);
    case 's':
        return build_impl<NestTrieDAWG,   SortedStrVec>(argc, argv);
    case 'f':
        return build_impl<NestTrieDAWG, FixedLenStrVec>(argc, argv);
    case 't':
        return build_impl<NestTrieDAWG, SortThinStrVec>(argc, argv);
    case 'd':
        return build_impl<NestTrieDAWG, DoSortedStrVec>(argc, argv);
	case 'q':
		return build_impl<NestTrieDAWG, QoSortedStrVec>(argc, argv);
	}
    return 0; // should not goes here, for compiler warnings
}

void StrVec_sanitize(SortableStrVec& strVec, size_t keylen) {
    // do nothing
}
void StrVec_sanitize(SortThinStrVec& strVec, size_t keylen) {
	// do nothing
}
void StrVec_sanitize(FixedLenStrVec& strVec, size_t keylen) {
    if (0 == strVec.m_fixlen) {
        strVec.m_fixlen = keylen;
    }
    else if (keylen != strVec.m_fixlen) {
        fprintf(stderr, "ERROR: %s: keylen = %zd, must be %zd\n"
            , BOOST_CURRENT_FUNCTION, keylen, strVec.m_fixlen);
        exit(1);
    }
}
void StrVec_sanitize(SortedStrVec& strVec, size_t keylen) {
    if (strVec.m_offsets.uintbits() == 0) {
        size_t cap = strVec.m_strpool.capacity();
        if (cap > 0) {
            strVec.m_offsets.resize_with_wire_max_val(0, cap);
        }
        else {
            size_t uintbits = 40; // 1TB
            strVec.m_offsets.resize_with_uintbits(0, uintbits);
        }
    }
}
void StrVec_sanitize(ZoSortedStrVecWithBuilder& strVec, size_t keylen) {
    if (!strVec.has_builder()) {
        strVec.init(128);
    }
}
template<class U>
void StrVec_sanitize(SortedStrVecUintTpl<U>& strVec, size_t keylen) {
	// do nothing
}

static bool checkSortOrder(fstring curr) {
    static valvec<byte_t> prev;
    static size_t nth = 0;
    nth++;
    int cmp = fstring_func::compare3()(prev, curr);
    if (cmp > 0) {
        fprintf(stderr, "ERROR: option(-s) is on but nth = %zd (base 1) is less than prev\n", nth);
        exit(1);
    }
    else if (cmp < 0) { // prev < curr
        prev.assign(curr);
        return true;
    }
    return false;
}

template<class StrVecType>
long binary_read_onekey(StrVecType& strVec, FILE* fp, size_t keylen) {
    StrVec_sanitize(strVec, keylen);
    strVec.push_back("");
    strVec.back_grow_no_init(keylen);
    size_t len = fread(strVec.mutable_nth_data(strVec.size() - 1), 1, keylen, fp);
    if (len != keylen) {
        strVec.pop_back();
    }
    else if (conf.isInputSorted) {
        if (!checkSortOrder(strVec.back()))
            strVec.pop_back();
    }
    return long(len);
}

long binary_read_onekey(FixedLenStrVec& strVec, FILE* fp, size_t keylen) {
    StrVec_sanitize(strVec, keylen);
    auto keyPtr = strVec.m_strpool.grow_no_init(keylen);
    strVec.m_size++;
    size_t len = fread(keyPtr, 1, keylen, fp);
    if (len != keylen) {
        strVec.pop_back();
    }
    else if (conf.isInputSorted) {
        if (!checkSortOrder(fstring(keyPtr, keylen)))
            strVec.pop_back();
    }
    return long(len);
}

long binary_read_onekey(ZoSortedStrVecWithBuilder& strVec, FILE* fp, size_t keylen) {
    StrVec_sanitize(strVec, keylen);
    static valvec<byte_t> buf;
    buf.resize_no_init(keylen);
    size_t len = fread(buf.data(), 1, keylen, fp);
    if (len == keylen) {
        if (!conf.isInputSorted || checkSortOrder(buf))
            strVec.push_back(buf);
    }
    return long(len);
}

void StrVec_index_reserve(SortableStrVec& strVec, size_t num, size_t) {
	strVec.m_index.reserve(num);
}
void StrVec_index_reserve(SortThinStrVec& strVec, size_t num, size_t) {
	strVec.m_index.reserve(num);
}
void StrVec_index_reserve(SortedStrVec& strVec, size_t num, size_t filesize) {
	strVec.m_offsets.resize_with_wire_max_val(num, filesize);
	strVec.m_offsets.resize(0);
}
template<class U>
void StrVec_index_reserve(SortedStrVecUintTpl<U>& strVec, size_t num, size_t) {
	strVec.m_offsets.reserve(num);
	strVec.m_delim_len = 1; // for '\n'
}
void StrVec_index_reserve(FixedLenStrVec&, size_t, size_t) {} // do nothing

template<class StrVecType>
void StrVec_init_reserve(StrVecType& strVec, size_t filesize) {
    size_t assume_avg_len = 100;
	if (mmap_input_file) {
		strVec.m_strpool_mem_type = MemType::Mmap;
		StrVec_index_reserve(strVec, filesize / assume_avg_len, filesize);
	}
	else {
		strVec.reserve(filesize / assume_avg_len, filesize);
	}
}
void StrVec_init_reserve(ZoSortedStrVecWithBuilder& strVec, size_t filesize) {
    strVec.init(128);
	if (!mmap_input_file)
    	strVec.reserve(0, filesize);
}

void StrVec_index_push(SortableStrVec& strVec, size_t offset, size_t len) {
    strVec.m_index.push_back({offset, len, (uint32_t)strVec.m_index.size()});
}
void StrVec_index_push(SortThinStrVec& strVec, size_t offset, size_t len) {
    strVec.m_index.push_back({offset,len});
}
void StrVec_index_push(SortedStrVec& strVec, size_t offset, size_t len) {
    strVec.m_offsets.push_back(offset);
}
void StrVec_index_push(ZoSortedStrVecWithBuilder& strVec, size_t offset, size_t) {
	strVec.push_offset(offset);
}
void StrVec_index_push(FixedLenStrVec& strVec, size_t offset, size_t) {}
template<class U>
void StrVec_index_push(SortedStrVecUintTpl<U>& strVec, size_t offset, size_t) {
    strVec.m_offsets.push_back(offset);
}

void StrVec_index_finish(SortableStrVec& strVec, size_t offset) {
    //strVec.m_index.shrink_to_fit();
	strVec.m_strpool.risk_set_size(strVec.m_index.back().endpos());
}
void StrVec_index_finish(SortThinStrVec& strVec, size_t offset) {
    //strVec.m_index.shrink_to_fit();
	strVec.m_strpool.risk_set_size(strVec.m_index.back().endpos());
}
void StrVec_index_finish(SortedStrVec& strVec, size_t offset) {
    strVec.m_offsets.push_back(offset);
}
void StrVec_index_finish(ZoSortedStrVecWithBuilder& strVec, size_t offset) {
	strVec.push_offset(offset);
	strVec.finish();
}
void StrVec_index_finish(FixedLenStrVec& strVec, size_t offset) {}
template<class U>
void StrVec_index_finish(SortedStrVecUintTpl<U>& strVec, size_t offset) {
    strVec.m_offsets.push_back(offset);
}

template<class StrVec>
bool StrVec_needs_end_lf(StrVec&) { return true; }
template<class U>
bool StrVec_needs_end_lf(SortedStrVecUintTpl<U>&) { return false; }
bool StrVec_needs_end_lf(SortableStrVec&) { return false; }
bool StrVec_needs_end_lf(SortThinStrVec&) { return false; }

template<class NestTrieDAWG, class StrVecType>
int build_impl(int argc, char* argv[]) {
	if (NULL == rlouds_trie_fname) {
		fprintf(stderr, "-o Output-Trie-File is required\n\n");
		usage(argv[0]);
	}
	terark::profiling pf;
	const char* input_fname = argv[optind];
	Auto_fclose fp;
	if (input_fname) {
		fp = fopen(input_fname, is_binary_input ? "rb" : "r");
		if (NULL == fp) {
			fprintf(stderr, "FATAL: fopen(\"%s\", \"r\") = %s\n", input_fname, strerror(errno));
			return 1;
		}
	}
	else {
		fprintf(stderr, "Reading from stdin...\n");
	}
	StrVecType strVec;
	{
		struct ll_stat st;
		int err = ::ll_fstat(fileno(fp.self_or(stdin)), &st);
		if (err) {
			fprintf(stderr, "ERROR: fstat failed = %s\n", strerror(errno));
		}
		else if (S_ISREG(st.st_mode)) { // OK, get file size
			StrVec_init_reserve(strVec, st.st_size);
		}
		else if (mmap_input_file) {
			fprintf(stderr, "input file is not a regular file, can not mmap\n");
			exit(1);
		}
	}
//	fprintf(stderr, "is_base64_input = %d\n", is_base64_input);
//	fprintf(stderr, "is_binary_input = %d\n", is_binary_input);
	long long t0 = pf.now();
	if (is_binary_input) {
		size_t nth = 0;
		while (!feof(fp)) {
			int32_t reclen;
			if (fread(&reclen, 1, 4, fp) != 4) {
				break;
			}
			if (reclen <= 8) {
				fprintf(stderr, "read binary data error: nth=%zd reclen=%ld too small\n"
						, nth, long(reclen));
				break;
			}
			long datalen = binary_read_onekey(strVec, fp, reclen-4);
			if (datalen != reclen-4) {
				fprintf(stderr, "read binary data error: nth=%zd requested=%ld returned=%ld\n"
						, nth, long(reclen-4), datalen);
				break;
			}
			nth++;
		}
		strVec.finish();
	}
	else if (is_base64_input) {
		LineBuf line;
		valvec<char> key;
	//	valvec<char> enc;
		while (line.getline(fp.self_or(stdin)) > 0) {
			line.chomp();
			terark::base64_decode(line.p, line.n, &key);
		//	terark::base64_encode(key, &enc);
		//	size_t len = std::min(enc.size(), line.size());
		//	assert(memcmp(line.data(), enc.data(), len) == 0);
			StrVec_sanitize(strVec, key.size());
			if (!conf.isInputSorted || checkSortOrder(key))
				strVec.push_back(key);
		}
		strVec.finish();
	}
	else if (mmap_input_file) {
		byte_t* base = NULL;
		size_t  size = 0;
		try {
			if (NULL == input_fname) {
			  #if defined(_MSC_VER)
				fprintf(stderr, "ERROR: input file name is required when using -m\n");
				usage(argv[0]);
				return 1;
			  #else
				base = (byte_t*)mmap_load("/dev/stdin", &size, false, true);
			  #endif
			}
			else {
				base = (byte_t*)mmap_load(input_fname, &size, false, true);
			}
		} catch (std::exception&) {
			fprintf(stderr, "mmap failed\n");
			exit(1);
		}
		if (StrVec_needs_end_lf(strVec) && '\n' != base[size-1]) {
			fprintf(stderr, "ERROR: StrVec: -%c needs file ending LF('\\n')\n", strVecTypeChar);
			exit(1);
		}
		strVec.m_strpool.risk_set_data(base);
		byte_t* endp = base + size;
		byte_t* line = base;
		while (line < endp) {
			byte_t* next = std::find(line, endp, '\n');
			StrVec_index_push(strVec, line - base, next - line);
			line = next + 1;
		}
		StrVec_index_finish(strVec, line - base);
	}
	else {
		LineBuf line;
		while (line.getline(fp.self_or(stdin)) > 0) {
			line.chomp();
			StrVec_sanitize(strVec, line.size());
			if (!conf.isInputSorted || checkSortOrder(line))
				strVec.push_back(line);
		}
		strVec.finish();
	}
//	fprintf(stderr, "strVec.size() = %zd, strVec.str_size() = %zd\n", strVec.size(), strVec.str_size());
	if (strVec.size() == 0) {
		fprintf(stderr, "Input is empty\n");
		return 1;
	}
	fstrvecll allstr0;
	if (benchmarkLoop && NULL == bench_input_fname) {
		allstr0.strpool.assign(strVec.m_strpool);
		allstr0.offsets.resize_no_init(strVec.size() + 1);
		size_t offset = 0;
		for (size_t i = 0; i < strVec.size(); ++i) {
			allstr0.offsets[i] = offset;
			offset += strVec.nth_size(i);
		}
		allstr0.offsets.back() = offset;
	}
	long long t1 = pf.now();
	NestTrieDAWG trie;
	size_t allstrlen = strVec.m_strpool.size();
	trie.build_from(strVec, conf);
	long long t2 = pf.now();
	if (b_write_dot_file && input_fname) {
		std::string dotFile = std::string(input_fname) + ".pz.dot";
		trie.patricia_trie_write_dot_file(dotFile.c_str());
	}
	long long t3 = pf.now();
	trie.set_is_dag(true);
	trie.set_kv_delim(kv_delim);
	trie.save_mmap(rlouds_trie_fname);
//	NestTrieDAWG::load_mmap(rlouds_trie_fname);
	long long t31 = pf.now();
    if (nlt_order_fname) {
        Auto_close_fp wfp(fopen(nlt_order_fname, "w"));
        if (wfp) {
            valvec<byte_t> word;
            for (size_t i = 0; i < trie.num_words(); ++i) {
                trie.nth_word(i, &word);
                fprintf(wfp, "%.*s\n", int(word.size()), word.data());
            }
        }
        else {
            fprintf(stderr, "ERROR: fopen(\"%s\", \"w\") = %s\n",
                nlt_order_fname, strerror(errno));
            nlt_order_fname = NULL;
        }
    }
	long long t4 = pf.now();

  auto run_benchmark = [&](const NestTrieDAWG& trie) {
	if (benchmarkLoop) {
        MatchContext ctx;
#if 0
        valvec<byte_t> word;
        ADFA_LexIteratorUP iter(trie.adfa_make_iter(initial_state));
        size_t index1, dict_rank1;
        size_t index2, dict_rank2;
        size_t index;
        index = trie.index_begin();
        for (iter->seek_begin(); ;) {
            if (index != trie.state_to_word_id(iter->word_state())) {
                fprintf(stderr, "Dict Index API Error");
            }
            index = trie.index_next(index);
            if (!iter->incr()) {
                if (index != size_t(-1)) {
                    fprintf(stderr, "Dict Index API Error");
                }
                break;
            }
        }
        index = trie.index_end();
        for (iter->seek_end(); ;) {
            if (index != trie.state_to_word_id(iter->word_state())) {
                fprintf(stderr, "Dict Index API Error");
            }
            index = trie.index_prev(index);
            if (!iter->decr()) {
                if (index != size_t(-1)) {
                    fprintf(stderr, "Dict Index API Error");
                }
                break;
            }
        }
        iter->seek_lower_bound(fstring());
        trie.lower_bound(ctx, fstring(), &index1, &dict_rank1);
        if (iter->word_state() != index1 || trie.state_to_dict_rank(iter->word_state()) != dict_rank1) {
            fprintf(stderr, "Dict Index API Error");
        }
        for (size_t i = 0; i < trie.num_words(); ++i) {
            size_t state = trie.dict_rank_to_state(i);
            size_t rank = trie.state_to_dict_rank(state);
            if (rank != i) {
                fprintf(stderr, "Dict Index API Error: index=%zd, state=%zd, rank=%zd\n"
                    , i, state, rank);
            }
            trie.nth_word(i, &word);
            word.append(0);
            for (size_t j = 1; j <= word.size(); ++j) {
                auto w = fstring(word).substr(0, j);
                if (iter->seek_lower_bound(w)) {
                    state = iter->word_state();
                    rank = trie.state_to_dict_rank(state);
                    index = trie.state_to_word_id(state);
                    ctx.reset();
                    trie.lower_bound(ctx, iter->word(), &index1, &dict_rank1);
                }
                else {
                    index = size_t(-1);
                    rank = trie.num_words();
                    trie.lower_bound(ctx, w, &index1, &dict_rank1);
                }
                ctx.reset();
                trie.lower_bound(ctx, w, &index2, &dict_rank2);
                if (rank != dict_rank1 || rank != dict_rank2 || index != index1 || index != index2) {
                    fprintf(stderr, "Dict Index API Error: index=%zd, state=%zd, rank=%zd\n"
                        , j, state, rank);
                }
            }
        }
        iter.reset();
#endif
		fstrvecll allstr;
		allstr.reserve(trie.num_words());
		allstr.reserve_strpool(allstrlen);
		valvec<uint32_t> strIdx(trie.num_words(), valvec_reserve());
		/*
		auto bench_for_each_word = [&](unsigned long long nth, fstring w) {
			assert(nth == allstr.size());
			unsigned long long idx = trie.index(w);
			if (idx >= trie.num_words()) {
				fprintf(stderr, "Error: nth=%lld, index=%lld, word=(%.*s)\n"
					, nth, idx, w.ilen(), w.data());
			}
			allstr.push_back(w);
			strIdx.push_back(uint32_t(idx));
		};
		trie.for_each_word(ref(bench_for_each_word));
		*/
		long long t5 = pf.now();
		/*
		for (size_t i = 0; i < allstr.size(); ++i) {
			fstring w = allstr[i];
			bool bExists = false;
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				bExists = trie.accept(w);
			}
			if (!bExists) {
				fprintf(stderr, "Error: nth=%lld, accept(%.*s) = false\n"
					, (long long)i, w.ilen(), w.data());
			}
		}
		*/
		long long t6 = pf.now();
		for (size_t i = 0; i < allstr.size(); ++i) {
			fstring w = allstr[i];
			unsigned long long idx = 0;
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				idx = trie.index(w);
			}
			if (idx >= trie.num_words()) {
				fprintf(stderr, "Error: nth=%lld, index=%lld, word=(%.*s)\n"
					, (long long)i, idx, w.ilen(), w.data());
			}
		}
		long long t7 = pf.now();
		valvec<byte_t> restored;
		for (size_t i = 0; i < trie.num_words(); ++i) {
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				trie.nth_word(i, &restored);
			}
		}
		long long t8 = pf.now();
		if (NULL == bench_input_fname) {
			for(size_t  i = 0; i < allstr0.size(); ++i) {
				fstring s = allstr0[i];
				size_t  f = 0;
				for (size_t j = 0; j < benchmarkLoop; ++j)
					f = trie.index(s);
				if (f >= trie.num_words())
					fprintf(stderr, "not found key=%.*s\n", s.ilen(), s.data());
			}
		}
		else {
			Auto_close_fp fp2(fopen(bench_input_fname, "r"));
			if (!fp2) {
				fprintf(stderr
					, "ERROR: fopen(bench_input_fname = \"%s\") = %s\n"
					  "  So index000 result is invalid\n"
					, bench_input_fname, strerror(errno));
			}
			else {
				LineBuf key;
				while (key.getline(fp2) > 0) {
					key.chomp();
					size_t  f = 0;
					for (size_t j = 0; j < benchmarkLoop; ++j)
						f = trie.index(key);
					if (f >= trie.num_words())
						fprintf(stderr, "not found key=%s\n", key.p);
				}
			}
		}
		long long t9 = pf.now();
		for (size_t i = 0; i < trie.total_states(); ++i) {
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				trie.state_to_dict_rank(i);
			}
		}
		long long t10 = pf.now();
		for (size_t i = 0; i < trie.num_words(); ++i) {
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				trie.dict_rank_to_state(i);
			}
		}
		long long t11 = pf.now();
		for (size_t i = 0; i < allstr.size(); ++i) {
			fstring w = allstr[i];
			size_t idx = 0;
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				ctx.reset();
				trie.lower_bound(ctx, w, nullptr, &idx);
			}
			if (idx >= trie.num_words()) {
				fprintf(stderr, "Error: nth=%zd, index=%zd, word=(%.*s)\n"
					, i, idx, w.ilen(), w.data());
			}
		}
		long long t12 = pf.now();
		for (size_t i = 0; i < allstr.size(); ++i) {
			fstring w = allstr[i];
			unsigned long long idx = 0;
			for (size_t j = 0; j < benchmarkLoop; ++j) {
				ADFA_LexIteratorUP iter(trie.adfa_make_iter(initial_state));
				iter->seek_lower_bound(w);
				idx = trie.state_to_dict_rank(iter->word_state());
			}
			if (idx >= trie.num_words()) {
				fprintf(stderr, "Error: nth=%lld, index=%lld, word=(%.*s)\n"
					, (long long)i, idx, w.ilen(), w.data());
			}
		}
		long long t13 = pf.now();

		bool write_html_table = false;
		if (const char* env = getenv("write_html_table")) {
			write_html_table = atoi(env) ? true : false;
		}
		if (write_html_table) {
			fprintf(stderr,
				"<table><tbody>\n"
				"<tr>"
					"<th>class</th>"
					"<th colspan='4'>%s</th>"
				"</tr>\n"
				"<tr>"
					"<th></th>"
					"<th>Time</th>"
					"<th>mQPS</th>"
					"<th>ns/Q</th>"
					"<th>MB/s</th>"
				"</tr>\n"
				, typeid(NestTrieDAWG).name()
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
				, trie.num_words() / pf.uf(t0, t1)
				, pf.nf(t0, t1) / trie.num_words()
				, allstrlen / pf.uf(t0, t1)
				);
			fprintf(stderr,
				"<tr>"
					"<th align='left'>Building trie</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t1, t2)
				, trie.num_words() / pf.uf(t1, t2)
				, pf.nf(t1, t2) / trie.num_words()
				, allstrlen / pf.uf(t1, t2)
				);
			fprintf(stderr,
				"<tr>"
					"<th align='left'>Save mmap dfa</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t3, t31)
				, trie.num_words() / pf.uf(t3, t31)
				, pf.nf(t3, t31) / trie.num_words()
				, allstrlen / pf.uf(t3, t31)
				);
			fprintf(stderr,
				"<tr>"
					"<th align='left'>for_each_word</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t4, t5)
				, trie.num_words() / pf.uf(t4, t5)
				, pf.nf(t4, t5) / trie.num_words()
				, allstrlen / pf.uf(t4, t5)
				);
			fprintf(stderr,
				"<tr align='left'>"
					"<th>Bench accept</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t5, t6)
				, benchmarkLoop * trie.num_words() / pf.uf(t5, t6)
				, pf.nf(t5, t6) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t5, t6)
				);
			fprintf(stderr,
				"<tr align='left'>"
					"<th>Bench indexMem</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t6, t7)
				, benchmarkLoop * trie.num_words() / pf.uf(t6, t7)
				, pf.nf(t6, t7) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t6, t7)
				);
			fprintf(stderr,
				"<tr align='left'>"
					"<th>Bench nth_word</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t7, t8)
				, benchmarkLoop * trie.num_words() / pf.uf(t7, t8)
				, pf.nf(t7, t8) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t7, t8)
				);
			fprintf(stderr,
				"<tr align='left'>"
					"<th>Bench indexFil</th>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.3f</td>"
					"<td align='right'>%8.1f</td>"
					"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t8, t9)
				, benchmarkLoop * trie.num_words() / pf.uf(t8, t9)
				, pf.nf(t8, t9) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t8, t9)
				);
			fprintf(stderr,
				"<tr align='left'>"
				"<th>Bench rank</th>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.1f</td>"
				"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t9, t10)
				, benchmarkLoop * trie.total_states() / pf.uf(t9, t10)
				, pf.nf(t9, t10) / (benchmarkLoop * trie.total_states())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t9, t10)
			);
			fprintf(stderr,
				"<tr align='left'>"
				"<th>Bench select</th>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.1f</td>"
				"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t10, t11)
				, benchmarkLoop * trie.num_words() / pf.uf(t10, t11)
				, pf.nf(t10, t11) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t10, t11)
			);
			fprintf(stderr,
				"<tr align='left'>"
				"<th>Word Rank</th>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.1f</td>"
				"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t11, t12)
				, benchmarkLoop * trie.num_words() / pf.uf(t11, t12)
				, pf.nf(t11, t12) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t11, t12)
			);
			fprintf(stderr,
				"<tr align='left'>"
				"<th>Iter Rank</th>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.1f</td>"
				"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t12, t13)
				, benchmarkLoop * trie.num_words() / pf.uf(t12, t13)
				, pf.nf(t12, t13) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t12, t13)
			);
			if (nlt_order_fname) fprintf(stderr,
				"<tr align='left'>"
				"<th>Write NLTOrder</th>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.3f</td>"
				"<td align='right'>%8.1f</td>"
				"<td align='right'>%8.3f</td>"
				"</tr>\n"
				, pf.sf(t31, t4)
				, trie.num_words() / pf.uf(t31, t4)
				, pf.nf(t31, t4) / (trie.num_words())
				, allstr0.strpool.size() / pf.uf(t31, t4)
			);
			fprintf(stderr, "</tbody></table>\n");
		}
		else {
			fprintf(stderr
				, "TrieDAWG class:  %s\n"
				, typeid(NestTrieDAWG).name()
				);
			fprintf(stderr
				, "Read text file:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t0, t1)
				, trie.num_words() / pf.mf(t0, t1)
				, pf.nf(t0, t1) / trie.num_words()
				, allstrlen / pf.uf(t0, t1)
				);
			fprintf(stderr
				, "Building trie :  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t1, t2)
				, trie.num_words() / pf.mf(t1, t2)
				, pf.nf(t1, t2) / trie.num_words()
				, allstrlen / pf.uf(t1, t2)
				);
			fprintf(stderr
				, "Save mmap dfa :  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t3, t31)
				, trie.num_words() / pf.mf(t3, t31)
				, pf.nf(t3, t31) / trie.num_words()
				, allstrlen / pf.uf(t3, t31)
				);
			fprintf(stderr
				, "for_each_word :  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t4, t5)
				, trie.num_words() / pf.mf(t4, t5)
				, pf.nf(t4, t5) / trie.num_words()
				, allstrlen / pf.uf(t4, t5)
				);
			fprintf(stderr
				, "Bench accept  :  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t5, t6)
				, benchmarkLoop * trie.num_words() / pf.mf(t5, t6)
				, pf.nf(t5, t6) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t5, t6)
				);
			fprintf(stderr
				, "Bench indexMem:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t6, t7)
				, benchmarkLoop * trie.num_words() / pf.mf(t6, t7)
				, pf.nf(t6, t7) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t6, t7)
				);
			fprintf(stderr
				, "Bench nth_word:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t7, t8)
				, benchmarkLoop * trie.num_words() / pf.mf(t7, t8)
				, pf.nf(t7, t8) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstrlen / pf.uf(t7, t8)
				);
			fprintf(stderr
				, "Bench indexFil:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t8, t9)
				, benchmarkLoop * trie.num_words() / pf.mf(t8, t9)
				, pf.nf(t8, t9) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t8, t9)
				);
			fprintf(stderr
				, "Bench NodeRank:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t9, t10)
				, benchmarkLoop * trie.total_states() / pf.mf(t9, t10)
				, pf.nf(t9, t10) / (benchmarkLoop * trie.total_states())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t9, t10)
			);
			fprintf(stderr
				, "Bench NodeSel :  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t10, t11)
				, benchmarkLoop * trie.num_words() / pf.mf(t10, t11)
				, pf.nf(t10, t11) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t10, t11)
			);
			fprintf(stderr
				, "Bench WordRank:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t11, t12)
				, benchmarkLoop * trie.num_words() / pf.mf(t11, t12)
				, pf.nf(t11, t12) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t11, t12)
			);
			fprintf(stderr
				, "Bench IterRank:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t12, t13)
				, benchmarkLoop * trie.num_words() / pf.mf(t12, t13)
				, pf.nf(t12, t13) / (benchmarkLoop * trie.num_words())
				, benchmarkLoop * allstr0.strpool.size() / pf.uf(t12, t13)
			);
			if (nlt_order_fname) fprintf(stderr
				, "Write NLTOrder:  Time:%8.3f   QPS:%8.1f K    %8.1f ns/Q   %9.3f MB/s\n"
				, pf.sf(t31, t4)
				, trie.num_words() / pf.mf(t31, t4)
				, pf.nf(t31, t4) / trie.num_words()
				, allstr0.strpool.size() / pf.uf(t31, t4)
			);
		}
	}
  };
	auto pDFA = BaseDFA::load_mmap(rlouds_trie_fname);
	if (NULL == pDFA) {
		fprintf(stderr
			, "ERROR: unexpected: file %s should be type: %s\n"
			, rlouds_trie_fname, typeid(trie).name());
	} else {
		std::unique_ptr<NestTrieDAWG> trie2(dynamic_cast<NestTrieDAWG*>(pDFA));
		trie2->debug_equal_check(trie);
        //trie2->build_fsa_cache(std::max(0.1, 101.0 / trie2->total_states()), nullptr);
		run_benchmark(*trie2);
	}
	return 0;
}
