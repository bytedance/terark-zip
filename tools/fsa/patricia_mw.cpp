#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif

#include <terark/fsa/cspptrie.inl>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/stat.hpp>
#include <getopt.h>
#include <fcntl.h>
#include <random>
#include <thread>
#include <terark/num_to_str.hpp>

using namespace terark;

void usage(const char* prog) {
    fprintf(stderr, R"EOS(Usage: %s Options Input-TXT-File
Options:
    -h Show this help information
    -m MaxMem
    -o Output-Trie-File
    -i Condurrent write interleave
    -j Mark readonly for read
    -d Read Key from mmap
    -r Reader Thread Num
    -t Writer Thread Num
    -w Writer ConcurrentLevel
    -v Value size ratio over key size
    -z Zero Value content
    -s print stat
    -S Single thread write
    -b BenchmarkLoop : Run benchmark
If Input-TXT-File is omitted, use stdin
)EOS", prog);
    exit(1);
}

size_t benchmarkLoop = 0;
size_t maxMem = 0;
const char* bench_input_fname = NULL;
const char* patricia_trie_fname = NULL;

int main(int argc, char* argv[]) {
    int write_thread_num = std::thread::hardware_concurrency();
    int read_thread_num = 0;
    bool mark_readonly = false;
    bool direct_read_input = false;
    bool print_stat = false;
    bool concWriteInterleave = false;
    bool single_thread_write = false;
    bool zeroValue = false;
    double valueRatio = 0;
    auto conLevel = Patricia::MultiWriteMultiRead;
    for (;;) {
        int opt = getopt(argc, argv, "b:dhm:o:t:w:r:ijsSv:z");
        switch (opt) {
        case -1:
            goto GetoptDone;
        case 'b':
            {
                char* endpos = NULL;
                benchmarkLoop = strtoul(optarg, &endpos, 10);
                if ('@' == *endpos) {
                    bench_input_fname = endpos + 1;
                }
                break;
            }
        case 'd':
            direct_read_input = true;
            break;
        case 'i':
            concWriteInterleave = true;
            break;
        case 'm':
            maxMem = ParseSizeXiB(optarg);
            break;
        case 'o':
            patricia_trie_fname = optarg;
            break;
        case 't':
            write_thread_num = atoi(optarg);
            break;
        case 'w':
            if (strcmp(optarg, "SingleThreadStrict") == 0) {
                conLevel = Patricia::SingleThreadStrict;
            }
            else if (strcmp(optarg, "Strict") == 0) {
                conLevel = Patricia::SingleThreadStrict;
            }
            else if (strcmp(optarg, "SingleThreadShared") == 0) {
                conLevel = Patricia::SingleThreadShared;
            }
            else if (strcmp(optarg, "Shared") == 0) {
                conLevel = Patricia::SingleThreadShared;
            }
            else if (strncmp(optarg, "OneWriteMultiRead", 3) == 0) {
                conLevel = Patricia::OneWriteMultiRead;
            }
            else if (strncmp(optarg, "MultiWriteMultiRead", 3) == 0) {
                conLevel = Patricia::MultiWriteMultiRead;
            }
            else {
                fprintf(stderr, "ERROR: -w %s : Invalid ConcurrentLevel\n", optarg);
                return 1;
            }
            break;
        case 'r':
            read_thread_num = std::max(1, atoi(optarg));
            break;
        case 'j':
            mark_readonly = true;
            break;
        case 's':
            print_stat = true;
            break;
        case 'S':
            single_thread_write = true;
            break;
        case 'v':
            valueRatio = atof(optarg);
            break;
        case 'z':
            zeroValue = true;
            break;
        case '?':
        case 'h':
        default:
            usage(argv[0]);
        }
    }
GetoptDone:
    terark::profiling pf;
    const char* input_fname = argv[optind];
    Auto_fclose fp;
    if (input_fname) {
        fp = fopen(input_fname, "rb");
        if (NULL == fp) {
            fprintf(stderr, "FATAL: fopen(\"%s\", \"r\") = %s\n", input_fname, strerror(errno));
            return 1;
        }
    }
    else {
        fprintf(stderr, "Reading from stdin...\n");
    }
    if (read_thread_num > 0 && !single_thread_write) {
        fprintf(stderr
          , "WARN: read_thread_num = %d > 0, auto enable single thread write\n"
          , read_thread_num);
        single_thread_write = true;
    }
    maximize(write_thread_num, 1);
    MmapWholeFile mmap;
    {
        struct ll_stat st;
        int err = ::ll_fstat(fileno(fp.self_or(stdin)), &st);
        if (err) {
            fprintf(stderr, "ERROR: fstat failed = %s\n", strerror(errno));
        }
        else if (S_ISREG(st.st_mode)) { // OK, get file size
            bool writable = false;
            bool populate = true;
            mmap.base = mmap_load(input_fname, &mmap.size, writable, populate);
            if (0 == maxMem)
                maxMem = 2*st.st_size;
        }
    }
    SortableStrVec strVec;
    MainPatricia trie(sizeof(size_t), maxMem, conLevel);
    MainPatricia trie2(sizeof(size_t), maxMem, Patricia::MultiWriteMultiRead);
    MainPatricia* pt = single_thread_write ? &trie2 : &trie;
    size_t sumkeylen = 0;
    size_t sumvaluelen = 0;
    size_t numkeys = 0;
    long long t0, t1, t2, t3, t4, t5, t6, t7;
    auto mmapReadStrVec = [&]() {
        strVec.m_strpool.reserve(mmap.size);
        strVec.m_index.reserve(mmap.size / 16);
        char* line = (char*)mmap.base;
        char* endp = line + mmap.size;
        while (line < endp) {
            char* next = std::find(line, endp, '\n');
            strVec.push_back(fstring(line, next - line));
            line = next + 1;
        }
        sumkeylen = strVec.str_size();
    };
    auto lineReadStrVec = [&]() {
        LineBuf line;
        while (line.getline(fp.self_or(stdin)) > 0) {
            line.chomp();
            if (line.empty()) {
                fprintf(stderr, "empty line\n");
            }
            sumkeylen += line.n;
            strVec.push_back(line);
        }
        as_atomic(sumvaluelen).fetch_add(8*strVec.size());
    };
    auto readStrVec = [&]() {
        t0 = pf.now();
        if (mmap.base) {
            if (!concWriteInterleave && !direct_read_input) {
                mmapReadStrVec();
            }
            else {
                // do not read strVec
            }
        }
        else {
            lineReadStrVec();
        }
        numkeys = strVec.size();
        t1 = pf.now();
        if (strVec.size()) {
            fprintf(stderr
                , "read      input: time = %8.3f sec, %8.3f MB/sec, avglen = %8.3f\n"
                , pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1), strVec.avg_size()
            );
        }
    };
    readStrVec();
    t0 = pf.now();
    valvec<size_t> randvec(strVec.size(), valvec_no_init());
    fstrvecll fstrVec;
	if (read_thread_num > 0 && single_thread_write) {
		for (size_t i = 0; i < randvec.size(); ++i) randvec[i] = i;
		shuffle(randvec.begin(), randvec.end(), std::mt19937_64());
		t1 = pf.now();
		fprintf(stderr
			, "generate  shuff: time = %8.3f sec, %8.3f MB/sec\n"
			, pf.sf(t0,t1), randvec.used_mem_size()/pf.uf(t0,t1)
		);
		t0 = pf.now();
		fstrVec.reserve(strVec.size());
		fstrVec.reserve_strpool(strVec.str_size());
		for(size_t i = 0; i < strVec.size(); ++i) {
			size_t j = randvec[i];
			fstrVec.push_back(strVec[j]);
		}
		t1 = pf.now();
		fprintf(stderr
			, "fstrVec   shuff: time = %8.3f sec, %8.3f MB/sec\n"
			, pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1)
		);
		t0 = pf.now(); // fstrVec memcpy
		{
			fstrVec.strpool.assign(strVec.m_strpool);
			size_t offset = 0;
			auto   offsets = fstrVec.offsets.data();
			for (size_t i = 0; i < strVec.size(); ++i) {
				offsets[i] = offset;
				offset += strVec.m_index[i].length;
			}
			fstrVec.offsets.back() = offset;
		}
		t1 = pf.now();
		fprintf(stderr
			, "fstrVec  memcpy: time = %8.3f sec, %8.3f MB/sec\n"
			, pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1)
		);
		t0 = pf.now(); // fstrVec append
		{
			fstrVec.offsets.erase_all();
			fstrVec.strpool.erase_all();
			for (size_t i = 0; i < strVec.size(); ++i)
				fstrVec.push_back(strVec[i]);
		}
		t1 = pf.now();
		fprintf(stderr
			, "fstrVec  append: time = %8.3f sec, %8.3f MB/sec\n"
			, pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1)
		);
	}
    auto patricia_find = [&](int tid, size_t Beg, size_t End) {
        Patricia::ReaderToken& token = *pt->acquire_tls_reader_token();
        if (mark_readonly) {
            for (size_t i = Beg; i < End; ++i) {
                fstring s = fstrVec[i];
                if (!pt->lookup(s, &token))
                    fprintf(stderr, "pttrie not found: %.*s\n", s.ilen(), s.data());
            }
        }
        else {
            for (size_t i = Beg; i < End; ++i) {
                token.update_lazy();
                fstring s = fstrVec[i];
                if (!pt->lookup(s, &token))
                    fprintf(stderr, "pttrie not found: %.*s\n", s.ilen(), s.data());
            }
        }
        token.release();
    };
    auto patricia_lb = [&](int tid, size_t Beg, size_t End) {
        char iter_mem[Patricia::ITER_SIZE];
        pt->construct_iter(iter_mem);
        auto& iter = *reinterpret_cast<Patricia::Iterator*>(iter_mem);
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            if (!iter.seek_lower_bound(s))
                fprintf(stderr, "pttrie lower_bound failed: %.*s\n", s.ilen(), s.data());
        }
        iter.~Iterator();
    };
    auto exec_read = [&](std::function<void(int,size_t,size_t)> read) {
        if (!single_thread_write) {
            fprintf(stderr, "single_thread_write = false, skip read test\n");
            return;
        }
        valvec<std::thread> thrVec(read_thread_num - 1, valvec_reserve());
        for (int i = 0; i < read_thread_num; ++i) {
            size_t Beg = (i + 0) * fstrVec.size() / read_thread_num;
            size_t End = (i + 1) * fstrVec.size() / read_thread_num;
            if (i < read_thread_num-1)
                thrVec.unchecked_emplace_back([=](){read(i, Beg, End);});
            else
                read(i, Beg, End);
        }
        for (auto& t : thrVec) t.join();
    };
	auto pt_write = [&](int tnum, MainPatricia* ptrie) {
		auto fins = [&](int tid) {
			//fprintf(stderr, "thread-%03d: beg = %8zd , end = %8zd , num = %8zd\n", tid, beg, end, end - beg);
            auto& ptoken = ptrie->tls_writer_token();
            //assert(ptoken->get() == nullptr);
            if (ptoken.get() == nullptr) {
                // user may extends WriterToken and override init() ...
                ptoken.reset(new Patricia::WriterToken(ptrie));
            }
            Patricia::WriterToken& token = *ptoken;
            size_t sumvaluelen1 = 0;
            auto insert_v0 = [&](fstring key, size_t i) {
                if (ptrie->insert(key, &i, &token)) {
                    if (!token.value()) {
                        // Patricia run out of maxMem
                        fprintf(stderr
                            , "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                            , tid, maxMem, i, ptrie->mem_frag_size());
                        return false;
                    }
                }
                return true;
            };
            auto insert_vx = [&](fstring key, size_t i) {
                struct PosLen { uint32_t pos, len; };
                PosLen pl{UINT32_MAX, uint32_t(valueRatio*key.size())};
                pl.pos = ptrie->mem_alloc(std::max(pl.len, 1u));
                if (uint32_t(MainPatricia::mem_alloc_fail) == pl.pos) {
                    fprintf(stderr
                        , "thread-%02d value alloc %d run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                        , int(pl.len)
                        , tid, maxMem, i, ptrie->mem_frag_size());
                    return false;
                }
                char* pv = (char*)ptrie->mem_get(pl.pos);
                if (zeroValue) {
                    memset(pv, 0, pl.len);
                }
                else {
                    for(size_t j = 0; j < pl.len; ) {
                        size_t n = std::min(pl.len - j, key.size());
                        memcpy(pv + j, key.p, n);
                        j += n;
                    }
                }
                if (ptrie->insert(key, &pl, &token)) {
                    if (!token.value()) {
                        // Patricia run out of maxMem
                        fprintf(stderr
                            , "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                            , tid, maxMem, i, ptrie->mem_frag_size());
                        return false;
                    }
                }
                sumvaluelen1 += pl.len;
                return true;
            };
#define isnewline(c) ('\r' == c || '\n' == c)
            if (mmap.base && direct_read_input) {
                auto mmap_endp = (const char*)mmap.base + mmap.size;
                auto line = (const char*)mmap.base + mmap.size * (size_t(tid) + 0) / tnum;
                auto endp = (const char*)mmap.base + mmap.size * (size_t(tid) + 1) / tnum;
                while (endp < mmap_endp && !isnewline(*endp)) endp++;
                if (0 != tid) {
                    while (line < endp && !isnewline(*line)) line++;
                    while (line < endp &&  isnewline(*line)) line++;
                }
                size_t i = 0;
                size_t sumkeylen1 = 0;
                if (valueRatio > 0) {
                    while (line < endp) {
                        const char* next = line;
                        while (next < endp && !isnewline(*next)) next++;
                        if (!insert_vx(fstring(line, next), i)) {
                            break;
                        }
                        sumkeylen1 += next - line;
                        while (next < endp && isnewline(*next)) next++;
                        line = next;
                        i++;
                    }
                }
                else {
                    while (line < endp) {
                        const char* next = line;
                        while (next < endp && !isnewline(*next)) next++;
                        if (!insert_v0(fstring(line, next), i)) {
                            break;
                        }
                        sumkeylen1 += next - line;
                        while (next < endp && isnewline(*next)) next++;
                        line = next;
                        i++;
                    }
                }
                as_atomic(sumvaluelen).fetch_add(sumvaluelen1 + 8*i);
                as_atomic(sumkeylen).fetch_add(sumkeylen1);
                as_atomic(numkeys).fetch_add(i);
            }
            else {
                size_t beg = strVec.size() * (tid + 0) / tnum;
                size_t end = strVec.size() * (tid + 1) / tnum;
                for (size_t i = beg; i < end; ++i) {
                    fstring s = strVec[i];
                    if (ptrie->insert(s, &i, &token)) {
                        if (!token.value()) {
                            // Patricia run out of maxMem
                            fprintf(stderr
                                , "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                                , tid, maxMem, i, ptrie->mem_frag_size());
                            break;
                        }
                    }
                }
            }
		};
		auto finsInterleave = [&](int tid) {
			//fprintf(stderr, "thread-%03d: interleave, num = %8zd\n", tid, strVec.size() / tnum);
            auto& ptoken = ptrie->tls_writer_token();
            //assert(ptoken->get() == nullptr);
            if (ptoken.get() == nullptr) {
                // user may extends WriterToken and override init() ...
                ptoken.reset(new Patricia::WriterToken(ptrie));
            }
            Patricia::WriterToken& token = *ptoken;
			for (size_t i = tid, n = strVec.size(); i < n; i += tnum) {
				fstring s = strVec[i];
				if (ptrie->insert(s, &i, &token)) {
					if (!token.value()) {
						// Patricia run out of maxMem
						fprintf(stderr
							, "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
							, tid, maxMem, i, ptrie->mem_frag_size());
						break;
					}
				}
			}
		};
		valvec<std::thread> thrVec(tnum, valvec_reserve());
		for (int i = 0; i < tnum; ++i) {
			auto tfunc = [=]() {
				if (concWriteInterleave)
					finsInterleave(i);
				else
					fins(i);
			};
			thrVec.unchecked_emplace_back(tfunc);
		}
		for (auto& thr: thrVec) {
			thr.join();
		}
		if (mark_readonly) {
			ptrie->set_readonly();
		}
	};
	t0 = pf.now(); if (single_thread_write) { pt_write(1, &trie); };
	t1 = pf.now();
	t2 = pf.now(); if (read_thread_num > 0) { exec_read(patricia_find); }
	t3 = pf.now(); if (read_thread_num > 0) { exec_read(patricia_lb);   }
	t4 = pf.now();
	t5 = pf.now();
    t6 = pf.now(); pt_write(write_thread_num, &trie2);
    t7 = pf.now();
    fprintf(stderr, "numkeys = %zd, sumkeylen = %zd, avglen = %f\n"
        , numkeys, sumkeylen, double(sumkeylen) / numkeys
    );
  if (single_thread_write) {
    fprintf(stderr
        , "patricia insert: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, fragments = %7zd (%.2f%%)), words = %zd, nodes = %zd, fanout = %.3f\n"
        , pf.sf(t0, t1), (sumkeylen + sumvaluelen) / pf.uf(t0, t1), numkeys / pf.uf(t0, t1)
        , trie.mem_size() / 1e6, (trie.mem_size() - sumvaluelen) / 1e6, sumvaluelen / 1e6
        , trie.mem_frag_size(), 100.0*trie.mem_frag_size()/trie.mem_size()
        , trie.num_words(), trie.v_gnode_states()
        , trie.num_words() / double(trie.v_gnode_states() - trie.num_words())
    );
  }
    fprintf(stderr
        , "patricia mt_Add: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, fragments = %7zd (%.2f%%)), words = %zd, nodes = %zd, fanout = %.3f, speed ratio = %.2f\n"
        , pf.sf(t6, t7), (sumkeylen + sumvaluelen) / pf.uf(t6, t7), numkeys / pf.uf(t6, t7)
        , trie2.mem_size() / 1e6, (trie2.mem_size() - sumvaluelen) / 1e6, sumvaluelen / 1e6
        , trie2.mem_frag_size(), 100.0*trie2.mem_frag_size()/trie2.mem_size()
        , trie2.num_words(), trie2.v_gnode_states()
        , trie2.num_words() / double(trie2.v_gnode_states() - trie2.num_words())
        , pf.uf(t0, t1) / pf.uf(t6, t7)
    );
	if (read_thread_num > 0 && single_thread_write) {
		fprintf(stderr
			, "patricia  point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
			, pf.sf(t2,t3), sumkeylen/pf.uf(t2,t3), numkeys/pf.uf(t2,t3)
		);
		fprintf(stderr
			, "patricia  lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia point)\n"
			, pf.sf(t3,t4), sumkeylen/pf.uf(t3,t4), numkeys/pf.uf(t3,t4)
			, 100.0*(t3-t2)/(t4-t3)
		);
	}
    if (patricia_trie_fname) {
        t0 = pf.now();
        pt->save_mmap(patricia_trie_fname);
        t1 = pf.now();
        fprintf(stderr
            , "patricia   save: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, mem_size = %9.3f M\n"
            , pf.sf(t0, t1), pt->mem_size() / pf.uf(t0, t1), numkeys / pf.uf(t0, t1)
            , pt->mem_size() / 1e6
        );
    }
    if (read_thread_num > 0) {
        t0 = pf.now();
      {
        std::unique_ptr<ADFA_LexIterator> iter(pt->adfa_make_iter());
        bool ok = iter->seek_begin();
        for (size_t i = 0; i < strVec.size(); ++i) {
            assert(ok);
            assert(iter->word() == strVec[i]);
            ok = iter->incr();
        }
        TERARK_UNUSED_VAR(ok);
      }
        t1 = pf.now();
        fprintf(stderr
            , "patricia   iter: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
            , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), numkeys / pf.uf(t0, t1)
        );
    }
    auto stat = pt->trie_stat();
    double sum = stat.sum() / 100.0;
    fprintf(stderr, "fstrVec    size: %8zd\n", fstrVec.size());
    fprintf(stderr, "patricia   size: %8zd\n", pt->num_words());
    fprintf(stderr
        , "patricia   stat|             fork  |           split  |      mark_final  |  add_state_move  |\n"
          "sum = %9zd| %8zd : %5.2f%% |%8zd : %5.2f%% |%8zd : %5.2f%% |%8zd : %5.2f%% |\n"
        , stat.sum()
        , stat.n_fork, stat.n_fork/sum
        , stat.n_split, stat.n_split/sum
        , stat.n_mark_final, stat.n_mark_final/sum
        , stat.n_add_state_move, stat.n_add_state_move/sum
    );
    if (!print_stat) {
        return 0;
    }
    auto ms = pt->mem_get_stat();
    fprintf(stderr, "------------------------------------------------------------------------\n");
    fprintf(stderr
        , " lazyfreelist  |   mem_cnt  | %10.6f M |            |\n"
        , ms.lazy_free_cnt / 1e6
    );
    fprintf(stderr
        , " lazyfreelist  |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , ms.lazy_free_sum / 1e6
        , 100.0* ms.lazy_free_sum / ms.used_size
    );
    fprintf(stderr
        , " fragments     |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , pt->mem_frag_size() / 1e6
        , 100.0* pt->mem_frag_size() / ms.used_size
    );
    fprintf(stderr
        , " real used     |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , (ms.used_size - ms.lazy_free_sum - pt->mem_frag_size()) / 1e6
        , (ms.used_size - ms.lazy_free_sum - pt->mem_frag_size())* 100.0 / ms.used_size
    );
    fprintf(stderr, "------------------------------------------------------------------------\n");
    fprintf(stderr, "mpool  fastbin | block size | entry num | total size | size ratio |\n");
    size_t sum_fast_cnt = 0;
    size_t sum_fast_size = 0;
    for(size_t i = 0; i < ms.fastbin.size(); ++i) {
        size_t k = ms.fastbin[i];
        sum_fast_cnt += k;
        sum_fast_size += 4*(i+1)*k;
        if (k)
            fprintf(stderr
                , "               | %10zd | %9zd | %10zd | %9.2f%% |\n"
                , 4*(i+1), k, 4*(i+1)*k, 100.0*4*(i+1)*k/ms.frag_size
            );
    }
    fprintf(stderr
        , "               | total fast | %9zd | %10zd | %9.2f%% |\n"
        , sum_fast_cnt, sum_fast_size
        , 100.0*sum_fast_size/pt->mem_frag_size()
    );
    fprintf(stderr
        , "               | total huge | %9zd | %10zd | %9.2f%% |\n"
        , ms.huge_cnt, ms.huge_size
        , 100.0*ms.huge_size/ms.frag_size
    );
    fprintf(stderr
        , "               | total frag | %9zd | %10zd | %9.2f%% |\n"
        , sum_fast_cnt + ms.huge_cnt, ms.frag_size
        , 100.0
    );
    fprintf(stderr
        , "               |   capacity |           | %10zd | %9.2f%% |\n"
        , ms.capacity, 100.0*ms.capacity/ms.used_size
    );
    assert(pt->mem_frag_size() - sum_fast_size == ms.huge_size);
    assert(pt->mem_frag_size() == ms.frag_size);
    assert(sum_fast_size == ms.frag_size - ms.huge_size);

    if (single_thread_write && write_thread_num > 1) {
        fprintf(stderr, "verify multi-written trie iter...\n");
        TERARK_RT_assert(trie.num_words() == trie2.num_words(), std::logic_error);
        t0 = pf.now();
        Patricia::IterMem iter1, iter2;
        iter1.construct(&trie);
        iter2.construct(&trie2);
        fprintf(stderr, "verify multi-written trie iter incr...\n");
        bool b1 = iter1.iter()->seek_begin();
        bool b2 = iter2.iter()->seek_begin();
        TERARK_RT_assert(b1 == b2, std::logic_error);
        while (b1) {
            TERARK_RT_assert(true == b2, std::logic_error);
            TERARK_RT_assert(iter1.iter()->word() == iter2.iter()->word(), std::logic_error);
            b1 = iter1.iter()->incr();
            b2 = iter2.iter()->incr();
        }
        assert(false == b2);
        t1 = pf.now();
        fprintf(stderr, "verify multi-written trie iter decr...\n");
        t2 = pf.now();
        b1 = iter1.iter()->seek_end();
        b2 = iter2.iter()->seek_end();
        TERARK_RT_assert(b1 == b2, std::logic_error);
        while (b1) {
            TERARK_RT_assert(true == b2, std::logic_error);
            TERARK_RT_assert(iter1.iter()->word() == iter2.iter()->word(), std::logic_error);
            b1 = iter1.iter()->decr();
            b2 = iter2.iter()->decr();
        }
        assert(false == b2);
        t3 = pf.now();
        fprintf(stderr, "verify multi-written trie iter decr...\n");
        fprintf(stderr, "verify multi-written trie iter done!\n");
        fprintf(stderr, "incr time = %f sec, throughput = %8.3f MB/sec, QPS = %8.3f\n", pf.sf(t0,t1), 2*sumkeylen/pf.uf(t0,t1), 2*numkeys/pf.uf(t0,t1));
        fprintf(stderr, "decr time = %f sec, throughput = %8.3f MB/sec, QPS = %8.3f\n", pf.sf(t2,t3), 2*sumkeylen/pf.uf(t2,t3), 2*numkeys/pf.uf(t2,t3));
    }
    return 0;
}
