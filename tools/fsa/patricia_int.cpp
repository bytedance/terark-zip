#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif

#include <terark/fsa/dynamic_patricia_trie.inl>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/stat.hpp>
#include <terark/io/byte_swap.hpp>
#include <getopt.h>
#include <fcntl.h>
#include <random>
#include <thread>

using namespace terark;

void usage(const char* prog) {
    fprintf(stderr, R"EOS(Usage: %s Options maxNum
Options:
    -h Show this help information
    -m MaxMem
    -o Output-Trie-File
    -i Condurrent write interleave
    -j Mark readonly for read
    -R random key
    -r Reader Thread Num
    -t Writer Thread Num
    -w Writer ConcurrentLevel
    -s print stat
    -B intkey to LittleEndian
    -S Single thread write
    -b BenchmarkLoop : Run benchmark
If Input-TXT-File is omitted, use stdin
)EOS", prog);
    exit(1);
}

int main(int argc, char* argv[]) {
    size_t benchmarkLoop = 0;
    size_t maxMem = 0;
    size_t write_thread_num = std::thread::hardware_concurrency();
    size_t read_thread_num = 0;
    bool little_endian = false;
    bool mark_readonly = false;
    bool print_stat = false;
    bool random_key = false;
    bool concWriteInterleave = false;
    bool single_thread_write = false;
    auto conLevel = Patricia::MultiWriteMultiRead;
    const char* patricia_trie_fname = NULL;
    for (;;) {
        int opt = getopt(argc, argv, "b:dhm:o:t:w:r:ijlRsS");
        switch (opt) {
        case -1:
            goto GetoptDone;
        case 'b':
            benchmarkLoop = strtoul(optarg, NULL, 10);
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
            break;
        case 'R':
            random_key = true;
            break;
        case 'r':
            read_thread_num = std::max(1, atoi(optarg));
            break;
        case 'j':
            mark_readonly = true;
            break;
        case 'l':
            little_endian = true;
        case 's':
            print_stat = true;
            break;
        case 'S':
            single_thread_write = true;
            break;
        case '?':
        case 'h':
        default:
            usage(argv[0]);
        }
    }
GetoptDone:
    size_t maxNum = 1000000;
    if (optind < argc) {
        maxNum = strtoull(argv[optind], NULL, 0);
    }
    if (0 == maxMem) {
        maxMem = 128 * maxNum;
    }
    if (print_stat) {
        mark_readonly = true;
    }
    terark::profiling pf;
    maximize(write_thread_num, 1);
    MainPatricia trie(sizeof(size_t), maxMem, conLevel);
    MainPatricia trie2(sizeof(size_t), maxMem, Patricia::MultiWriteMultiRead);
    MainPatricia* pt = single_thread_write ? &trie : &trie2;
    long long t0, t1, t2, t3, t4;
    t0 = pf.now();
    t1 = pf.now();
    t0 = pf.now();
    valvec<ullong> keyvec(maxNum, valvec_no_init());
    auto cvtkey = [=](ullong k) {
        if (little_endian)
            return k;
        else
            return byte_swap(k);
    };
    if (little_endian) {
        for (size_t i = 0; i < keyvec.size(); ++i) keyvec[i] = i;
    }
    else {
        for (size_t i = 0; i < keyvec.size(); ++i) keyvec[i] = byte_swap(i);
    }
    if (random_key) {
        shuffle(keyvec.begin(), keyvec.end(), std::mt19937_64());
    }
	t1 = pf.now();
	fprintf(stderr
		, "generate  shuff: time = %8.3f sec, %8.3f MB/sec\n"
		, pf.sf(t0,t1), keyvec.used_mem_size()/pf.uf(t0,t1)
	);
    auto patricia_find = [&](size_t tid, size_t Beg, size_t End) {
        Patricia::ReaderToken& token = *pt->acquire_tls_reader_token();
        if (mark_readonly) {
            for (size_t i = Beg; i < End; ++i) {
                fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong));
                if (!pt->lookup(s, &token))
                    fprintf(stderr, "pttrie lookup not found: %llu\n", cvtkey(keyvec[i]));
                if (token.value_of<ullong>() != unaligned_load<ullong>(s.p))
                    fprintf(stderr, "pttrie lookup wrong value: %llu\n", cvtkey(keyvec[i]));
            }
        }
        else {
            for (size_t i = Beg; i < End; ++i) {
                token.update_lazy();
                fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong));
                if (!pt->lookup(s, &token))
                    fprintf(stderr, "pttrie lookup not found: %llu\n", cvtkey(keyvec[i]));
                if (token.value_of<ullong>() != unaligned_load<ullong>(s.p))
                    fprintf(stderr, "pttrie lookup wrong value: %llu\n", cvtkey(keyvec[i]));
            }
        }
        token.release();
    };
    auto patricia_lb = [&](size_t tid, size_t Beg, size_t End) {
        Patricia::Iterator iter(pt);
        for (size_t i = Beg; i < End; ++i) {
            fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong));
            if (!iter.seek_lower_bound(s) || iter.word() != s)
                fprintf(stderr, "pttrie lower_bound failed: %lld\n", cvtkey(keyvec[i]));
            if (iter.value_of<ullong>() != unaligned_load<ullong>(s.p))
                fprintf(stderr, "pttrie lower_bound wrong value: %llu\n", cvtkey(keyvec[i]));
        }
    };
    auto exec_read = [&](std::function<void(size_t,size_t,size_t)> read) {
        valvec<std::thread> thrVec(read_thread_num - 1, valvec_reserve());
        for (size_t i = 0; i < read_thread_num; ++i) {
            size_t Beg = (i + 0) * keyvec.size() / read_thread_num;
            size_t End = (i + 1) * keyvec.size() / read_thread_num;
            if (i < read_thread_num-1)
                thrVec.unchecked_emplace_back([=](){read(i, Beg, End);});
            else
                read(i, Beg, End);
        }
        for (auto& t : thrVec) t.join();
    };
	auto pt_write = [&](MainPatricia* ptrie, size_t tnum) {
		auto fins = [&](size_t tid) {
            auto& ptoken = ptrie->tls_writer_token();
            //assert(ptoken->get() == nullptr);
            if (ptoken.get() == nullptr) {
                // user may extends WriterToken and override init() ...
                ptoken.reset(new Patricia::WriterToken(ptrie));
            }
            Patricia::WriterToken& token = *ptoken;
            size_t beg = keyvec.size() * (tid + 0) / tnum;
            size_t end = keyvec.size() * (tid + 1) / tnum;
            fprintf(stderr, "thread-%03zd: beg = %8zd , end = %8zd , num = %8zd\n", tid, beg, end, end - beg);
            for (size_t i = beg; i < end; ++i) {
                fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong));
                if (ptrie->insert(s, keyvec.data() + i, &token)) {
                    if (!token.value()) {
                        // Patricia run out of maxMem
                        fprintf(stderr
                            , "thread-%02zd write concurrent run out of maxMem = %zd, i = %zd, frag = %zd\n"
                            , tid, maxMem, i, ptrie->mem_frag_size());
                        break;
                    }
                }
            }
		};
		auto finsInterleave = [&](size_t tid) {
			//fprintf(stderr, "thread-%03d: interleave, num = %8zd\n", tid, strVec.size() / tnum);
            auto& ptoken = ptrie->tls_writer_token();
            //assert(ptoken->get() == nullptr);
            if (ptoken.get() == nullptr) {
                // user may extends WriterToken and override init() ...
                ptoken.reset(new Patricia::WriterToken(ptrie));
            }
            Patricia::WriterToken& token = *ptoken;
			for (size_t i = tid, n = keyvec.size(); i < n; i += tnum) {
                fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong));
                if (ptrie->insert(s, keyvec.data() + i, &token)) {
					if (!token.value()) {
						// Patricia run out of maxMem
						fprintf(stderr
							, "thread-%02zd write concurrent run out of maxMem = %zd, i = %zd, frag = %zd\n"
							, tid, maxMem, i, ptrie->mem_frag_size());
						break;
					}
				}
			}
		};
		valvec<std::thread> thrVec(tnum, valvec_reserve());
		for (size_t i = 0; i < tnum; ++i) {
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
	t0 = pf.now(); if (single_thread_write) { pt_write(&trie, 1); };
	t1 = pf.now(); pt_write(&trie2, write_thread_num);
	t2 = pf.now(); if (read_thread_num > 0) { exec_read(patricia_find); }
	t3 = pf.now(); if (read_thread_num > 0) { exec_read(patricia_lb);   }
	t4 = pf.now();
    size_t sumkeylen = keyvec.used_mem_size();
    fprintf(stderr, "numkeys = %f M, sumkeylen = %.6f M, avglen = %zd, maxMem = %.6f MB\n"
        , keyvec.size()/1e6, sumkeylen/1e6, sizeof(ullong), maxMem/1e6
    );
  if (single_thread_write) {
    fprintf(stderr
        , "patricia insert: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, "
          "memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, frag = %7zd (%.2f%%)), "
          "words = %zd, nodes = %zd, fanout = %.3f\n"
        , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), keyvec.size() / pf.uf(t0, t1)
        , trie.mem_size() / 1e6
        , (trie.mem_size() - 8 * trie.num_words()) / 1e6
        , 8 * trie.num_words() / 1e6
        , trie.mem_frag_size(), 100.0*trie.mem_frag_size()/trie.mem_size()
        , trie.num_words(), trie.v_gnode_states()
        , trie.num_words() / double(trie.v_gnode_states() - trie.num_words())
    );
  }
    fprintf(stderr
        , "patricia mt_Add: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, "
          "memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, frag = %7zd (%.2f%%)), "
          "words = %zd, nodes = %zd, fanout = %.3f, speed ratio = %.2f\n"
        , pf.sf(t1,t2), sumkeylen / pf.uf(t1,t2), keyvec.size() / pf.uf(t1,t2)
        , trie2.mem_size() / 1e6
        , (trie2.mem_size() - 8 * trie2.num_words()) / 1e6
        , 8 * trie2.num_words() / 1e6
        , trie2.mem_frag_size(), 100.0*trie2.mem_frag_size()/trie2.mem_size()
        , trie2.num_words(), trie2.v_gnode_states()
        , trie2.num_words() / double(trie2.v_gnode_states() - trie2.num_words())
        , pf.uf(t0, t1) / pf.uf(t1,t2)
    );
	if (read_thread_num > 0) {
		fprintf(stderr
			, "patricia  point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
			, pf.sf(t2,t3), sumkeylen/pf.uf(t2,t3), keyvec.size()/pf.uf(t2,t3)
		);
		fprintf(stderr
			, "patricia  lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, "
              "speed ratio = %6.3f%%(over patricia point)\n"
			, pf.sf(t3,t4), sumkeylen/pf.uf(t3,t4), keyvec.size()/pf.uf(t3,t4)
			, 100.0*(t3-t2)/(t4-t3)
		);
	}
    if (patricia_trie_fname) {
        t0 = pf.now();
        pt->save_mmap(patricia_trie_fname);
        t1 = pf.now();
        fprintf(stderr
            , "patricia   save: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, mem_size = %9.3f M\n"
            , pf.sf(t0, t1), pt->mem_size() / pf.uf(t0, t1), keyvec.size() / pf.uf(t0, t1)
            , pt->mem_size() / 1e6
        );
    }
    if (read_thread_num > 0) {
        t0 = pf.now();
      {
        std::unique_ptr<ADFA_LexIterator> iter(pt->adfa_make_iter());
        bool ok = iter->seek_begin();
        for (size_t i = 0; i < keyvec.size(); ++i) {
            TERARK_IF_DEBUG(fstring s((byte_t*)(keyvec.data() + i), sizeof(ullong)),);
            assert(ok);
            assert(iter->word() == s);
            ok = iter->incr();
        }
        TERARK_UNUSED_VAR(ok);
      }
        t1 = pf.now();
        fprintf(stderr
            , "patricia   iter: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
            , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), keyvec.size() / pf.uf(t0, t1)
        );
    }
    auto stat = pt->trie_stat();
    double sum = stat.sum() / 100.0;
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
        , ms.lazy_free_cnt/1e6
    );
    fprintf(stderr
        , " lazyfreelist  |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , ms.lazy_free_sum/1e6
        , 100.0* ms.lazy_free_sum / ms.used_size
    );
    fprintf(stderr
        , " fragments     |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , pt->mem_frag_size()/1e6
        , 100.0* pt->mem_frag_size() / ms.used_size
    );
    fprintf(stderr
        , " real used     |   mem_sum  | %10.6f M | %9.2f%% |\n"
        , (ms.used_size - ms.lazy_free_sum - pt->mem_frag_size())/1e6
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
        , "               | total fast | %9zd | %10zd | %9.2f%% | %9.2f%% |\n"
        , sum_fast_cnt, sum_fast_size
        , 100.0*sum_fast_size/pt->mem_frag_size()
        , 100.0*sum_fast_size/pt->mem_size()
    );
    fprintf(stderr
        , "               | total huge | %9zd | %10zd | %9.2f%% | %9.2f%% | %9.2f\n"
        , ms.huge_cnt, ms.huge_size
        , 100.0*ms.huge_size/ms.frag_size
        , 100.0*ms.huge_size/pt->mem_size()
        , 100.0*ms.huge_size/ms.huge_cnt
    );
    fprintf(stderr
        , "               | total frag | %9zd | %10zd | %9.2f%% | %9.2f%% |\n"
        , sum_fast_cnt + ms.huge_cnt, ms.frag_size
        , 100.0
        , 100.0* ms.frag_size / pt->mem_size()
    );
    fprintf(stderr
        , "               |   capacity |           | %10zd | %9.2f%% |\n"
        , ms.capacity, 100.0*ms.capacity/ms.used_size
    );
    assert(pt->mem_frag_size() - sum_fast_size == ms.huge_size);
    assert(pt->mem_frag_size() == ms.frag_size);
    assert(sum_fast_size == ms.frag_size - ms.huge_size);
    return 0;
}
