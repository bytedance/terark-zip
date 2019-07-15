#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif

#include <terark/fsa/cspptrie.inl>
#include <terark/fsa/nest_trie_dawg.hpp>
#ifdef __GNUC__
  #include <terark/fsa/nest_louds_trie_inline.hpp>
#endif
#include <terark/util/autoclose.hpp>
#include <terark/util/base64.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/stat.hpp>
#include <getopt.h>
#include <fcntl.h>
#include <map>
#include <random>
#include <thread>

using namespace terark;

void usage(const char* prog) {
    fprintf(stderr, R"EOS(Usage: %s Options Input-TXT-File
Options:
    -h Show this help information
    -m MaxMem
    -o Output-Trie-File
    -g Output-Graphviz-Dot-File
    -i Condurrent write interleave
    -j Mark readonly for read
    -r Reader Thread Num
    -t Writer Thread Num
    -w Writer ConcurrentLevel
    -s print stat
    -b BenchmarkLoop : Run benchmark
    -B Input is binary(bson) data
    -6 Input is base64 encoded data
If Input-TXT-File is omitted, use stdin
)EOS", prog);
    exit(1);
}

size_t benchmarkLoop = 0;
size_t maxMem = 0;
bool b_write_dot_file = false;
bool is_binary_input = false;
bool is_base64_input = false;
const char* bench_input_fname = NULL;
const char* patricia_trie_fname = NULL;

int nth_dot = 0;

void debug_print(const Patricia& trie) {
    char dot_name[64];
    snprintf(dot_name, 64, "pat-trie-%03d.dot", nth_dot++);
    trie.write_dot_file(dot_name);
    int level = 0;
    std::string ws;
    auto indent = "                                                                   ";
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    std::function<void(size_t child, auchar_t ch)>
    op = [&](size_t child, auchar_t ch) {
        fstring zs;
        if (trie.v_is_pzip(child)) {
            zs = trie.v_get_zpath_data(child, NULL);
            ws.append(zs.data(), zs.size());
        }
        ws.push_back(ch);
        int wn = 0;
        if (trie.v_is_term(child)) {
            wn = ws.size();
        }
        printf("%.*s%02X -> %6zd zn=%03zd: %.*s   ---- %.*s\n"
            , level*2, indent, ch, child
            , zs.size(), zs.ilen(), zs.data(), wn, ws.c_str());
        level++;
        trie.v_for_each_move(child, op);
        level--;
        ws.resize(ws.size() - zs.size() - 1);
    };
    trie.v_for_each_move(initial_state, op);
}

int main(int argc, char* argv[]) {
    int write_thread_num = 0;
    int read_thread_num = 1;
    bool mark_readonly = false;
    bool print_stat = false;
    bool concWriteInterleave = false;
    auto conLevel = Patricia::SingleThreadStrict;
    for (;;) {
        int opt = getopt(argc, argv, "Bb:ghm:o:6:t:w:r:ijs");
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
        case 'g':
            b_write_dot_file = true;
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
        case 'r':
            read_thread_num = std::max(1, atoi(optarg));
            break;
        case 'j':
            mark_readonly = true;
            break;
        case 's':
            print_stat = true;
            break;
        case '?':
        case 'h':
        default:
            usage(argv[0]);
        }
    }
GetoptDone:
    if (NULL == patricia_trie_fname) {
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
    if (0 == maxMem) {
        struct ll_stat st;
        int err = ::ll_fstat(fileno(fp.self_or(stdin)), &st);
        if (err) {
            fprintf(stderr, "ERROR: fstat failed = %s\n", strerror(errno));
        }
        else if (S_ISREG(st.st_mode)) { // OK, get file size
            maxMem = 2*st.st_size;
        }
    }
    SortableStrVec strVec;
    MainPatricia trie(sizeof(size_t), maxMem, conLevel);
    MainPatricia trie2(sizeof(size_t), maxMem, conLevel);
    size_t sumkeylen = 0;
    long long t0 = pf.now();
    if (is_binary_input) {
        size_t nth = 0;
        valvec<char> buf;
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
            buf.resize_no_init(reclen-4);
            long datalen = fread(buf.data(), 1, reclen-4, fp);
            if (datalen != reclen-4) {
                fprintf(stderr, "read binary data error: nth=%zd requested=%ld returned=%ld\n"
                        , nth, long(reclen-4), datalen);
                break;
            }
            Patricia::WriterToken token(&trie);
            if (trie.insert(buf, &nth, &token)) {
                nth++;
            } else {
                fprintf(stderr, "dup binary key, len = %zd\n", buf.size());
            }
            if (token.value() == NULL) { // run out of maxMem
                fprintf(stderr
                    , "write concurrent run out of maxMem = %zd, fragments = %zd (%.2f%%)\n"
                    , maxMem, trie.mem_frag_size(), 100.0*trie.mem_frag_size()/trie.mem_size());
            }
        }
    }
    else if (is_base64_input) {
        LineBuf line;
        valvec<char> key;
    //  valvec<char> enc;
        while (line.getline(fp.self_or(stdin)) > 0) {
            line.chomp();
            terark::base64_decode(line.p, line.n, &key);
            fprintf(stderr, "base64 is not supported\n");
            return 1;
        }
    }
    else {
        LineBuf line;
        while (line.getline(fp.self_or(stdin)) > 0) {
            line.chomp();
            if (line.empty()) {
                fprintf(stderr, "empty line\n");
            }
            sumkeylen += line.n;
            strVec.push_back(line);
#if 0
            Patricia::WriterToken token(&trie);
            if (trie.insert(line, &nth, &token)) {
                if (token.value()) {
                    nth++;
#if 0
                    trie.print_output(stdout);
                    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                    debug_print(trie);
                    printf("-------------------------------\n");
                    strVec.sort();
                    trie.for_each_word([&](size_t i, fstring w1) {
                        fstring w0 = strVec[i];
                        assert(w1 == w0);
                    });
#endif
                } else { // Patricia run out of maxMem
                    fprintf(stderr
                        , "write concurrent run out of maxMem = %zd, fragments = %zd\n"
                        , maxMem, trie.mem_frag_size());
                    break;
                }
            } else {
                fprintf(stderr, "dup key, len = %03zd: %.*s\n"
                              , line.size(), (int)line.n, line.p);
            }
#endif
        }
    }
    long long t1 = pf.now();
    fprintf(stderr
        , "read      input: time = %8.3f sec, %8.3f MB/sec, avglen = %8.3f\n"
        , pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1), strVec.avg_size()
    );
    t0 = pf.now();
    valvec<size_t> randvec(strVec.size(), valvec_no_init());
    for (size_t i = 0; i < randvec.size(); ++i) randvec[i] = i;
    shuffle(randvec.begin(), randvec.end(), std::mt19937_64());
    t1 = pf.now();
    fprintf(stderr
        , "generate  shuff: time = %8.3f sec, %8.3f MB/sec\n"
        , pf.sf(t0,t1), randvec.used_mem_size()/pf.uf(t0,t1)
    );
    t0 = pf.now();
    fstrvecll fstrVec;
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
    for(size_t i = 0; i < strVec.size(); ++i) {
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
    for(size_t i = 0; i < strVec.size(); ++i)
        fstrVec.push_back(strVec[i]);
  }
    t1 = pf.now();
    fprintf(stderr
        , "fstrVec  append: time = %8.3f sec, %8.3f MB/sec\n"
        , pf.sf(t0,t1), sumkeylen/pf.uf(t0,t1)
    );
    long long t_append_fstrVec = t1 - t0;
    std::map<std::string, size_t> stdmap;
    auto patricia_find = [&](int tid, size_t Beg, size_t End) {
        Patricia::ReaderToken token(&trie);
        if (mark_readonly) {
            for (size_t i = Beg; i < End; ++i) {
                fstring s = fstrVec[i];
                if (!trie.lookup(s, &token))
                    fprintf(stderr, "pttrie not found: %.*s\n", s.ilen(), s.data());
            }
        }
        else {
            for (size_t i = Beg; i < End; ++i) {
                token.update_lazy();
                fstring s = fstrVec[i];
                if (!trie.lookup(s, &token))
                    fprintf(stderr, "pttrie not found: %.*s\n", s.ilen(), s.data());
            }
        }
    };
    auto patricia_lb = [&](int tid, size_t Beg, size_t End) {
        char iter_mem[Patricia::ITER_SIZE];
        trie.construct_iter(iter_mem);
        auto& iter = *reinterpret_cast<Patricia::Iterator*>(iter_mem);
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            if (!iter.seek_lower_bound(s))
                fprintf(stderr, "pttrie lower_bound failed: %.*s\n", s.ilen(), s.data());
        }
        iter.~Iterator();
    };
    auto stdmap_find = [&](int tid, size_t Beg, size_t End) {
        std::string strkey;
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            strkey.assign(s.p, s.n);
            auto iter = stdmap.find(strkey);
            if (stdmap.end() == iter) {
                fprintf(stderr, "stdmap not found: %.*s\n", s.ilen(), s.data());
            }
        }
    };
    auto stdmap_lb = [&](int tid, size_t Beg, size_t End) {
        std::string strkey;
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            strkey.assign(s.p, s.n);
            auto iter = stdmap.lower_bound(strkey);
            if (stdmap.end() == iter) {
                fprintf(stderr, "stdmap lower_bound fail: %.*s\n", s.ilen(), s.data());
            }
        }
    };
    auto strVec_find = [&](int tid, size_t Beg, size_t End) {
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            size_t  j = strVec.find(s);
            if (strVec.size() == j)
                fprintf(stderr, "strVec not found: %.*s\n", s.ilen(), s.data());
        }
    };
    auto strVec_lb = [&](int tid, size_t Beg, size_t End) {
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            size_t  j = strVec.lower_bound(s);
            if (strVec.size() == j)
                fprintf(stderr, "strVec lower_bound failed: %.*s\n", s.ilen(), s.data());
        }
    };
    auto exec_read = [&](std::function<void(int,size_t,size_t)> read) {
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
    t0 = pf.now(); // patricia insert
  {
    size_t nth = 0;
    for (size_t i = 0; i < strVec.size(); ++i) {
        fstring s = strVec[i];
        Patricia::WriterToken token(&trie);
        if (trie.insert(s, &nth, &token)) {
            if (token.value()) {
                nth++;
            } else { // Patricia run out of maxMem
                fprintf(stderr
                    , "write single thread run out of maxMem = %zd, i = %zd, fragments = %zd (%.2f%%)\n"
                    , maxMem, i, trie.mem_frag_size(), 100.0*trie.mem_frag_size()/trie.mem_size());
                break;
            }
        }
    }
    if (mark_readonly)
        trie.set_readonly();
  }
    t1 = pf.now(); // std::map.insert
  {
    size_t nth = 0;
    for (size_t i = 0; i < strVec.size(); ++i) {
        fstring s = strVec[i];
        auto ib = stdmap.insert(std::make_pair(std::string(s.p, s.n), nth));
        if (ib.second) {
            nth++;
        }
    }
  }
    long long t2 = pf.now(); exec_read(patricia_find);
    long long t3 = pf.now(); exec_read(patricia_lb);
    long long t4 = pf.now(); stdmap.clear();
    long long t5 = pf.now(); // std::map.append
    for (size_t i = 0; i < strVec.size(); ++i) {
        fstring s = strVec[i];
        stdmap.insert(stdmap.end(), std::make_pair(std::string(s.p, s.n), i));
    }
    long long t6 = pf.now(); // patricia concurrent write
    if (write_thread_num > 0) {
        auto fins = [&](int tid, size_t beg, size_t end) {
            fprintf(stderr, "thread-%02d: beg = %8zd , end = %8zd , num = %8zd\n", tid, beg, end, end - beg);
            Patricia::WriterToken token(&trie2);
            for (size_t i = beg; i < end; ++i) {
                fstring s = strVec[i];
                if (trie2.insert(s, &i, &token)) {
                    if (!token.value()) {
                        // Patricia run out of maxMem
                        fprintf(stderr
                            , "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                            , tid, maxMem, i, trie2.mem_frag_size());
                        break;
                    }
                }
            }
        };
        auto finsInterleave = [&](int tid) {
            size_t tnum = write_thread_num;
            fprintf(stderr, "thread-%02d: interleave, num = %8zd\n", tid, strVec.size()/tnum);
            Patricia::WriterToken token(&trie2);
            for (size_t i = tid, n = strVec.size(); i < n; i += tnum) {
                fstring s = strVec[i];
                if (trie2.insert(s, &i, &token)) {
                    if (!token.value()) {
                        // Patricia run out of maxMem
                        fprintf(stderr
                            , "thread-%02d write concurrent run out of maxMem = %zd, i = %zd, fragments = %zd\n"
                            , tid, maxMem, i, trie2.mem_frag_size());
                        break;
                    }
                }
            }
        };
        valvec<std::thread> thrVec(write_thread_num, valvec_reserve());
        for (int i = 0; i < write_thread_num; ++i) {
            size_t beg = strVec.size() * (i + 0) / write_thread_num;
            size_t end = strVec.size() * (i + 1) / write_thread_num;
            if (concWriteInterleave)
                thrVec.unchecked_emplace_back(std::bind(finsInterleave, i));
            else
                thrVec.unchecked_emplace_back(std::bind(fins, i, beg, end));
        }
        for (int i = 0; i < write_thread_num; ++i) {
            thrVec[i].join();
        }
        if (mark_readonly)
            trie2.set_readonly();
    }
    long long t7 = pf.now(); exec_read(stdmap_find);
    long long t8 = pf.now(); exec_read(stdmap_lb);
    long long t9 = pf.now(); strVec.sort();
    long long ta = pf.now(); exec_read(strVec_find);
    long long tb = pf.now(); exec_read(strVec_lb);
    long long tc = pf.now();
    fprintf(stderr
        , "patricia insert: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, fragments = %7zd (%.2f%%)), words = %zd, nodes = %zd, fanout = %.3f\n"
        , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), strVec.size() / pf.uf(t0, t1)
        , trie.mem_size() / 1e6, (trie.mem_size() - 8 * stdmap.size()) / 1e6, 8 * stdmap.size() / 1e6
        , trie.mem_frag_size(), 100.0*trie.mem_frag_size()/trie.mem_size()
        , trie.num_words(), trie.v_gnode_states()
        , trie.num_words() / double(trie.v_gnode_states() - trie.num_words())
    );
if (write_thread_num > 0) {
    fprintf(stderr
        , "patricia mt_Add: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M, fragments = %7zd (%.2f%%)), words = %zd, nodes = %zd, fanout = %.3f, speed ratio = %.2f\n"
        , pf.sf(t6, t7), sumkeylen / pf.uf(t6, t7), strVec.size() / pf.uf(t6, t7)
        , trie2.mem_size() / 1e6, (trie2.mem_size() - 8 * stdmap.size()) / 1e6, 8 * stdmap.size() / 1e6
        , trie2.mem_frag_size(), 100.0*trie2.mem_frag_size()/trie2.mem_size()
        , trie2.num_words(), trie2.v_gnode_states()
        , trie2.num_words() / double(trie2.v_gnode_states() - trie2.num_words())
        , pf.uf(t0, t1) / pf.uf(t6, t7)
    );
}
    fprintf(stderr
        , "std::map insert: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M), speed ratio = %6.3f%%, mem ratio = %7.3f%%\n"
        , pf.sf(t1, t2), sumkeylen / pf.uf(t1, t2),  strVec.size() / pf.uf(t1, t2)
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4 + sizeof(size_t))) / 1e6
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4)) / 1e6
        , 8 * stdmap.size() / 1e6
        , 100.0*(t1-t0)/(t2-t1)
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4 + sizeof(size_t))) * 100.0 / trie.mem_size()
    );
    fprintf(stderr
        , "std::map append: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M), speed ratio = %6.3f%%\n"
        , pf.sf(t5, t6), sumkeylen / pf.uf(t5, t6),  strVec.size() / pf.uf(t5, t6)
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4 + sizeof(size_t))) / 1e6
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4)) / 1e6
        , 8 * stdmap.size() / 1e6
        , 100.0*(t1-t0)/(t6-t5)
    );
    fprintf(stderr
        , "std::map  clear: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, memory(sum = %8.3f M, key = %8.3f M, val = %8.3f M), speed ratio = %6.3f%%\n"
        , pf.sf(t4, t5), sumkeylen / pf.uf(t4, t5), strVec.size() / pf.uf(t4, t5)
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4 + sizeof(size_t))) / 1e6
        , (sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4)) / 1e6
        , 8 * stdmap.size() / 1e6
        , 100.0*(t1-t0)/(t5-t4)
    );
    fprintf(stderr
        , "strVec     sort: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia insert)\n"
        , pf.sf(t9, ta), sumkeylen / pf.uf(t9, ta), strVec.size() / pf.uf(t9, ta)
        , 100.0*(t1-t0)/(ta-t9)
    );
    ta += t_append_fstrVec;
    fprintf(stderr
        , "strVec wrt+sort: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia insert)\n"
        , pf.sf(t9, ta), sumkeylen / pf.uf(t9, ta), strVec.size() / pf.uf(t9, ta)
        , 100.0*(t1-t0)/(ta-t9)
    );
    ta -= t_append_fstrVec;
    fprintf(stderr
        , "patricia  point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
        , pf.sf(t2,t3), sumkeylen/pf.uf(t2,t3), strVec.size()/pf.uf(t2,t3)
    );
    fprintf(stderr
        , "patricia  lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia point)\n"
        , pf.sf(t3,t4), sumkeylen/pf.uf(t3,t4), strVec.size()/pf.uf(t3,t4)
        , 100.0*(t3-t2)/(t4-t3)
    );
    fprintf(stderr
        , "std::map  point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia point)\n"
        , pf.sf(t7,t8), sumkeylen/pf.uf(t7,t8), strVec.size()/pf.uf(t7,t8)
        , 100.0*(t3-t2)/(t8-t7)
    );
    fprintf(stderr
        , "std::map  lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia lower)\n"
        , pf.sf(t8,t9), sumkeylen/pf.uf(t8,t9), strVec.size()/pf.uf(t8,t9)
        , 100.0*(t4-t3)/(t9-t8)
    );
    fprintf(stderr
        , "strVec    point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia point)\n"
        , pf.sf(ta,tb), sumkeylen/pf.uf(ta,tb), strVec.size()/pf.uf(ta,tb)
        , 100.0*(t3-t2)/(tb-ta)
    );
    fprintf(stderr
        , "strVec    lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia lower)\n"
        , pf.sf(tb,tc), sumkeylen/pf.uf(tb,tc), strVec.size()/pf.uf(tb,tc)
        , 100.0*(t4-t3)/(tc-tb)
    );
    long long tt0 = t0, tt1 = t1;
    t0 = pf.now();
    MainPatricia* pt = trie2.num_words() ? &trie2 : &trie;
    pt->save_mmap(patricia_trie_fname);
    t1 = pf.now();
    fprintf(stderr
        , "patricia   save: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, mem_size = %9.3f M\n"
        , pf.sf(t0, t1), pt->mem_size() / pf.uf(t0, t1), strVec.size() / pf.uf(t0, t1)
        , pt->mem_size() / 1e6
    );
    t0 = pf.now();
    //pt->print_output((std::string(patricia_trie_fname) + ".txt").c_str());
    t1 = pf.now();
    fprintf(stderr
        , "patricia  print: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
        , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), strVec.size() / pf.uf(t0, t1)
    );
    t0 = pf.now();
/*
    pt->for_each_word([&](size_t nth, fstring w) {
#if !defined(NDEBUG)
        fstring s = strVec[nth];
        if (w != s) {
            printf("nth=%zd, w.size=%zd: %.*s\n", nth, w.size(), w.ilen(), w.data());
            printf("nth=%zd, s.size=%zd: %.*s\n", nth, s.size(), s.ilen(), s.data());
        }
        //assert(w == ws);
#endif
    });
*/
    t1 = pf.now();
    fprintf(stderr
        , "patricia   each: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M\n"
        , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), strVec.size() / pf.uf(t0, t1)
    );
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
        , pf.sf(t0, t1), sumkeylen / pf.uf(t0, t1), strVec.size() / pf.uf(t0, t1)
    );
    fprintf(stderr, "NestLouds test...\n");
    typedef NestLoudsTrieDAWG_Mixed_XL_256_32_FL  nlt_t;
    nlt_t  nlt;
    NestLoudsTrieConfig conf;
    auto nlt_find = [&](int tid, size_t Beg, size_t End) {
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            size_t  j = nlt.index(s);
            if (size_t(-1) == j)
                fprintf(stderr, "nlt not found: %.*s\n", s.ilen(), s.data());
        }
    };
#if 0
    #define DECL_NLT_ITER nlt_t::Iterator nltIter(&nlt)
#else
    #define DECL_NLT_ITER \
        std::unique_ptr<ADFA_LexIterator> uNltIter(nlt.adfa_make_iter()); \
        auto& nltIter = *uNltIter
#endif
    auto nlt_lb = [&](int tid, size_t Beg, size_t End) {
        DECL_NLT_ITER;
        for (size_t i = Beg; i < End; ++i) {
            fstring s = fstrVec[i];
            if (!nltIter.seek_lower_bound(s))
                fprintf(stderr, "nlt lower_bound failed: %.*s\n", s.ilen(), s.data());
        }
    };
    std::swap(t0, tt0);
    std::swap(t1, tt1);
    long long t_append_and_sort = ta - t9;
    long long td;
    conf.isInputSorted = true;
    ta = pf.now();  nlt.build_from(strVec, conf);
    tb = pf.now();  exec_read(nlt_find);
    tc = pf.now();  exec_read(nlt_lb);
    td = pf.now();
  {
    DECL_NLT_ITER;
    bool ok = nltIter.seek_begin();
    TERARK_IF_DEBUG(size_t cnt = 0,);
    while (ok) {
        TERARK_IF_DEBUG(cnt++,);
        ok = nltIter.incr();
    }
  }
    long long te = pf.now();
    fprintf(stderr
        , "NestLoudsMemory: %8.3f MB, ratio over patricia = %6.3f%%, ratio over strVec = %6.3f%%, ratio over stdmap = %6.3f%%\n"
        , 1.0*nlt.mem_size()/1e6
        , 100.0*nlt.mem_size()/trie.mem_size()
        , 100.0*nlt.mem_size()/(sumkeylen + 8*fstrVec.size())
        , 100.0*nlt.mem_size()/(sumkeylen + stdmap.size() * (sizeof(std::string) + sizeof(void*) * 4))
    );
    fprintf(stderr
        , "NestLouds build: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia build)\n"
        , pf.sf(ta,tb), sumkeylen/pf.uf(ta,tb), fstrVec.size()/pf.uf(ta,tb)
        , 100.0*(t1-t0)/(tb-ta)
    );
    tb += t_append_and_sort;
    fprintf(stderr
        , "NestLouds BUILD: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia build) - with sort\n"
        , pf.sf(ta,tb), sumkeylen/pf.uf(ta,tb), fstrVec.size()/pf.uf(ta,tb)
        , 100.0*(t1-t0)/(tb-ta)
    );
    tb -= t_append_and_sort;
    fprintf(stderr
        , "NestLouds point: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia point)\n"
        , pf.sf(tb,tc), sumkeylen/pf.uf(tb,tc), fstrVec.size()/pf.uf(tb,tc)
        , 100.0*(t3-t2)/(tc-tb)
    );
    fprintf(stderr
        , "NestLouds lower: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia lower)\n"
        , pf.sf(tc,td), sumkeylen/pf.uf(tc,td), fstrVec.size()/pf.uf(tc,td)
        , 100.0*(t4-t3)/(td-tc)
    );
    fprintf(stderr
        , "NestLouds  iter: time = %8.3f sec, %8.3f MB/sec, QPS = %8.3f M, speed ratio = %6.3f%%(over patricia  iter)\n"
        , pf.sf(td,te), sumkeylen/pf.uf(td,te), fstrVec.size()/pf.uf(td,te)
        , 100.0*(tt1-tt0)/(te-td)
    );

    auto stat = trie.trie_stat();
    double sum = stat.sum() / 100.0;
    fprintf(stderr, "fstrVec    size: %8zd\n", fstrVec.size());
    fprintf(stderr, "std::map   size: %8zd\n", stdmap.size());
    fprintf(stderr, "patricia   size: %8zd\n", trie.num_words());
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
    fprintf(stderr, "------------------------------------------------------------------------\n");
    fprintf(stderr, "mpool  fastbin | block size | entry num | total size | size ratio |\n");
    auto ms = trie.mem_get_stat();
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
        , 100.0*sum_fast_size/trie.mem_frag_size()
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
    assert(trie.mem_frag_size() - sum_fast_size == ms.huge_size);
    assert(trie.mem_frag_size() == ms.frag_size);
    assert(sum_fast_size == ms.frag_size - ms.huge_size);
    return 0;
}
