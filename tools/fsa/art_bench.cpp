#include "../../3rdparty/libart/art.h"
#include "../../3rdparty/libart/art.c"
#include <terark/util/mmap.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/profiling.hpp>
#include <terark/valvec.hpp>
#include <thread>

#if defined(__GLIBC__) || defined(__CYGWIN__)
#include <malloc.h>
#endif

int main(int argc, char* argv[]) {
    using namespace terark;
    if (argc < 2) {
        fprintf(stderr, "usage: %s text-file\n", argv[0]);
        return 1;
    }
    size_t thread_num = 1;
    size_t numkeys = 0;
    size_t sumkeylen = 0;
    MmapWholeFile mmap(fstring(argv[1]), true, true);
    art_tree tree;
    art_tree_init(&tree);

    fprintf(stderr, "start..\n");
#define isnewline(c) ('\r' == c || '\n' == c)

    valvec<std::thread> thr(thread_num, valvec_reserve());
    profiling pf;
    auto t0 = pf.now();
    for (size_t tid = 0; tid < thread_num; ++tid) {
        auto insert_tree = [&,tid]() {
            auto mmap_endp = (const byte_t *) mmap.base + mmap.size;
            auto line = (const byte_t *) mmap.base + mmap.size * (size_t(tid) + 0) / thread_num;
            auto endp = (const byte_t *) mmap.base + mmap.size * (size_t(tid) + 1) / thread_num;
            while (endp < mmap_endp && !isnewline(*endp)) endp++;
            if (0 != tid) {
                while (line < endp && !isnewline(*line)) line++;
                while (line < endp &&  isnewline(*line)) line++;
            }
            size_t i = 0;
            size_t sumkeylen1 = 0;
            while (line < endp) {
                const byte_t* next = line;
                while (next < endp && !isnewline(*next)) next++;
                art_insert(&tree, line, next-line, (void*)line);
                sumkeylen1 += next - line;
                while (next < endp && isnewline(*next)) next++;
                line = next;
                i++;
            }
            as_atomic(sumkeylen).fetch_add(sumkeylen1);
            as_atomic(numkeys).fetch_add(i);
        };
        thr.unchecked_emplace_back(std::thread(insert_tree));
    }

    for (auto& t : thr) t.join();

    auto t1 = pf.now();

    fprintf(stderr
            , "write time = %f sec, QPS = %f M op/sec, ThroughPut = %f MB/sec\n"
            , pf.sf(t0, t1), numkeys/pf.uf(t0, t1), sumkeylen/pf.uf(t0, t1)
            );

#if defined(__GLIBC__) || defined(__CYGWIN__)
    malloc_stats();
#endif

    art_tree_destroy(&tree);
    return 0;
}

