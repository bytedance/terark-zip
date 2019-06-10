//
// Created by leipeng on 2019-05-27.
//
#include <terark/util/mmap.hpp>
#include <terark/valvec.hpp>
#include <mutex>
#include <fcntl.h>
#include <getopt.h>

using namespace terark;

// write system call can not write more than 2G
void fuck_write(size_t tid, int fd, const void* vbuf, size_t len) {
    const char* pbuf = (const char*)vbuf;
    size_t remain = len;
    while (remain) {
        intptr_t len1 = std::min(remain, size_t(1)<<30);
        intptr_t len2 = write(fd, pbuf, len1);
        if (len2 != len1) {
            int err = errno;
            if (err || len2 < 0) {
                fprintf(stderr, "ERROR: tid = %zd, write(fd=%d, len=%zd) = %zd : %s\n"
                              , tid, fd, len1, len2, strerror(err));
                exit(err);
            }
            else {
                // may interrupted by an harmless signal such as SIGSTOP
            }
        }
        remain -= len2;
        pbuf += len2;
    }
}

int usage(const char* prog) {
    fprintf(stderr, "usage: %s input output1 output2 ...\n", prog);
    fprintf(stderr, "  -- or\n");
    fprintf(stderr, "usage: %s -l num_threads input\n", prog);
    return 1;
}

int main(int argc, char* argv[]) {
    intptr_t num_threads_for_list = 0;
    for (;;) {
        int opt = getopt(argc, argv, "l:");
        switch (opt) {
        case 'l':
            num_threads_for_list = atoi(optarg);
            if (num_threads_for_list <= 0) {
                return usage(argv[0]);
            }
            break;
        case '?':
            return usage(argv[0]);
		case -1:
			goto GetoptDone;
        }
    }
GetoptDone:
    if (argc < optind + 2) {
        return usage(argv[0]);
    }
    const char* fname = argv[optind];
    try {
        MmapWholeFile mmap(fname);
        if (num_threads_for_list) {
            valvec<std::pair<size_t, size_t> >
                    poslen(num_threads_for_list, valvec_reserve());
            std::mutex mtx;
            mmap.parallel_for_lines(num_threads_for_list,
            [&](size_t, byte_t* beg, byte_t* end) {
                mtx.lock();
                poslen.emplace_back(beg - (byte_t*)mmap.base, end-beg);
                mtx.unlock();
            });
            std::sort(poslen.begin(), poslen.end());
            for (intptr_t i = 0; i < num_threads_for_list; ++i) {
                printf("%zd\t%zd\t%zd\n", i, poslen[i].first, poslen[i].second);
            }
            return 0;
        }
        int ret = 0; // success
        mmap.parallel_for_lines(argc-2,
                [&](size_t tid, byte_t* beg, byte_t* end) {
            const char* ofname = argv[2 + tid];
            int fd = open(ofname, O_WRONLY|O_CREAT, 0600);
            if (fd < 0) {
                ret = errno;
                fprintf(stderr,
                        "ERROR: tid = %zd, open(%s, O_WRONLY|O_CREAT) = %s\n",
                        tid, ofname, strerror(ret));
                return;
            }
            fuck_write(tid, fd, beg, end - beg);
            close(fd);
        });
        return ret;
    }
    catch (const std::exception& ex) {
        fprintf(stderr, "ERROR: %s\n", ex.what());
    }
    return 0;
}
