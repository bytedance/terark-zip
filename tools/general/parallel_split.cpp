//
// Created by leipeng on 2019-05-27.
//
#include <terark/util/mmap.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/valvec.hpp>
#include <thread>
#include <fcntl.h>

using namespace terark;

// write system call can not write more than 2G
void fuck_write(size_t tid, int fd, const void* vbuf, size_t len) {
    const char* pbuf = (const char*)vbuf;
    size_t remain = len;
    while (remain) {
        intptr_t len1 = std::min(len, size_t(1)<<30);
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

const byte_t* adjust_bondary(const byte_t* ptr, const byte_t* end) {
#define isnewline(c) ('\n' == c || '\r' == c)
    while (ptr < end && !isnewline(*ptr)) ++ptr;
    while (ptr < end &&  isnewline(*ptr)) ++ptr;
    return ptr;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s input output1 output2 ...\n", argv[0]);
        return 1;
    }
    const char* fname = argv[1];
    try {
        MmapWholeFile mmap(fname);
        valvec<std::thread> thrVec(argc - 2, valvec_reserve());
        size_t ptlen = mmap.size / thrVec.capacity();
        auto finish = (const byte_t*)mmap.base + mmap.size;
        int ret = 0; // success
        auto tfunc = [&](size_t tid) {
            const byte_t* beg = (const byte_t*)mmap.base + ptlen * tid;
            const byte_t* end = beg + ptlen;
            if (0 != tid) {
                beg = adjust_bondary(beg, end);
            }
            end = adjust_bondary(end, finish);
            const char* ofname = argv[2 + tid];
            int fd = open(ofname, O_WRONLY|O_CREAT, 0600);
            if (fd < 0) {
                ret = errno;
                fprintf(stderr, "ERROR: open(%s, O_WRONLY|O_CREAT) = %s\n", ofname, strerror(ret));
                return;
            }
            fuck_write(tid, fd, beg, end - beg);
            close(fd);
        };
        for (size_t i = 0; i < thrVec.capacity(); ++i) {
            thrVec.unchecked_emplace_back([=](){tfunc(i);});
        }
        assert(thrVec.size() == thrVec.capacity());
        for (auto& t : thrVec) {
            t.join();
        }
        return ret;
    }
    catch (const std::exception& ex) {
        fprintf(stderr, "ERROR: %s\n", ex.what());
    }
    return 0;
}
