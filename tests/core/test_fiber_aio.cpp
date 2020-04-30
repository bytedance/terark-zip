#include <terark/thread/fiber_aio.hpp>
#include <terark/fstring.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/profiling.hpp>
#include <random>
#include <thread>
#include <vector>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main() {
    const char* fname = "fiber_aio.test.bin";
    int flags = O_CLOEXEC|O_CREAT|O_RDWR;
  #if defined(O_DIRECT)
    flags |= O_DIRECT;
  #endif
    int fd = open(fname, flags, 0600);
    if (fd < 0) {
        fprintf(stderr, "ERROR: open(%s) = %s\n", fname, strerror(errno));
        return 1;
    }
  #if defined(F_NOCACHE)
    if (fcntl(fd, F_SETFD, flags|F_NOCACHE) < 0) {
        fprintf(stderr, "ERROR: fcntl(%d -> '%s') = %s\n", fd, fname, strerror(errno));
        return 1;
    }
  #endif
    using namespace terark;
    const intptr_t FileSize = ParseSizeXiB(getenv("FileSize"), 2L << 20); // default 2M
    const intptr_t WriteSize = ParseSizeXiB(getenv("WriteSize"), FileSize*10);
    const intptr_t ReadSize = ParseSizeXiB(getenv("ReadSize"), FileSize*10);
    const intptr_t Threads = getEnvLong("Threads", 4);
    const intptr_t BlockSize = ParseSizeXiB(getenv("BlockSize"), 4096);
    if (BlockSize < 512 || (BlockSize & (BlockSize-1)) != 0) {
        fprintf(stderr, "Invalid BlockSize = %zd\n", BlockSize);
        return 1;
    }
    if (Threads <= 0) {
        fprintf(stderr, "Invalid Threads = %zd\n", Threads);
        return 1;
    }
    fprintf(stderr, "Threads=%zd, BlockSize=%zd, FileSize=%zd, ReadSize=%zd, WriteSize=%zd\n",
        Threads, BlockSize, FileSize, ReadSize, WriteSize);
    ftruncate(fd, FileSize);
    intptr_t allsum = 0;
    auto thr_fun = [&](intptr_t tno) {
        void* buf = NULL;
        int err = posix_memalign(&buf, BlockSize, BlockSize);
        if (err) {
            fprintf(stderr,
                "ERROR: tno = %zd, posix_memalign(%zd) = %s\n",
                tno, BlockSize, strerror(err));
            exit(1);
        }
        std::mt19937_64 rand;
        intptr_t sum = 0;
        while (sum < WriteSize / Threads) {
            intptr_t offset = rand() % FileSize & -BlockSize;
            intptr_t n = fiber_put_write(fd, buf, BlockSize, offset);
            if (n != BlockSize) {
                fprintf(stderr,
                    "ERROR: fiber_put_wirte(offset = %zd, len = %zd) = %zd : %s\n",
                    offset, BlockSize, n, strerror(errno));
                exit(1);
            }
            sum += BlockSize;
        }
        as_atomic(allsum) += sum;
    };
    std::vector<std::thread> tv;
    profiling pf;
    long long t0 = pf.now();
    for (intptr_t tno = 0; tno < Threads; ++tno) {
        tv.emplace_back(thr_fun, tno);
    }
    for (std::thread& t : tv) {
        t.join();
    }
    long long t1 = pf.now();

    close(fd);
    remove(fname);

    fprintf(stderr, "wr time = %8.3f sec\n", pf.sf(t0,t1));
    fprintf(stderr, "wr iops = %8.3f K\n", WriteSize/BlockSize/pf.mf(t0,t1));
    fprintf(stderr, "wr iobw = %8.3f GiB\n", WriteSize/pf.sf(t0,t1)/(1L<<30));
    return 0;
}
