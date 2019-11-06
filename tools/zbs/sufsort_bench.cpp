//
// Created by leipeng on 2019-11-04.
//
#include <terark/fstring.hpp>
#include <terark/util/stat.hpp>
#include <terark/util/hugepage.hpp>
#include <terark/util/throw.hpp>
#include <terark/zbs/sufarr_inducedsort.h>
#include <zstd/dictBuilder/divsufsort.h>

//Makefile:CXXFLAGS:-I../../3rdparty/zstd

namespace terark {
    extern int g_useDivSufSort;
}
using namespace terark;

int main(int argc, char* argv[])
try {
    bool openMP = getEnvBool("use_openmp", 0);
    valvec<byte_t> mem;
    size_t fsize = 0;
    {
        struct ll_stat st;
        if (::ll_fstat(0, &st) < 0) {
            THROW_STD(runtime_error, "fstat failed");
        }
        fsize = st.st_size;
    }
    use_hugepage_resize_no_init(&mem, pow2_align_up(fsize, 8)*5);
    auto rdsize = ::read(0, mem.data(), fsize);
    if (size_t(rdsize) != fsize) {
        THROW_STD(runtime_error, "ERROR: read(stdin, %zd) = %zd : err = %s\n", fsize, rdsize, strerror(errno));
    }
    int* sufarr = (int*)(mem.data() + pow2_align_up(fsize, 8));
	if (g_useDivSufSort == 1)
		divsufsort(mem.data(), sufarr, fsize, openMP);
	else
		sufarr_inducedsort(mem.data(), sufarr, fsize);

    return 0;
}
catch (...) {
    fprintf(stderr, "exit 1 on exception\n");
    return 1;
}

