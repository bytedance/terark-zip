//
// Created by leipeng on 2019-08-09.
//

#include <terark/util/fiber_pool.hpp>

#include <vector>
#include <stdio.h>
#include <terark/num_to_str.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>
#include <terark/fstring.hpp>

int main(int argc, char* argv[]) {
    using namespace terark;
    RunOnceFiberPool fp1( 8);
    RunOnceFiberPool fp2(64);

    profiling pf;
    size_t cnt = 0;
    size_t per_fp2 = fp2.capacity()/fp1.capacity();
    size_t loop_cnt = getEnvLong("loop_cnt", 10000);
    long long t0, t1;
    int h1 = -1;

    t0 = pf.now();
    for (size_t i = 0; i < loop_cnt; ++i) {
        fp2.async(h1, [&](){cnt++;});
    }
    fp2.reap(h1);
    t1 = pf.now();
    printf("test_fiber_pool-async : time = %f sec, cnt = %zd, ops = %f M/sec, latency = %f ns\n",
            pf.sf(t0,t1), cnt, cnt/pf.uf(t0,t1), pf.nf(t0,t1)/cnt);

    cnt = 0;
    h1 = -1;
    t0 = pf.now();
    for (size_t i = 0; i < loop_cnt; ++i) {
        fp1.submit(h1, [&]() {
        int h2 = -1;
        for (size_t i = 0; i < per_fp2; ++i) {
            fp2.submit(h2, [&]() {
                cnt++;
            });
        }
        fp2.reap(h2);
    });
    }
    fp1.reap(h1);
    t1 = pf.now();

    printf("test_fiber_pool-submit: time = %f sec, cnt = %zd, ops = %f M/sec, latency = %f ns\n",
            pf.sf(t0,t1), cnt, cnt/pf.uf(t0,t1), pf.nf(t0,t1)/cnt);

    return 0;
}
