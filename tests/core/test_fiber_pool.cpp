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

#include <boost/fiber/fiber.hpp>

int main(int argc, char* argv[]) {
    {
        using namespace boost::fibers;
        fiber(launch::dispatch, []() {
            printf("boost::fibers::fiber launched\n");
        }).detach();
    }
    if (0){
        using namespace boost::context;
        fiber m;
        fiber([&](fiber&& c) {
            m = std::move(c);
            printf("boost::context::fiber resumed\n");
            return fiber{};
        }).resume();
        // m becomes valid ??
    }
    using namespace terark;
    return 0;
    RunOnceFiberPool fp1( 8);
    RunOnceFiberPool fp2(64);

    profiling pf;
    size_t cnt = 0;
    size_t loop_cnt = getEnvLong("loop_cnt", TERARK_IF_DEBUG(2000, 10000));
    long long t0, t1;
    RunOnceFiberPool::Worker h1;

    t0 = pf.now();
    for (size_t i = 0; i < loop_cnt; ++i) {
        fp2.submit(h1, [&](){
            cnt++;
            fp2.yield();
        });
    }
    fp2.reap(h1);
    assert(fp2.capacity() == fp2.freesize());
    t1 = pf.now();
    //cnt = fp2.yield_cnt();
    printf("test_fiber_pool-async : time = %f sec, cnt = %zd, ops = %f M/sec, latency = %f ns\n",
            pf.sf(t0,t1), cnt, cnt/pf.uf(t0,t1), pf.nf(t0,t1)/cnt);

    cnt = 0;
    t0 = pf.now();
    size_t per_fp2 = fp2.capacity()/fp1.capacity();
    for (size_t i = 0; i < loop_cnt; ++i) {
        fp1.submit(h1, [&]() {
            RunOnceFiberPool::Worker h2;
            for (size_t i = 0; i < per_fp2; ++i) {
                fp2.submit(h2, [&]() {cnt++;});
            }
            fp2.reap(h2);
        });
        printf("submit i = %zd\n", i);
    }
    fp1.reap(h1);
    assert(fp1.capacity() == fp1.freesize());
    assert(fp2.capacity() == fp2.freesize());
    t1 = pf.now();

    printf("test_fiber_pool-submit: time = %f sec, cnt = %zd, ops = %f M/sec, latency = %f ns\n",
            pf.sf(t0,t1), cnt, cnt/pf.uf(t0,t1), pf.nf(t0,t1)/cnt);

    return 0;
}
