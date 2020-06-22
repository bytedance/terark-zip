#include <stdio.h>
#include <random>
#include <terark/fstring.hpp>
#include <terark/int_vector.hpp>
#include <terark/util/profiling.hpp>

using namespace terark;
profiling pf;
const char* prog = NULL;
template<class UintVec, bool Bench, bool MemFunc>
double test(const valvec<size_t>& data,
            const valvec<size_t>& shuf,
            size_t loop) {
    fprintf(stderr, "%s: %s: size = %zd, loop = %zd\n",
            prog, BOOST_CURRENT_FUNCTION, data.size(), loop);

    size_t size = data.size();
    UintVec uv; uv.build_from(data);
    auto t0 = pf.now();
    for (size_t i = 0; i < loop; ++i) {
        for(size_t j = 0; j < size; ++j) {
            size_t k = shuf[j];
            size_t l1 = MemFunc ? uv.lower_bound(k) : lower_bound_0<UintVec&>(uv, size, k);
            size_t u1 = MemFunc ? uv.upper_bound(k) : upper_bound_0<UintVec&>(uv, size, k);
            auto   r1 = MemFunc ? uv.equal_range(k) : equal_range_0<UintVec&>(uv, size, k);
            if (!Bench) {
                size_t l2 = lower_bound_0(data.begin(), size, k);
                size_t u2 = upper_bound_0(data.begin(), size, k);
                auto   r2 = equal_range_0(data.begin(), size, k);
                TERARK_VERIFY_F(l1 == l2, "%zd %zd", l1, l2);
                TERARK_VERIFY_F(u1 == u2, "%zd %zd", u1, u2);
                TERARK_VERIFY_F(r1 == r2, "%zd %zd %zd %zd",
                    r1.first, r1.second, r2.first, r2.second);
            }
        }
        if (!Bench)
            break;
    }
    auto t1 = pf.now();
    double d = pf.sf(t0, t1);
    if (Bench) {
        fprintf(stderr, "%s: time = %8.5f sec, %8.3f ns per op\n",
                BOOST_CURRENT_FUNCTION, d, 1e9/3*d/size/loop);
    }
    return d;
}

int main(int argc, char* argv[]) {
    prog = argv[0];
    size_t size = (size_t)getEnvLong("size", TERARK_IF_DEBUG(10000, 100000));
    size_t loop = (size_t)getEnvLong("loop", TERARK_IF_DEBUG(1, 10));
    valvec<size_t> data(size, valvec_no_init());
    valvec<size_t> shuf(size, valvec_no_init());
    for (size_t i = 0; i < size; ++i) data[i] = i;
    for (size_t i = 0; i < size; ++i) shuf[i] = i;
    std::shuffle(shuf.begin(), shuf.end(), std::mt19937_64());
    test<   UintVecMin0, 0, 1>(data, shuf, loop);
    test<BigUintVecMin0, 0, 1>(data, shuf, loop);

    double td1 = test<   UintVecMin0, 1, 0>(data, shuf, loop);
    double td2 = test<BigUintVecMin0, 1, 0>(data, shuf, loop);
    double td3 = test<   UintVecMin0, 1, 1>(data, shuf, loop);
    double td4 = test<BigUintVecMin0, 1, 1>(data, shuf, loop);

    fprintf(stderr, "time: GenFunc: BigUintVecMin0/UintVecMin0 = %f\n", td2/td1);
    fprintf(stderr, "time: MemFunc: BigUintVecMin0/UintVecMin0 = %f\n", td4/td3);

    return 0;
}
