#include <stdio.h>
#include <string.h>
#include <random>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/int_vector.hpp>
#include <terark/bitmap.hpp>
#include <terark/fstring.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/linebuf.hpp>

#ifdef __GNUC__
  #pragma GCC diagnostic ignored "-Wunused-variable"
#endif

using namespace terark;

int main(int argc, char* argv[]) {
	LineBuf line;
	size_t lastVal = 0;
	valvec<size_t> trueVals;
	while (line.getline(stdin) > 0) {
		size_t val = strtoull(line.p, NULL, 10);
		if (val >= lastVal) {
			trueVals.push_back(val);
			lastVal = val;
		}
		else {
			fprintf(stderr, "ERROR: val(%zd) < lastVal(%zd)\n", val, lastVal);
			return 1;
		}
	}
	SortedUintVec szipVals;
	std::unique_ptr<SortedUintVec::Builder>
		builder(SortedUintVec::createBuilder(128));
	for (size_t i = 0; i < trueVals.size(); ++i) {
		builder->push_back(trueVals[i]);
	}
	builder->finish(&szipVals);
	for(size_t i = 0; i < trueVals.size()-1; ++i) {
		size_t t0 = trueVals[i];
		size_t t1 = trueVals[i+1];
		size_t z[2];
		szipVals.get2(i, z);
		assert(t0 == z[0]);
		assert(t1 == z[1]);
	}
	const size_t n = trueVals.size();
	const size_t maxWidth = 1 + terark_bsr_u64(trueVals.back());
	febitvec uv(maxWidth * n);
	for (size_t i = 0; i < n; ++i) {
		uv.set_uint(maxWidth*i, maxWidth, trueVals[i]);
	}
	size_t u = uv.mem_size();
	size_t z = szipVals.mem_size();
	printf("units = %9zd, minVal = %zd, maxVal = %zd\n", n, trueVals[0], trueVals.back());
	printf("unzip = %9zd, bits per unit = %6.3f\n", u, 8.0*u/n);
	printf("__zip = %9zd, bits per unit = %6.3f\n", z, 8.0*z/n);
	printf("unz/z = %9.4f\n", 1.0*u/z);
	printf("z/unz = %9.4f\n", 1.0*z/u);

	size_t loop = 5;
	printf("run benchmark, loop = %zd ...\n", loop);
	valvec<size_t> randidx(n, valvec_no_init());
	for (size_t i = 0; i < n; ++i) randidx[i] = i;
	randidx[n-1] = n-2;
	std::shuffle(randidx.begin(), randidx.end(), std::mt19937_64());
	terark::profiling pf;
	long long t0 = pf.now();
	long long zsum = 0;
	for (size_t l = 0; l < loop; ++l)
		for(size_t i = 0; i < n; ++i) {
			size_t idx = randidx[i];
			size_t z[2];
			szipVals.get2(idx, z);
			zsum += z[1] + z[0];
		}
	long long t1 = pf.now();
	long long usum = 0;
	for (size_t l = 0; l < loop; ++l)
		for(size_t i = 0; i < n; ++i) {
			size_t idx = randidx[i];
			size_t z[2];
			uv.get2_uints(maxWidth*idx, maxWidth, z);
			usum += z[1] + z[0];
		}
	long long t2 = pf.now();
	printf("random zip: time = %12.3f sec, avg = %8.1f ns, QPS = %8.3f M/sec, sum = %016llX, ratio = %6.3f\n"
		, pf.mf(t0,t1), pf.nf(t0,t1)/(n*loop), (n*loop)/pf.uf(t0,t1), zsum
		, pf.nf(t0,t1)/pf.nf(t1,t2)
	);
	printf("random raw: time = %12.3f sec, avg = %8.1f ns, QPS = %8.3f M/sec, sum = %016llX\n"
		, pf.mf(t1,t2), pf.nf(t1,t2)/(n*loop), (n*loop)/pf.uf(t1,t2), usum);

    size_t blockUnits = size_t(1) << szipVals.log2_block_units();
    valvec<size_t> buf(blockUnits);
    long long xsum = 0;
    zsum = 0;
    usum = 0;
    t0 = pf.now();
    for(size_t l = 0; l < loop; ++l) {
        size_t m = align_down(n-1, blockUnits);
        size_t z[2];
        for(size_t i = 0; i < m; ++i) {
            szipVals.get2(i, z);
            zsum += z[0];
        }
    }
    t1 = pf.now();
    for(size_t l = 0; l < loop; ++l) {
        size_t m = align_down(n-1, blockUnits);
        size_t z[2];
        for(size_t i = 0; i < m; ++i) {
            uv.get2_uints(maxWidth*i, maxWidth, z);
            usum += z[0];
        }
    }
    t2 = pf.now();
    for(size_t l = 0; l < loop; ++l) {
        size_t m = align_down(n-1, blockUnits);
        for (size_t i = 0, h = m / blockUnits; i < h; ++i) {
            szipVals.get_block(i, buf.data());
            szipVals.get_block(i, buf.data());
            for (size_t j = 0; j < blockUnits; ++j) {
                xsum += buf[j];
                assert(trueVals[i*blockUnits+j] == buf[j]);
            }
        }
    }
    long long t3 = pf.now();
	printf("sequen zip: time = %12.3f sec, avg = %8.1f ns, QPS = %8.3f M/sec, sum = %016llX, ratio = %6.3f\n"
		, pf.mf(t0,t1), pf.nf(t0,t1)/(n*loop), (n*loop)/pf.uf(t0,t1), zsum
		, pf.nf(t0,t1)/pf.nf(t1,t2)
	);
	printf("sequen raw: time = %12.3f sec, avg = %8.1f ns, QPS = %8.3f M/sec, sum = %016llX\n"
		, pf.mf(t1,t2), pf.nf(t1,t2)/(n*loop), (n*loop)/pf.uf(t1,t2), usum);
	printf("bulk unzip: time = %12.3f sec, avg = %8.1f ns, QPS = %8.3f M/sec, sum = %016llX\n"
		, pf.mf(t2,t3), pf.nf(t2,t3)/(n*loop), (n*loop)/pf.uf(t2,t3), xsum);

    return 0;
}

