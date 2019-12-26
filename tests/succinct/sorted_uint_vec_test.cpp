#include <stdio.h>
#include <string.h>
#include <random>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/int_vector.hpp>
#include <terark/bitmap.hpp>
#include <terark/fstring.hpp>
#include <terark/util/profiling.hpp>

#ifdef __GNUC__
  #pragma GCC diagnostic ignored "-Wunused-variable"
#endif


using namespace terark;

void unit_test_bug1() {
    SortedUintVec szipVals;
    std::unique_ptr<SortedUintVec::Builder>
        builder(SortedUintVec::createBuilder(false, 128));
    static const char vals[] = "420864208642";
    for (size_t i = 0; i < strlen(vals); ++i) {
        builder->push_back(vals[i] - '0');
    }
    builder->finish(&szipVals);
    for (size_t i = 0; i < szipVals.size(); ++i) {
        assert(szipVals[i] == vals[i] - '0');
    }
}

void unit_test_small() {
    SortedUintVec szipVals;
    std::unique_ptr<SortedUintVec::Builder>
        builder(SortedUintVec::createBuilder(128));
    builder->push_back(0);
    builder->push_back(2);
    builder->push_back(5);
    builder->finish(&szipVals);
    size_t BegEnd[2];
    szipVals.get2(0, BegEnd);
    assert(BegEnd[0] == 0);
    assert(BegEnd[1] == 2);
    szipVals.get2(1, BegEnd);
    assert(BegEnd[0] == 2);
    assert(BegEnd[1] == 5);
    printf("done unit_test_small!\n");
}

void unit_test_binary_search() {
    valvec<size_t> truth;
    for (size_t i = 0; i < 500; ++i) truth.push_back(0);
    for (size_t i = 0; i < 500; ++i) truth.push_back(2);
    for (size_t i = 0; i < 500; ++i) truth.push_back(5);
    for (size_t i = 0; i < 500; ++i) truth.push_back(5 + i);
    for (size_t i = 0; i < 500; ++i) truth.push_back(600);
    for (size_t i = 0; i <  65; ++i) truth.push_back(500);
    for (size_t i = 0; i <  65; ++i) truth.push_back(400+i);
    for (size_t i = 0; i <  65; ++i) truth.push_back(300+i);
    for (size_t i = 0; i <  65; ++i) truth.push_back(200+i);
    for (size_t i = 0; i <  65; ++i) truth.push_back(150+i);
    for (size_t i = 0; i < 040; ++i) truth.push_back(160+i);
    for (size_t i = 0; i < 500; ++i) truth.push_back(700);
    SortedUintVec szip;
    szip.build_from(truth, 128);
    auto test = [&](auto getLo, auto getHi, auto getKey) {
        for(size_t i = 0; i < truth.size(); ++i) {
            size_t k = getKey(truth[i]);
            auto lo = getLo(i);
            auto hi = std::min(truth.size(), getHi(i));
            for (size_t j = lo+1; j < hi; ++j) {
                if (truth[j] < truth[j-1])
                    goto Continue;
            }
            {
                auto l1 = lower_bound_n(truth, lo, hi, k);
                auto u1 = upper_bound_n(truth, lo, hi, k);
                auto r1 = equal_range_n(truth, lo, hi, k);
                auto l2 = szip.lower_bound(lo, hi, k);
                auto u2 = szip.upper_bound(lo, hi, k);
                auto r2 = szip.equal_range(lo, hi, k);
                assert(l1 == r1.first);
                assert(u1 == r1.second);
                assert(l1 == l2);
                assert(u1 == u2);
                assert(l2 == r2.first);
                assert(u2 == r2.second);
            }
            Continue:;
        }
    };
    auto test2 = [&](auto getKey) {
        test([](auto i){return i/2;},
             [](auto i){return i + size_t(10*sqrt(i));}, getKey
        );
        test([](auto i){return i/3;},
             [](auto i){return i + 64;}, getKey
        );
        test([](auto i){return i/2;},
             [](auto i){return i + 128;}, getKey
        );
        test([](auto i){return i/2;},
             [](auto i){return size_t(-1);}, getKey
        );
        test([](auto i){return i;},
             [](auto i){return i + 1;}, getKey
        );
        test([](auto i){return i;},
             [](auto i){return i + 3;}, getKey
        );
    };
    auto hit = [](auto key) { return key;};
    auto miss = [](auto key) { return key+1;};
    test2(hit);
    test2(miss);
    printf("done unit_test_binary_search!\n");
}

int main(int argc, char* argv[]) {
	unit_test_bug1();
	unit_test_small();
	unit_test_binary_search();
	size_t groupSize = 200;
	if (argc >= 2) {
		groupSize = atoi(argv[1]);
	}
	const char* fname = NULL;
	if (argc >= 3) {
		fname = argv[2];
	}
	valvec<size_t>  trueVals(groupSize*10, valvec_reserve());
	std::mt19937_64 random;
	size_t curVal = 100;
	for (size_t i = 0; i < groupSize; ++i) {
		trueVals.push_back(curVal);
	}
	for (size_t i = 0; i < groupSize; ++i) {
	// for widthType = 11, all 1 bit
		trueVals.push_back(curVal);
		curVal += 1;
		if (random() % 16 < 6) {
			curVal += 1;
		}
	}
	for (size_t i = 0; i < groupSize; ++i) {
	// for widthType = 15, all 2 bits
		trueVals.push_back(curVal);
		curVal += 1;
		if (random() % 16 < 6) {
			curVal += 3;
		}
	}
	auto add = [&](size_t maxDiff) {
		for(size_t i = 0; i < groupSize; ++i) {
			size_t r = random();
			if (r % 16 == 1 && maxDiff < 1LL<<48) {
				curVal += r % maxDiff * 80;
			}
			if (maxDiff >= 2) {
				if (r % 16 < 5)
					curVal += maxDiff - 2;
				else if (r % 16 < 7)
					curVal += maxDiff / 2 + 3;
				else if (r % 16 < 10)
					curVal += maxDiff * 2 / 3;
			}
			curVal += maxDiff;
			trueVals.push_back(curVal);
		}
		if (maxDiff >= 2) {
			for (size_t i = 0; i < groupSize; ++i) {
				curVal += (maxDiff-2) * 2;
				trueVals.push_back(curVal);
			}
		}
	};
	add(1ull <<  0);
	add(1ull <<  1);
	add(1ull <<  2);
	add(1ull <<  3);
	add(1ull <<  4);
	add(1ull <<  5);
	add(1ull <<  6);
	add(1ull <<  7);
	add(1ull <<  8);
	add(1ull <<  9);
	add(1ull << 10);
	add(1ull << 11);
	add(1ull << 12);
	add(1ull << 13);
	add(1ull << 14);
	add(1ull << 15);
	add(1ull << 16);
	add(1ull << 17);
	add(1ull << 18);
	add(1ull << 19);
	add(1ull << 20);
	add(1ull << 21);
	add(1ull << 30);
	add(1ull << 31);
	add(1ull << 32);
	if (groupSize < 1000) {
		add(1ull << 37);
		add(1ull << 49);
	}
	bool mustSorted = getEnvBool("sorted", true);
	if (!mustSorted) {
		for (size_t i = 0; i < 100; ++i) {
			for (size_t j = 0; j < 20; ++j) {
				trueVals.push_back(curVal + 3*i*i+j);
			}
		}
	}
	SortedUintVec szipVals;
	std::unique_ptr<SortedUintVec::Builder>
		builder(SortedUintVec::createBuilder(mustSorted, 128, fname));
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
			if (mustSorted && z[0] > z[1]) {
				printf("mis-order: idx = %zd\n", idx);
			}
			zsum += z[1] + z[0];
		}
	long long t1 = pf.now();
	long long usum = 0;
	for (size_t l = 0; l < loop; ++l)
		for(size_t i = 0; i < n; ++i) {
			size_t idx = randidx[i];
			size_t z[2];
			uv.get2_uints(maxWidth*idx, maxWidth, z);
			if (mustSorted && z[0] > z[1]) {
				printf("mis-order: idx = %zd\n", idx);
			}
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
            if (mustSorted && z[0] > z[1]) {
                printf("mis-order: idx = %zd\n", i);
            }
            zsum += z[0];
        }
    }
    t1 = pf.now();
    for(size_t l = 0; l < loop; ++l) {
        size_t m = align_down(n-1, blockUnits);
        size_t z[2];
        for(size_t i = 0; i < m; ++i) {
            uv.get2_uints(maxWidth*i, maxWidth, z);
            if (mustSorted && z[0] > z[1]) {
                printf("mis-order: idx = %zd\n", i);
            }
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

