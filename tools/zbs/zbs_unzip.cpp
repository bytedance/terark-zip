#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <thread>
#include <terark/zbs/fast_zip_blob_store.hpp>
#include <terark/fsa/fsa.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/profiling.hpp>
#include <getopt.h>
//#include <thread> // weired complition error in vs2015, so move to first inlcude
#include <random>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr, "Usage: %s Options Input-BlobStore-File [recId1 recId2 ...]\n"
		"Synopsis:\n"
		"    If recId1 recId2 ... are provided, just unzip/extract the specified records\n"
		"    Record id is 0-base, so the min record id is 0, not 1\n"
		"Options:\n"
		"    -h Show this help information\n"
		"    -t Show timing and through put\n"
		"    -p MMAP_POPULATE(linux) or FileMapping prefetch(windows)\n"
		"    -r Unzip in random order\n"
		"    -b Bench mark loop, this will not output unzipped data\n"
		"    -B Output as binary, do not append newline for each record\n"
		"    -T thread num, when benchmark, use multi thread\n"
		, prog);
	exit(1);
}

int main(int argc, char* argv[]) {
	bool isBinary = false;
	bool timing = false;
	bool isRandom = false;
	bool mmapPopulate = false;
	int benchmarkLoop = false;
	int threads = 0;
	for (;;) {
		int opt = getopt(argc, argv, "b:BhtrpT:");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'b':
			benchmarkLoop = atoi(optarg);
			break;
		case 'B':
			isBinary = true;
			break;
		case 't':
			timing = true;
			break;
		case 'r':
			isRandom = true;
			break;
		case 'p':
			mmapPopulate = true;
			break;
		case 'T':
			threads = atoi(optarg);
			break;
		case '?':
		case 'h':
		default:
			usage(argv[0]);
		}
	}
GetoptDone:
	if (optind >= argc) {
		fprintf(stderr, "Missing input BlobStore file\n");
		usage(argv[0]);
		return 1;
	}
	const char* dfaFname = argv[optind];
	profiling pf;
	long long t0 = pf.now();
#ifdef NDEBUG
	std::unique_ptr<AbstractBlobStore> ds;
	try { ds.reset(AbstractBlobStore::load_from_mmap(dfaFname, mmapPopulate)); }
	catch (const std::exception&) { return 3; }
#else
	std::unique_ptr<AbstractBlobStore> ds(AbstractBlobStore::load_from_mmap(dfaFname, mmapPopulate));
#endif
	valvec<byte_t> rec;
	long long t1 = pf.now();
	long long t2 = t1;
	long long num = 0;
	long long bytes = 0;
	auto getOne = [&](size_t recId) {
		ds->get_record(recId, &rec);
	//	fprintf(stderr, "%08zd: len=%05zd\n", recId, rec.size());
		bytes += rec.size();
		if (!benchmarkLoop) {
			if (isBinary) {
				uint32_t len = rec.size() + 4;
				rec.insert(0, (byte_t*)&len, 4);
			} else {
				rec.push_back('\n');
			}
			fwrite(rec.data(), 1, rec.size(), stdout);
		}
	};
	if (optind + 1 < argc) {
		valvec<size_t> idvec(argc - (optind+1), valvec_reserve());
		for (int i = optind + 1; i < argc; ++i) {
			size_t recId = (size_t)strtoull(argv[i], NULL, 0);
			if (recId >= ds->num_records()) {
				fprintf(stderr
					, "recId = %zd is out of range(num_records = %zd)\n"
					, recId, ds->num_records());
				continue;
			}
			idvec.push_back(recId);
		}
		for (size_t i = 0; i < idvec.size(); ++i) {
			getOne(idvec[i]);
		}
		num = idvec.size();
	}
    else if (isRandom) {
        long long t2_0 = pf.now();
        auto threadCount = threads ? threads : 1;
        fprintf(stderr, "%d threads benchmark ...\n", threadCount);
        fprintf(stderr, "random shuffle ..."); fflush(stderr);
        valvec<size_t> idvec(ds->num_records(), valvec_no_init());
        for (size_t i = 0; i < idvec.size(); ++i) idvec[i] = i;
        std::mt19937_64 random;
        std::shuffle(idvec.begin(), idvec.end(), random);
        t2 = pf.now();
        fprintf(stderr, "done, time = %f\n", pf.sf(t2_0, t2));
        std::vector<std::thread> thr;
        std::vector<size_t> tbytes(threadCount, 0);
        thr.reserve(threadCount);
        for (int j = 0; j < threadCount; ++j) {
            size_t Beg = ds->num_records() * (j + 0) / threadCount;
            size_t End = ds->num_records() * (j + 1) / threadCount;
            auto pds = ds.get();
            auto pb = &tbytes[j];
            thr.emplace_back([&idvec, Beg, End, pds, pb, benchmarkLoop] {
                valvec<byte_t> buf;
                size_t* idptr = idvec.data();
                for (int k = 0; k < benchmarkLoop; ++k) {
                    size_t b = 0; // use local to avoid false sharing
                    for (size_t i = Beg; i < End; ++i) {
                        pds->get_record(idptr[i], &buf);
                        b += buf.size();
                    }
                    *pb = b;
                }
            });
        }
        for (auto& t : thr) t.join();
        for (auto b : tbytes) bytes += b;
        num = ds->num_records();
        fprintf(stderr, "%d threads benchmark done.\n", threads);
	}
	else if (threads) {
		std::vector<std::thread> thr;
		std::vector<size_t> tbytes(threads, 0);
		thr.reserve(threads);
		fprintf(stderr, "%d threads benchmark ...\n", threads);
		for (int j = 0; j < threads; ++j) {
			size_t Beg = ds->num_records() * (j + 0) / threads;
			size_t End = ds->num_records() * (j + 1) / threads;
			auto pds = ds.get();
			auto pb = &tbytes[j];
			thr.emplace_back([Beg,End,pds,pb,benchmarkLoop]() {
				valvec<byte_t> buf;
                for (int k = 0; k < benchmarkLoop; ++k) {
    				size_t b = 0; // use local to avoid false sharing
				    for (size_t i = Beg; i < End; ++i) {
					    pds->get_record(i, &buf);
					    b += buf.size();
				    }
    				*pb = b;
                }
			});
		}
		for (auto& t : thr) t.join();
		for (auto  b : tbytes) bytes += b;
		num = ds->num_records();
		fprintf(stderr, "%d threads benchmark done.\n", threads);
	}
	else {
		for (size_t i = 0; i < ds->num_records(); ++i) {
			getOne(i);
		}
		num = ds->num_records();
	}
	long long t3 = pf.now();
	if (timing || benchmarkLoop || threads) {
		fprintf(stderr, "record num: %12lld\n", num);
		fprintf(stderr, "unzip size: %12lld, avg = %8.1f\n", bytes, 1.0*bytes/num);
		fprintf(stderr, "ziped size: %12zd, avg = %8.1f\n", ds->mem_size(), 1.0*ds->mem_size()/num);
		fprintf(stderr, "unzip /zip: %12.3f\n", 1.0*bytes / ds->mem_size());
		fprintf(stderr, "zip /unzip: %12.3f\n", 1.0*ds->mem_size() / bytes);
		fprintf(stderr, "load  time: %12.3f, QPS: %9.3f K,  through-put: %9.3f MB/s\n",
			pf.sf(t0,t1), ds->num_records()/pf.mf(t0,t1), ds->total_data_size()/pf.uf(t0,t1));
		fprintf(stderr, "unzip time: %12.3f, QPS: %9.3f K,  through-put: %9.3f MB/s\n",
			pf.sf(t2,t3), benchmarkLoop*num/pf.mf(t2,t3), bytes*benchmarkLoop/pf.uf(t2,t3));
	}
	return 0;
}

