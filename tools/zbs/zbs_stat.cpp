#ifdef _MSC_VER
#define _CRT_NONSTDC_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <terark/zbs/abstract_blob_store.hpp>
#include <getopt.h>

using namespace terark;

void usage(const char* prog) {
	fprintf(stderr, "Usage: %s Options Input-BlobStore-File\n"
		"Synopsis:\n"
		"Options:\n"
		"    -h Show this help information\n"
		, prog);
	exit(1);
}

int main(int argc, char* argv[]) {
	for (;;) {
		int opt = getopt(argc, argv, "h");
		switch (opt) {
		case -1:
			goto GetoptDone;
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
	}
	const char* fname = argv[optind];
    const bool  mmapPopulate = false;
#ifdef NDEBUG
	std::unique_ptr<AbstractBlobStore> ds;
	try { ds.reset(AbstractBlobStore::load_from_mmap(fname, mmapPopulate)); }
	catch (const std::exception&) { return 3; }
#else
	std::unique_ptr<AbstractBlobStore> ds(AbstractBlobStore::load_from_mmap(fname, mmapPopulate));
#endif
	long long num   = ds->num_records();
	long long unzip = ds->total_data_size();
	long long ziped = ds->mem_size();
	long long dict  = ds->get_dict().memory.size();
	fprintf(stderr, "record  num: %11lld\n", num);
	fprintf(stderr, "unzip  size: %11lld, avg: %8.3f\n", unzip, 1.0*unzip/num);
	fprintf(stderr, "ziped  size: %11lld, avg: %8.3f\n", ziped, 1.0*ziped/num);
	fprintf(stderr, "unzip / zip: %11.7f\n", 1.0*unzip / ziped);
	fprintf(stderr, "zip / unzip: %11.7f\n", 1.0*ziped / unzip);
    fprintf(stderr, "dict   size: %11lld\n", dict);
	return 0;
}

