#define _SCL_SECURE_NO_WARNINGS // fuck vc
#define _CRT_SECURE_NO_WARNINGS
#define _CRT_NONSTDC_NO_WARNINGS

#include <terark/fsa/fsa.hpp>
#include <terark/zbs/fast_zip_blob_store.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>
#include <getopt.h>
#include <stdint.h>

static void usage(const char* prog) {
	fprintf(stderr, R"EOS(Usage:
   %s Options DFA-BlobStore-File [ RecordID-List-File ]

Description:
   Blinde bench mark data extraction performance of DFA based BlobStore.
   Inputs are a required DFA-BlobStore-File and an optinal RecordID-List-File,
   If RecordID-List-File is missing, read it from stdin.

Options:
   -B
      Indicate that data stored in DFA is binary, and will not add line feed
	  for each record on output
   -b
      Indicate that input RecordID-List-File is in binary
	  Default is text file, every line is a text integer number for ID
	  When -b is specified, every 4 byte is a BigEndian uint32
   -o Output-File
      Write extracted records to Output-File, this output can be monitored and
	  checked for correctness.
	  If this argument is ommited, will not write any output, this will remove
      time used for write output, and the unzip speed is more acurate.
)EOS" , prog);
}

int main(int argc, char* argv[]) {
	using namespace terark;
	bool isBinaryInput = false;
	bool isBinaryDFA = false;
	bool mmapPopulate = false;
	const char* dfaFname = NULL;
	const char* recIdFname = NULL;
	const char* outputFname = NULL;
	for (;;) {
		int opt = getopt(argc, argv, "Bbo:p");
		switch (opt) {
		default:
			usage(argv[0]);
			return 1;
		case -1:
			goto GetoptDone;
		case 'B':
			isBinaryDFA = true;
			break;
		case 'b':
			isBinaryInput = true;
			break;
		case 'o':
			outputFname = optarg;
			break;
		case 'p':
			mmapPopulate = true;
			break;
		}
	}
GetoptDone:
	if (optind >= argc) {
		fprintf(stderr, "-f DFA-BlobStore-File is missing\n");
		usage(argv[0]);
		return 1;
	}
	dfaFname = argv[optind];
	if (optind < argc) {
		recIdFname = argv[optind+1];
	}
	valvec<uint32_t> idvec;
	Auto_fclose fp, ofp;
	if (recIdFname) {
		fp = fopen(recIdFname, isBinaryInput ? "rb" : "r");
		if (!fp) {
			fprintf(stderr, "ERROR: fopen(%s, r) = %s\n", recIdFname, strerror(errno));
			return 3;
		}
	}
	if (outputFname) {
		ofp = fopen(outputFname, isBinaryDFA ? "wb" : "w");
		if (!fp) {
			fprintf(stderr, "ERROR: fopen(%s, w) = %s\n", outputFname, strerror(errno));
			return 3;
		}
	}
	terark::profiling pf;
	fprintf(stderr, "Loading dfa...\n");
	long long t0 = pf.now();
#ifdef NDEBUG
	std::unique_ptr<BlobStore> ds;
	try { ds.reset(BlobStore::load_from_mmap(dfaFname, mmapPopulate)); }
	catch (const std::exception&) { return 3; }
#else
	std::unique_ptr<BlobStore> ds(BlobStore::load_from_mmap(dfaFname, mmapPopulate));
#endif
	long long t1 = pf.now();
	fprintf(stderr, "Loaded dfa, numRecords=%ld, used %f seconds!\n", long(ds->num_records()), pf.sf(t0,t1));
	fprintf(stderr, "Loading RecordID-List...\n");
	if (!isBinaryInput) {
		LineBuf line;
		while (line.getline(fp.self_or(stdin)) > 0) {
			line.chomp();
			if (!line.empty()) {
				unsigned long recID = strtoul(line, NULL, 10);
				if (recID >= ds->num_records()) {
					fprintf(stderr, "ERROR: invalid record id=%ld, discarded\n", long(recID));
				} else {
					idvec.push_back(recID);
				}
			}
		}
	} else {
		for (;;) {
			uint32_t bulkData[128];
			intptr_t bytes = fread(&bulkData, 1, sizeof(bulkData), fp.self_or(stdin));
			if (bytes <= 0) break;
			for(intptr_t i = 0; i < bytes/4; ++i) {
				unsigned recID = bulkData[i];
				if (recID >= ds->num_records()) {
					fprintf(stderr, "ERROR: invalid record id=%d, discarded\n", recID);
				} else {
					idvec.push_back(recID);
				}
			}
		}
	}
	long long t2 = pf.now();
	fprintf(stderr, "Loaded %ld RecordID, used %f seconds, qps is %f M\n",
		   	long(idvec.size()), pf.sf(t1,t2), idvec.size()/pf.uf(t1,t2));
	fprintf(stderr, "Start bench mark...\n");

	long long t3 = pf.now();
	valvec<byte_t> recData;
	long long total = 0;
	for (size_t i = 0; i < idvec.size(); ++i) {
		uint32_t recID = idvec[i];
		ds->get_record(recID, &recData);
		total += recData.size();
		if (ofp) {
			if (!isBinaryDFA)
				recData.push_back('\n');
			fwrite(recData.data(), 1, recData.size(), ofp);
		}
	}
	long long t4 = pf.now();
	fprintf(stderr, "bench mark    elipsed time: %f seconds\n", pf.sf(t3,t4));
	fprintf(stderr, "total queried records size: %lld\n", total);
	fprintf(stderr, "query(unzip)  through-put : %f MB/s\n", total/pf.uf(t3,t4));
	fprintf(stderr, "query(unzip)  QPS         : %f K\n", idvec.size()/pf.mf(t3,t4));

	return 0;
}

