#include "blob_store_file_header.hpp"
#include "dict_zip_blob_store.hpp"
#include "suffix_array_dict.hpp"
#include "xxhash_helper.hpp"
#include "zip_reorder_map.hpp"
#include "lru_page_cache.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/IStreamWrapper.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/mmap.hpp>
#include <terark/bitmanip.hpp>
#include <terark/bitmap.hpp>
#include <terark/bitfield_array.hpp>
#include <assert.h>
#include <zstd/zstd.h>
#include <zstd/dictBuilder/divsufsort.h>
#include "sufarr_inducedsort.h"
#include <terark/thread/pipeline.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/small_memcpy.hpp>
#include <terark/util/hugepage.hpp>
#include <random>
#include <zstd/common/fse.h>
#include <atomic>

#include "blob_store_file_header.hpp"
#include <terark/entropy/huffman_encoding.hpp>

#if defined(_WIN32) || defined(_WIN64)
#   define WIN32_LEAN_AND_MEAN
#   define NOMINMAX
#   include <Windows.h>
#   if !defined(NDEBUG) && 0
#       undef assert
#       define assert(exp) ((exp) ? (void)0 : DebugBreak())
#   endif
#else
#   include <unistd.h> // for usleep
#endif

namespace terark {

REGISTER_BlobStore(DictZipBlobStore, "DictZipBlobStore");

static profiling g_pf;
static std::atomic<uint64_t> g_dataThroughBytes(0);
extern int g_useDivSufSort;

#if !defined(NDEBUG)
static void DoUnzip(const byte_t* pos, const byte_t* end, valvec<byte_t>* recData, const byte_t* dic, size_t dicLen, size_t reserveOutputMultiplier);
#endif

enum class EmbeddedDictType : uint8_t {
  External,
  Raw,
  ZSTD,
};

//#define DzTypeBits 3
enum class DzType : byte {
	Literal,   // len in [1, 32]
	Global,    // len in [6, ...)
	RLE,       // distance is 1, len in [2, 33]
	NearShort, // distance in [2, 9], len in [2, 5]
	Far1Short, // distance in [2, 257], len in [2, 33]
	Far2Short, // distance in [258, 258+65535], len in [2, 33]

	// min distance = 0, reduce 1 instruction when unzip
	Far2Long,  // distance in [0, 65535], len in [34, ...)
	Far3Long,  // distance in [0, 2^24-1], len in [5, 35] or [36, ...)
};
struct DzEncodingMeta {
	DzType type;
	signed char len;
};
DzEncodingMeta GetBackRef_EncodingMeta(size_t distance, size_t len) {
	assert(distance >= 1);
	assert(distance <= 1ul<<24);
	assert(len >= 2);
	assert(len <= 1ul<<24);
	if (1 == len) {
		return { DzType::Literal, 2 };
	}
	if (1 == distance && len <= 33) {
		return { DzType::RLE, 1 };
	}
	if (distance >= 2 && distance <= 9 && len <= 5) {
		return { DzType::NearShort, 1 };
	}
	if (distance >= 2 && distance <= 257 && len <= 33) {
		return { DzType::Far1Short, 2 };
	}
	if (distance >= 258 && distance <= 258+65535 && len <= 33) {
		return { DzType::Far2Short, 3 };
	}
	if (distance <= 65535 && len >= 34) {
		if (len <= 34+30)
			return { DzType::Far2Long, 3 };
		else
			// var uint, likely max extra 3 bytes for encoded len
			return { DzType::Far2Long, 6 };
	}
	// Far3Long
	if (len <= 35)
		return { DzType::Far3Long, 4 };
	else
		// var uint, likely max extra 3 bytes for encoded len
		return { DzType::Far3Long, 7 };
}

void*
FSE_zip(const void* data, size_t size,
		void* gz, size_t gzsize, FSE_CTable* gtable,
		size_t* ezsize) {
	if (size > 2) {
		gzsize = FSE_compress_usingCTable(gz, gzsize, data, size, gtable);
		if (gzsize < 2 || FSE_isError(gzsize)) {
			return NULL;
		}
		if (gzsize < size) {
			*ezsize = gzsize;
			return gz;
		}
		return NULL;
	}
	else {
		return NULL;
	}
}

size_t
FSE_unzip(const void* zdata, size_t zsize, void* udata, size_t usize, const void* gtable) {
    return FSE_decompress_usingDTable(udata, usize, zdata, zsize, (const FSE_DTable*)gtable);
}

template<class ByteArray>
inline size_t
FSE_unzip(const void* zdata, size_t zsize, ByteArray& udata, const void* gtable) {
	return FSE_unzip(zdata, zsize, udata.data(), udata.size(), gtable);
}

inline void
DictZipBlobStore::offsetGet2(size_t recId, size_t BegEnd[2], bool isZipped)
const {
	if (isZipped)
		m_zOffsets.get2(recId, BegEnd);
	else
		m_offsets.get2(recId, BegEnd);
}

DictZipBlobStore::DictZipBlobStore() {
	init();
}

void DictZipBlobStore::init() {
	m_entropyAlgo = Options::kNoEntropy;
    m_reserveOutputMultiplier = 5;
	m_globalEntropyTableObject = NULL;
    m_huffman_decoder = NULL;
	m_isNewRefEncoding = true;
	new(&m_offsets)UintVecMin0();
    m_gOffsetBits = 0;
}

DictZipBlobStore::~DictZipBlobStore() {
	destroyMe();
}

void DictZipBlobStore::swap(DictZipBlobStore& y) {
    AbstractBlobStore::risk_swap(y);
    std::swap(m_unzip, y.m_unzip);
	std::swap(m_strDict, y.m_strDict);
	std::swap(m_ptrList, y.m_ptrList);
    std::swap(m_entropyBitmap, y.m_entropyBitmap);
    bytewise_swap(m_offsets, y.m_offsets);
    std::swap(m_reserveOutputMultiplier, y.m_reserveOutputMultiplier);
	std::swap(m_globalEntropyTableObject, y.m_globalEntropyTableObject);
    std::swap(m_huffman_decoder, y.m_huffman_decoder);
	std::swap(m_entropyAlgo, y.m_entropyAlgo);
	std::swap(m_isNewRefEncoding, y.m_isNewRefEncoding);
    std::swap(m_entropyInterleaved, y.m_entropyInterleaved);
    std::swap(m_gOffsetBits, y.m_gOffsetBits);
}

void DictZipBlobStore::destroyMe() {
    if (m_isDetachMeta) {
        m_strDict.risk_release_ownership();
        m_offsets.risk_release_ownership();
    }
    switch (m_dictCloseType) {
    case MemoryCloseType::Clear:
        m_strDict.clear();
        break;
    case MemoryCloseType::MmapClose:
        mmap_close(m_strDict.data(), m_strDict.size());
        // fall through
    case MemoryCloseType::RiskRelease:
        m_strDict.risk_release_ownership();
        break;
    }
    m_dictCloseType = MemoryCloseType::Clear;
    if (m_mmapBase) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_offsets.risk_release_ownership();
        m_ptrList.risk_release_ownership();
        m_entropyBitmap.risk_release_ownership();
        m_mmapBase = nullptr;
        m_isMmapData = false;
    }
    else {
        m_offsets.clear();
        m_ptrList.clear();
        m_entropyBitmap.clear();
    }
    if (m_globalEntropyTableObject) {
        FSE_freeDTable((FSE_DTable*)m_globalEntropyTableObject);
        m_globalEntropyTableObject = nullptr;
    }
    if (m_huffman_decoder) {
        delete m_huffman_decoder;
        m_huffman_decoder = nullptr;
    }
}

/////////////////////////////////////////////////////////////////////////////
static inline uint32_t HashBytes(uint32_t bytes, unsigned shift) {
	uint32_t kMul = 0x1e35a7bd;
	return (bytes * kMul) >> shift;
}
static inline uint32_t HashPtr(const byte* p, unsigned shift) {
	return HashBytes(unaligned_load<uint32_t>(p), shift);
}

// "DZBSNARK" = 0x4b52414e53425a44ull // DictZipBlobStoreNARK
static const uint64_t g_dzbsnark_seed = 0x4b52414e53425a44ull;

class DictZipBlobStoreBuilder : public DictZipBlobStore::ZipBuilder {
public:
	typedef DictZipBlobStore::Options    Options;
	typedef DictZipBlobStore::FileHeader FileHeader;
	//valvec<size_t> m_offsets; // modified by writer thread (MyWriteStage)
    febitvec m_entropyBitmap;
	XXHash64 m_xxhash64;
	size_t m_zipDataSize;     // modified by writer thread
	byte*  m_entropyZipDataBase;

	DictZipBlobStore::ZipStat m_zipStat;

	// constant during compressing
	// separate hot data for different threads, to reduce false sharing
	DictZipBlobStore::Options m_opt;
    freq_hist_o1* m_freq_hist;
	FSE_CTable* m_fse_gtable; // global compress table
    Huffman::encoder_o1* m_huffman_encoder;
	valvec<byte_t> m_entropyTableData;
	valvec<byte_t> m_strDict;
	struct PosLen {
		uint32_t pos;
		uint32_t len;
	};
	valvec<PosLen> m_posLen;
    uint64_t m_dictXXHash;
	std::unique_ptr<SuffixDictCacheDFA> m_dict;
    SeekableStreamWrapper<FileMemIO*> m_memStream;
    SeekableStreamWrapper<FileMemIO> m_memLengthStream;
	FileStream  m_fp;
    FileStream  m_fpDelta;
	std::string m_fpath;
    std::string m_fpathForLength;
    size_t      m_fpOffset;
    size_t      m_lengthCount;
    NativeDataOutput<OutputBuffer> m_fpWriter;
    NativeDataOutput<OutputBuffer> m_lengthWriter;
	ullong  m_dataThroughBytesAtBegin;
	ullong  m_sampleStartTime;
	ullong  m_prepareStartTime;
	ullong  m_dictZipStartTime;
	size_t  m_sampleNumber;
	size_t  m_requestSampleBytes;
	size_t  m_lastWarnSampleBytes;
	double  m_warnWindowSize;
	// -------------------------------

	size_t m_unzipSize; // modified by app thread (addRecord)

	DictZipBlobStoreBuilder(const DictZipBlobStore::Options& opt)
		: m_xxhash64(g_dzbsnark_seed)
        , m_memStream(nullptr)
	{

        m_fpWriter.add_ref();
        m_lengthWriter.add_ref();
        m_freq_hist = NULL;
        m_fse_gtable = NULL;
        m_huffman_encoder = NULL;
		m_opt = opt;
		if (m_opt.maxMatchProbe <= 0) {
			if (m_opt.useSuffixArrayLocalMatch)
				m_opt.maxMatchProbe = 30;
			else
				m_opt.maxMatchProbe = 5;
		}
		m_unzipSize = 0;
		m_zipDataSize = 0;
		m_entropyZipDataBase = nullptr;
		m_sampleStartTime = g_pf.now();
		m_prepareStartTime = m_sampleStartTime;
		m_dictZipStartTime = m_sampleStartTime;
		initWarn();
	}

	~DictZipBlobStoreBuilder() {
        if (m_fpDelta) {
            m_lengthWriter.flush();
            m_fpDelta.close();
        }
        if (!m_fpathForLength.empty()) {
            ::remove(m_fpathForLength.c_str());
        }
        if (m_freq_hist) {
            delete m_freq_hist;
        }
        assert(m_fse_gtable == NULL);
        assert(m_huffman_encoder == NULL);
	}

	void initWarn() {
		m_sampleNumber = 0;
		m_requestSampleBytes = 0;
		m_lastWarnSampleBytes = 0;
		m_warnWindowSize = 100.0;
	}

	void finishSample() override;

    AbstractBlobStore::Dictionary getDictionary() const override {
        return AbstractBlobStore::Dictionary(m_strDict, m_dictXXHash);
    }

    void prepareDict() override;

    void entropyStore(std::unique_ptr<terark::DictZipBlobStore> &store, terark::ullong &t2, terark::ullong &t3);
    void EmbedDict(std::unique_ptr<terark::DictZipBlobStore> &store);

    void finish(int flag) override;
    void abandon() override;

	const DictZipBlobStore::ZipStat& getZipStat() override { return m_zipStat; }

    void freeDict() override { m_dict.reset(); }

	void dictSortLeft();  void dictSortLeft(valvec<byte_t>& tmp);
	void dictSortRight(); void dictSortRight(valvec<byte_t>& tmp);
	void dictSortBoth();
	virtual void prepareZip() {}
	virtual void finishZip() = 0;

	bool entropyZip(const byte* rData, size_t rSize,
					EntropyContext& context,
					NativeDataOutput<AutoGrownMemIO>& dio);

	void zipRecord(const byte* rData, size_t rSize,
				   valvec<uint32_t>& hashTabl,
				   valvec<uint32_t>& hashLink,
				   NativeDataOutput<AutoGrownMemIO>& dio);

    template<bool UseSuffixArrayLocalMatch>
    void zipRecord_impl2(const byte* rData, size_t rSize,
                   valvec<uint32_t>& hashTabl,
                   valvec<uint32_t>& hashLink,
                   NativeDataOutput<AutoGrownMemIO>& dio);

    void prepare(size_t records, FileMemIO& mem) override;
    void prepare(size_t records, fstring fpath) override;
    void prepare(size_t records, fstring fpath, size_t ) override;
	void addSample(const byte* rData, size_t rSize) override;
	void useSample(valvec<byte>& sample) override;
	using DictZipBlobStore::ZipBuilder::addSample;

	void dictSwapOut(fstring fname) override;
	void dictSwapIn(fstring fname) override;

	class SingleThread;
	class MultiThread;
};

struct PosLenCmpLeft {
	bool operator()(const DictZipBlobStoreBuilder::PosLen& x,
				    const DictZipBlobStoreBuilder::PosLen& y)
    const {
		auto sx = base + x.pos;
		auto sy = base + y.pos;
		size_t sn = std::min(x.len, y.len);
		int r = memcmp(sx, sy, sn);
		if (r)
			return r < 0;
		else
			return x.len > y.len; // longer is less
	}
	const byte_t* base;
};
struct PosLenCmpRight {
	bool operator()(const DictZipBlobStoreBuilder::PosLen& x,
				    const DictZipBlobStoreBuilder::PosLen& y)
    const {
		auto sx = base + x.pos + x.len;
		auto sy = base + y.pos + y.len;
		size_t sn = std::min(x.len, y.len);
		while (sn) {
			if (*--sx != *--sy) {
				return *sx < *sy;
			}
			--sn;
		}
		return x.len > y.len; // longer is less
	}
	const byte_t* base;
};
void DictZipBlobStoreBuilder::dictSortLeft() {
	valvec<byte_t> tmp;
	dictSortLeft(tmp);
	m_strDict.swap(tmp);
	m_posLen.clear();
}
void DictZipBlobStoreBuilder::dictSortLeft(valvec<byte_t>& tmp) {
	assert(m_posLen.size() > 0);
	std::sort(m_posLen.begin(), m_posLen.end(), PosLenCmpLeft{m_strDict.data()});
	tmp.reserve(m_strDict.size());
	auto pl = m_posLen.data();
	auto base = m_strDict.data();
	tmp.append(base + pl[0].pos, pl[0].len);
	for (size_t  i = 1; i < m_posLen.size(); ++i) {
		auto sprev = base + pl[i-1].pos; auto lprev = pl[i-1].len;
		auto scurr = base + pl[i-0].pos; auto lcurr = pl[i-0].len;
		if (lprev >= lcurr && memcmp(sprev, scurr, lcurr) == 0) {}
		else {
			tmp.append(scurr, lcurr);
		}
	}
	m_strDict.swap(tmp);
	m_posLen.clear();
}

void DictZipBlobStoreBuilder::dictSortRight() {
	valvec<byte_t> tmp;
	dictSortRight(tmp);
	m_strDict.swap(tmp);
	m_posLen.clear();
}
void DictZipBlobStoreBuilder::dictSortRight(valvec<byte_t>& tmp) {
	assert(m_posLen.size() > 0);
	std::sort(m_posLen.begin(), m_posLen.end(), PosLenCmpRight{m_strDict.data()});
	tmp.reserve(m_strDict.size());
	auto pl = m_posLen.data();
	auto base = m_strDict.data();
	tmp.append(base + pl[0].pos, pl[0].len);
	for (size_t  i = 1; i < m_posLen.size(); ++i) {
		auto sprev = base + pl[i-1].pos; auto lprev = pl[i-1].len;
		auto scurr = base + pl[i-0].pos; auto lcurr = pl[i-0].len;
		if (lprev >= lcurr && memcmp(sprev+lprev-lcurr, scurr, lcurr) == 0) {}
		else {
			tmp.append(scurr, lcurr);
		}
	}
}

void DictZipBlobStoreBuilder::dictSortBoth() {
	assert(m_posLen.size() > 0);
	valvec<byte_t> tmp;
	dictSortLeft(tmp);
	dictSortRight();
	if (tmp.size() < m_strDict.size()) {
		tmp.swap(m_strDict);
	}
}

void DictZipBlobStoreBuilder::prepareDict() {
	if (!m_dict) {
		switch (m_opt.sampleSort) {
		default:
			THROW_STD(runtime_error, "invalid sampleSort = %d", m_opt.sampleSort);
			break;
		case Options::kSortNone : break;
		case Options::kSortLeft : dictSortLeft (); break;
		case Options::kSortRight: dictSortRight(); break;
		case Options::kSortBoth : dictSortBoth (); break;
		}
		m_dict.reset(new SuffixDictCacheDFA());
		//m_dict.reset(new HashSuffixDictCacheDFA()); // :( much slower
		m_dict->build_sa(m_strDict);
		size_t minFreq = m_strDict.size() < (1ul << 30) ? 15 : 31;
		//size_t minFreq = 32*1024; // for benchmark pure suffix array match
		m_dict->bfs_build_cache(minFreq, 64);
	}
}

void DictZipBlobStoreBuilder::addSample(const byte* rData, size_t rSize) {
	m_sampleNumber++;
	m_requestSampleBytes += rSize;
	if (m_strDict.size() + rSize >= INT32_MAX) {
		if (m_requestSampleBytes - m_lastWarnSampleBytes > m_warnWindowSize) {
			fprintf(stderr, "WARN: ZipBuilder::addSample:"
				" m_requestSampleBytes = %.6f MB, new samples are ignored\n"
				, m_requestSampleBytes / 1e6
			);
			m_lastWarnSampleBytes = m_requestSampleBytes;
			m_warnWindowSize *= 1.618;
		}
		return;
	}
	if (Options::kSortNone != m_opt.sampleSort) {
		m_posLen.push_back({uint32_t(m_strDict.size()), uint32_t(rSize)});
	}
	m_strDict.append(rData, rSize);
}

/// @sample will be cleared, memory ownershipt is taken by m_strDict
void DictZipBlobStoreBuilder::useSample(valvec<byte>& sample) {
	if (m_strDict.size()) {
		THROW_STD(invalid_argument, "m_strDict is not empty: size = %zd", m_strDict.size());
	}
	m_strDict.clear();
	m_strDict.swap(sample);
	m_zipStat.sampleTime = g_pf.sf(m_sampleStartTime, g_pf.now());
    m_dictXXHash = AbstractBlobStore::Dictionary(m_strDict).xxhash;
}

void DictZipBlobStoreBuilder::finishSample() {
	ullong t1 = g_pf.now();
	m_strDict.shrink_to_fit();
	m_posLen.shrink_to_fit();
	m_zipStat.sampleTime = g_pf.sf(m_sampleStartTime, t1);
    m_dictXXHash = AbstractBlobStore::Dictionary(m_strDict).xxhash;
}

void DictZipBlobStoreBuilder::dictSwapOut(fstring fname) {
	FileStream f(fname, "wb");
	size_t suffixArrayBytes = m_strDict.capacity();
	f.ensureWrite(m_strDict.data(), suffixArrayBytes);
	free(m_strDict.data());
	m_strDict.risk_set_data(nullptr);
	m_dict->da_swapout(f);
}

void DictZipBlobStoreBuilder::dictSwapIn(fstring fname) {
	assert(m_strDict.data() == nullptr);
	FileStream f(fname, "rb");
	size_t suffixArrayBytes = m_strDict.capacity();
	AutoFree<byte_t> sa(suffixArrayBytes);
	f.ensureRead(sa.p, suffixArrayBytes);
	m_dict->da_swapin(f);
	m_strDict.risk_set_data(sa.release());
}

class DictZipBlobStoreBuilder::SingleThread : public DictZipBlobStoreBuilder {
	valvec<uint32_t> m_hashLink;
	valvec<uint32_t> m_hashBucket;
    EntropyContext m_entropyCtx;
	NativeDataOutput<AutoGrownMemIO> m_dio;
public:
	explicit SingleThread(const DictZipBlobStore::Options& opt)
	  : DictZipBlobStoreBuilder(opt) {}

    void prepareZip() override {
        m_xxhash64.reset(g_dzbsnark_seed);
    }
	void finishZip() override {
		m_hashLink.clear();
		m_hashBucket.clear();
		m_dio.clear();
	}
	// rSize can be 0 for this function
	void addRecord(const byte* rData, size_t rSize) override {
		assert(m_dio.tell() == 0);
		if (m_entropyZipDataBase) {
			// entropyZip is the second pass
			size_t maxBytes = align_up(FSE_compressBound(rSize), 4);
			m_hashBucket.resize_no_init(maxBytes/4);
			m_hashLink.resize_no_init(maxBytes/4);
			bool zip = entropyZip(rData, rSize, m_entropyCtx, m_dio);
			memcpy(m_entropyZipDataBase + m_zipDataSize, m_dio.begin(), m_dio.tell());
			m_xxhash64.update(m_dio.begin(), m_dio.tell());
			m_entropyBitmap.push_back(zip);
		}
		else {
			zipRecord(rData, rSize, m_hashBucket, m_hashLink, m_dio);
			if (m_opt.kNoEntropy == m_opt.entropyAlgo) {
				m_xxhash64.update(m_dio.begin(), m_dio.tell());
			}
            m_fpWriter.ensureWrite(m_dio.begin(), m_dio.tell());
		}
        if (m_freq_hist) {
            m_freq_hist->add_record(fstring(m_dio.begin(), m_dio.tell()));
        }
		m_zipDataSize += m_dio.tell();
		m_unzipSize += rSize;
        m_lengthWriter << var_uint64_t(m_dio.tell());
        ++m_lengthCount;
		m_dio.rewind();
	}
	using DictZipBlobStore::ZipBuilder::addRecord;
};

static int g_zipThreads = (int)getEnvLong("DictZipBlobStore_zipThreads", -1);
static bool g_isPipelineStarted = false;

static bool g_silentPipelineMsg = getEnvBool("DictZipBlobStore_silentPipelineMsg", false);
static bool g_printEntropyCount = getEnvBool("DictZipBlobStore_printEntropyCount", false);

TERARK_DLL_EXPORT void DictZipBlobStore_setZipThreads(int zipThreads) {
	if (g_isPipelineStarted) {
		fprintf(stderr,
			"WARN: DictZipBlobStore pipeline has started, can not change zipThreads\n");
	}
	else {
		g_zipThreads = zipThreads;
	}
}

void DictZipBlobStore_silentPipelineMsg(bool silent) {
  if (g_isPipelineStarted) {
    fprintf(stderr,
      "WARN: DictZipBlobStore pipeline has started, can not change silentPipelineMsg\n");
  }
  else {
    g_silentPipelineMsg = silent;
  }
}

static size_t DictZipBlobStore_batchBufferSize() {
//  long def = 2L*1024*1024; // 2M
    long def = 256*1024;
    long val = getEnvLong("DictZipBlobStore_batchBufferSize", def);
    if (val <= 0) {
        val = def;
    }
    return val;
}
static size_t g_input_bufsize = DictZipBlobStore_batchBufferSize();
class DictZipBlobStoreBuilder::MultiThread : public DictZipBlobStoreBuilder {
	class MyTask : public PipelineTask {
	public:
		MultiThread* builder;
		size_t  firstRecId;
		fstrvec ibuf;
		valvec<uint32_t> offsets;
		febitvec entropyBitmap;
		NativeDataOutput<AutoGrownMemIO> obuf;

		MyTask(MultiThread* b, size_t recId) {
			builder = b;
			firstRecId = recId;
			ibuf.strpool.reserve(g_input_bufsize);
			ibuf.offsets.reserve(g_input_bufsize/100);
			offsets.reserve(g_input_bufsize/100);
            entropyBitmap.reserve(g_input_bufsize/100);
		//	offsets.push_back(0);
		}
	};
	class MyZipStage : public PipelineStage {
	public:
		explicit MyZipStage(int threadcnt) : PipelineStage(threadcnt) {
			this->m_step_name = "ZipInputData";
		}
		void process(int threadno, PipelineQueueItem* task) override;
	};
	class MyWriteStage : public PipelineStage {
	public:
		MyWriteStage() : PipelineStage(0) {
			this->m_step_name = "WriteZipData";
		}
		void process(int threadno, PipelineQueueItem* task) override;
	};
	class MyPipeline : public PipelineProcessor {
	public:
		int zipThreads;
		MyPipeline() {
			using namespace std;
			int cpuCount = this->sysCpuCount();
			if (g_zipThreads > 0) {
				zipThreads = min(cpuCount, g_zipThreads);
			}
			else {
				zipThreads = min(cpuCount, 8);
			}
			this->m_silent = g_silentPipelineMsg;
			this->setQueueSize(8*zipThreads);
			this->add_step(new MyZipStage(zipThreads));
			this->add_step(new MyWriteStage());
			this->compile();
			g_isPipelineStarted = true;
		}
		~MyPipeline() {
			this->stop();
			this->wait();
		}
	};
	static MyPipeline& getPipeline() { static MyPipeline p; return p; }

	struct HashTable {
		valvec<uint32_t> hashLink;
		valvec<uint32_t> hashTabl;
        EntropyContext eCtx;
	//	size_t  paddingForSeparateThreads[10]; // insignificant
	};
	valvec<HashTable> m_hash;
	size_t m_inputRecords;
	MyTask* m_curTask;
	valvec<MyTask*> m_lake;
	size_t m_lakeBytes = 0;

public:
	explicit MultiThread(const DictZipBlobStore::Options& opt)
	: DictZipBlobStoreBuilder(opt) {
		if (opt.enableLake) {
			m_lake.reserve(getPipeline().getQueueSize());
		}
	}
	void finishZip() override {
		if (m_opt.enableLake) {
			if (m_curTask && m_curTask->ibuf.size() > 0) {
				m_lake.push_back(m_curTask);
				m_curTask = nullptr;
			}
			drainLake();
		} else {
			if (m_curTask && m_curTask->ibuf.size() > 0) {
				getPipeline().enqueue(m_curTask);
			}
		}
		while (m_lengthCount < m_inputRecords) {
#if defined(_WIN32) || defined(_WIN64)
			::Sleep(20);
#else
			::usleep(20000);
#endif
		//  pre g++-4.8 has no sleep_for
		//	std::this_thread::sleep_for(std::chrono::microseconds(20));
		}
		m_curTask = NULL;
		m_hash.clear();
	}
	void prepareZip() override {
		m_inputRecords = 0;
		m_curTask = new MyTask(this, m_inputRecords);
		m_hash.resize(getPipeline().zipThreads);
        m_xxhash64.reset(g_dzbsnark_seed);
	}

	void drainLake() {
		auto& pipeline = getPipeline();
		{
			// serialize pipeline enqueue
			//
			// when there are multiple builders concurrently addRecord,
			// the compressing stage in pipeline will work concurrently
			// for these builders, each builder consumes many CPU Cache
			// especially L3 cache which shared by multiple CPU core on
			// one CPU socket/die!
			pipeline.enqueue((PipelineTask**)m_lake.data(), m_lake.size());
		}
		m_lake.erase_all();
		m_lakeBytes = 0;
	}

	void addRecord(const byte* rData, size_t rSize) override {
	  static const size_t lake_MAX_BYTES = getPipeline().zipThreads * 1024 * 1024;
		MyTask* task = m_curTask;
		if (task->ibuf.strpool.size() > 0 &&
			task->ibuf.strpool.unused() < rSize) {
			if (m_opt.enableLake) {
				if (m_lake.size() == m_lake.capacity() || m_lakeBytes >= lake_MAX_BYTES) {
					drainLake();
				}
				m_lakeBytes += task->ibuf.strpool.size();
				m_lake.push_back(task);
			} else {
				getPipeline().enqueue(task);
			}
			m_curTask = task = new MyTask(this, m_inputRecords);
		}
		task->ibuf.emplace_back((const char*)rData, rSize);
		this->m_unzipSize += rSize;
		this->m_inputRecords++;
	}
};

void
DictZipBlobStoreBuilder::MultiThread::
MyZipStage::process(int tno, PipelineQueueItem* item) {
	MyTask* task = static_cast<MyTask*>(item->task);
	auto  builder = task->builder;
	auto& hash = builder->m_hash[tno];
	assert(tno < (int)builder->m_hash.size());
	assert(task->offsets.size() == 0);
	assert(task->obuf.tell() == 0);
	// reserve 1/2 of input data for zipped data memory
	task->obuf.resize(task->ibuf.strpool.size() / 2);
	task->offsets.reserve(task->ibuf.size());
	if (builder->m_entropyZipDataBase) {
		for(size_t i = 0; i < task->ibuf.size(); ++i) {
			fstring rec = task->ibuf[i];
			bool zip = builder->entropyZip(rec.udata(), rec.size(),
				hash.eCtx, task->obuf);
			task->entropyBitmap.push_back(zip);
			task->offsets.push_back(task->obuf.tell());
		}
	}
	else {
		for(size_t i = 0; i < task->ibuf.size(); ++i) {
			fstring rec = task->ibuf[i];
			builder->zipRecord(rec.udata(), rec.size(),
				hash.hashTabl, hash.hashLink, task->obuf);
			task->offsets.push_back(task->obuf.tell());
		}
	}
}

void
DictZipBlobStoreBuilder::MultiThread::
MyWriteStage::process(int tno, PipelineQueueItem* item) {
	MyTask* task = static_cast<MyTask*>(item->task);
	auto builder = task->builder;
	auto taskOffsets   = task->offsets.data();
	size_t taskZipSize = task->obuf.tell();
	size_t baseZipSize = builder->m_zipDataSize;
	assert(0 == tno);
	assert(task->offsets.size() == task->ibuf.size());
    auto taskRecNum = task->offsets.size();
	if (byte* ebase = builder->m_entropyZipDataBase) {
		assert(builder->m_opt.kNoEntropy != builder->m_opt.entropyAlgo);
		memcpy(ebase + baseZipSize, task->obuf.begin(), taskZipSize);
        builder->m_entropyBitmap.append(task->entropyBitmap);
		builder->m_xxhash64.update(task->obuf.begin(), taskZipSize);
	}
	else {
		if (builder->m_opt.kNoEntropy == builder->m_opt.entropyAlgo) {
			builder->m_xxhash64.update(task->obuf.begin(), taskZipSize);
		}
        builder->m_fpWriter.ensureWrite(task->obuf.begin(), taskZipSize);
	}
#if 0
	fprintf(stderr
		, "DictZip::WriteStage: baseZipSize=%zd taskZipSize=%zd taskRecNum=%zd firstRecId=%zd\n"
		, baseZipSize, taskZipSize, taskRecNum, task->firstRecId
		);
#endif
#if !defined(NDEBUG)
	for (size_t i = 1; i < taskRecNum; ++i) {
		assert(taskOffsets[i-1] <= taskOffsets[i]); // can be empty
	}
#endif
    if (builder->m_freq_hist) {
        auto freq_hist = builder->m_freq_hist;
        freq_hist->add_record(fstring(task->obuf.begin(), taskOffsets[0]));
        for (size_t i = 1; i < taskRecNum; ++i) {
            freq_hist->add_record(fstring(task->obuf.begin() + taskOffsets[i - 1],
                taskOffsets[i] - taskOffsets[i - 1]));
        }
    }
    builder->m_lengthWriter << var_uint64_t(taskOffsets[0]);
	for (size_t i = 1; i < taskRecNum; ++i) {
        builder->m_lengthWriter << var_uint64_t(taskOffsets[i] - taskOffsets[i - 1]);
	}
	builder->m_zipDataSize += taskZipSize;
    builder->m_lengthCount += taskRecNum;
}

bool
DictZipBlobStoreBuilder::entropyZip(const byte* rData, size_t rSize,
			EntropyContext& context,
			NativeDataOutput<AutoGrownMemIO>& dio) {
	if (0 == rSize) {
		return false;
	}
	size_t dsize = rSize;
	if (m_opt.checksumLevel == 2) {
		assert(rSize > 4);
		dsize -= 4;
		uint32_t crc1 = unaligned_load<uint32_t>(rData + dsize);
		uint32_t crc2 = Crc32c_update(0, rData, dsize);
		if (crc2 != crc1) {
			throw BadCrc32cException(
				"DictZipBlobStoreBuilder::entropyZip", crc1, crc2);
		}
	}
    auto gtab = m_fse_gtable;
    auto zzn = size_t(0);
    const void* zzd = nullptr;
    if (m_opt.entropyAlgo == DictZipBlobStore::Options::kFSE) {
        context.buffer.ensure_capacity(FSE_compressBound(dsize));
        zzd = FSE_zip(rData, dsize, context.buffer.data(), context.buffer.capacity(), gtab, &zzn);
    }
    if (m_opt.entropyAlgo == DictZipBlobStore::Options::kHuffmanO1) {
        fstring bytes;
        switch (m_opt.entropyInterleaved) {
        default:
        case 8: bytes = m_huffman_encoder->encode_x8(fstring(rData, dsize), &context); break;
        case 4: bytes = m_huffman_encoder->encode_x4(fstring(rData, dsize), &context); break;
        case 2: bytes = m_huffman_encoder->encode_x2(fstring(rData, dsize), &context); break;
        case 1: bytes = m_huffman_encoder->encode_x1(fstring(rData, dsize), &context); break;
        }
        if (bytes.size() < dsize) {
            zzd = bytes.data();
            zzn = bytes.size();
        }
        else {
            zzd = nullptr;
        }
    }
    if (zzd) {
        dio.ensureWrite(zzd, zzn);
        if (m_opt.checksumLevel == 2) {
            uint32_t crc = Crc32c_update(0, zzd, zzn);
            dio << crc;
        }
        return true;
    }
    else {
        dio.ensureWrite(rData, rSize);
        return false;
    }
}

void
DictZipBlobStoreBuilder::zipRecord(const byte* rData, size_t rSize,
								   valvec<uint32_t>& hashTabl,
								   valvec<uint32_t>& hashLink,
								   NativeDataOutput<AutoGrownMemIO>& dio) {
	if (terark_unlikely(0 == rSize)) {
		return;
	}
	size_t oldsize = dio.tell();

	if (m_opt.useSuffixArrayLocalMatch)
		zipRecord_impl2<true>(rData, rSize, hashTabl, hashLink, dio);
	else
		zipRecord_impl2<false>(rData, rSize, hashTabl, hashLink, dio);

	if (m_opt.checksumLevel == 2) {
		assert(dio.tell() > oldsize);
		auto zdata = dio.begin() + oldsize;
		auto zsize = dio.tell()  - oldsize;
		uint32_t crc = Crc32c_update(0, zdata, zsize);
		dio << crc;
	}

	g_dataThroughBytes += rSize;
}

template<uint32_t LowerBytes>
static inline
void WriteUint(AutoGrownMemIO& dio, size_t x) {
    BOOST_STATIC_ASSERT(LowerBytes <= sizeof(x));
    dio.ensureWrite(&x, LowerBytes);
}

static inline
int GlobalMatchEncLen(size_t matchlen, size_t refBits, size_t maxShortLen) {
    int refBytes = refBits <= 24 ? 3 : 4;
    if (matchlen <= maxShortLen) {
        return refBytes + 1;
    }
    if (matchlen <= maxShortLen + 127) {
        return refBytes + 2;
    }
    if (matchlen <= maxShortLen + 128 * 128 - 1) {
        return refBytes + 3;
    }
    if (matchlen <= maxShortLen + 128 * 128 * 128 - 1) {
        return refBytes + 4;
    }
    if (matchlen <= maxShortLen + 128 * 128 * 128 * 128 - 1) {
        return refBytes + 5;
    }
    return refBytes + 6;
}

#if 0
#   define DzType_Trace printf
#   define DzType_Flush fflush
#else
#   define DzType_Trace(...)
#   define DzType_Flush(...)
#endif

template<bool UseSuffixArrayLocalMatch>
void
DictZipBlobStoreBuilder::zipRecord_impl2(const byte* rData, size_t rSize,
                   valvec<uint32_t>& hashTabl,
                   valvec<uint32_t>& hashLink,
                   NativeDataOutput<AutoGrownMemIO>& dio) {
#if !defined(NDEBUG)
    size_t old_dio_pos = dio.tell();
#endif
    size_t MAX_PROBE = m_opt.maxMatchProbe;
    uint32_t const nil = UINT32_MAX;
    unsigned const bits = 1 == rSize ? 1 : My_bsr_size_t(rSize - 1) + 1;
    unsigned const shift = 32 - bits;
    if (UseSuffixArrayLocalMatch) {
        BOOST_STATIC_ASSERT(sizeof(saidx_t) == sizeof(hashTabl[0]));
        hashTabl.resize_no_init(rSize);
        hashLink.resize_no_init(rSize);
        divsufsort(rData, (saidx_t*)hashTabl.data(), rSize, 0);
        auto local_sa = hashTabl.data();
        auto local_rsa = hashLink.data(); // reverse sa
        for (size_t i = 0; i < rSize; ++i) {
            local_rsa[local_sa[i]] = i;
        }
    }
    else {
        hashTabl.resize_fill(size_t(1) << bits, nil);
        hashLink.resize_fill(rSize, nil);
    }
    DzType_Trace("Compress %zd\n", rSize);
    size_t literal_len = 0;
    auto emitLiteral = [&](size_t j) {
        const byte* curptr = rData + j;
        for (; literal_len >= 32; literal_len -= 32) {
            dio << byte_t(byte_t(DzType::Literal) | (31 << 3));
            dio.ensureWrite(curptr - literal_len, 32);
            DzType_Trace("%zd Literal 32\n", j - literal_len);
        }
        if (literal_len) {
            dio << byte_t(byte_t(DzType::Literal) | ((literal_len - 1) << 3));
            dio.ensureWrite(curptr - literal_len, literal_len);
            DzType_Trace("%zd Literal %zd\n", j - literal_len, literal_len);
            literal_len = 0;
        }
    };
    const int* sa = m_dict->sa_data();
    auto bucket_or_sa = hashTabl.data();
    auto hpLink_or_rsa = hashLink.data();

#define gMinLen 5
    assert(m_strDict.size() <= INT32_MAX);
    auto gOffsetBits = My_bsr_size_t(m_strDict.size() - gMinLen) + 1;
    auto gLenBitsInOffset = (gOffsetBits <= 24 ? 24 : 32) - gOffsetBits;
    auto gShortLenBits = 5 + gLenBitsInOffset;
    auto gMaxShortLen = (size_t(1) << gShortLenBits) - 2 + gMinLen;

    for (size_t j = 0; j < rSize; ) {
        size_t localmatchPos = nil;
        intptr_t localmatchLen = 0;
        DzEncodingMeta localmatchEncMeta = { DzType::Literal, 2 };
        if (UseSuffixArrayLocalMatch) {
            size_t rank = hpLink_or_rsa[j];
            size_t minLoRank = rank > MAX_PROBE ? rank - MAX_PROBE : 0;
            for (size_t loRank = rank; loRank > minLoRank; ) {
                size_t loPos = bucket_or_sa[--loRank];
                if (loPos < j && terark_likely(j - loPos < 1L << 24)) {
                    intptr_t loLen = unmatchPos(rData + j, rData + loPos, 0, rSize - j);
                    if (loLen >= 2) {
                        DzEncodingMeta encMeta = GetBackRef_EncodingMeta(j - loPos, loLen);
                        if (loLen - encMeta.len > localmatchLen - localmatchEncMeta.len) {
                            localmatchPos = loPos;
                            localmatchLen = loLen;
                            localmatchEncMeta = encMeta;
                            break;
                        }
                    }
                }
            }
            size_t maxHiRank = std::min(rank + MAX_PROBE, rSize - 2);
            for (size_t hiRank = rank; hiRank < maxHiRank; ) {
                size_t hiPos = bucket_or_sa[++hiRank];
                if (hiPos < j && terark_likely(j - hiPos < 1L << 24)) {
                    intptr_t hiLen = unmatchPos(rData + j, rData + hiPos, 0, rSize - j);
                    if (hiLen >= 2) {
                        DzEncodingMeta encMeta = GetBackRef_EncodingMeta(j - hiPos, hiLen);
                        if (hiLen - encMeta.len > localmatchLen - localmatchEncMeta.len) {
                            localmatchPos = hiPos;
                            localmatchLen = hiLen;
                            localmatchEncMeta = encMeta;
                            break;
                        }
                    }
                }
            }
        }
        else if (j + 4 <= rSize) {
            size_t hVal = HashPtr(rData + j, shift);
            size_t hPos = bucket_or_sa[hVal];
            size_t probe = 0;
            while (probe < MAX_PROBE && hPos != nil && terark_likely(j - hPos < 1L << 24)) {
                intptr_t len = unmatchPos(rData + j, rData + hPos, 0, rSize - j);
                if (len >= 2) {
                    DzEncodingMeta encMeta = GetBackRef_EncodingMeta(j - hPos, len);
                    if (len - encMeta.len > localmatchLen - localmatchEncMeta.len) {
                        localmatchLen = len;
                        localmatchPos = hPos;
                        localmatchEncMeta = encMeta;
                    }
                }
                probe++;
                hPos = hpLink_or_rsa[hPos];
            }
            hpLink_or_rsa[j] = bucket_or_sa[hVal];
            bucket_or_sa[hVal] = j;
        }
        auto gMatch = m_dict->da_match_max_length(rData + j, rSize - j);
        assert(memcmp(rData + j, m_dict->str(sa[gMatch.lo]), gMatch.depth) == 0);
        if (int(gMatch.depth) - GlobalMatchEncLen(gMatch.depth, gOffsetBits, gMaxShortLen) >
            std::max<int>(0, localmatchLen - localmatchEncMeta.len)) {
            assert(gMatch.depth >= gMinLen);
            emitLiteral(j);
            size_t encLen = gMatch.depth - gMinLen;
            uint32_t offset = sa[gMatch.lo];
            if (terark_likely(gMatch.depth <= gMaxShortLen)) {
                dio << byte_t(byte_t(DzType::Global) | (encLen << 3));
                if (gOffsetBits < 24) {
                    WriteUint<3>(dio, (offset << (24 - gOffsetBits)) | (encLen >> 5));
                }
                else if (gOffsetBits > 24) {
                    WriteUint<4>(dio, (offset << (32 - gOffsetBits)) | (encLen >> 5));
                }
                else { // gOffsetBits == 24
                    WriteUint<3>(dio, offset);
                }
            }
            else {
                dio << byte_t(byte_t(DzType::Global) | (31 << 3));
                if (gOffsetBits < 24) {
                    WriteUint<3>(dio, (offset << (24 - gOffsetBits)) | (0x00FFFFFFu >> gOffsetBits));
                }
                else if (gOffsetBits > 24) {
                    WriteUint<4>(dio, (offset << (32 - gOffsetBits)) | (0xFFFFFFFFu >> gOffsetBits));
                }
                else { // gOffsetBits == 24
                    WriteUint<3>(dio, offset);
                }
                dio << var_size_t(gMatch.depth - gMaxShortLen - 1);
            }
            DzType_Trace("%zd Global %d %zd\n", j, offset, gMatch.depth);
            j += gMatch.depth;
        }
        else {
            assert(j + localmatchLen <= rSize);
            if (localmatchLen <= localmatchEncMeta.len) {
                literal_len += 1;
                j += 1;
            }
            else {
                assert(DzType::Literal != localmatchEncMeta.type);
                emitLiteral(j);
                switch (localmatchEncMeta.type) {
                case DzType::Literal:
                case DzType::Global:
                    abort();
                    break;
                case DzType::RLE:
                    assert(localmatchLen >= 2);
                    assert(localmatchLen <= 33);
                    assert(j - localmatchPos == 1);
                    dio << byte_t(byte_t(DzType::RLE) | ((localmatchLen - 2) << 3));
                    DzType_Trace("%zd RLE %zd\n", j, localmatchLen);
                    break;
                case DzType::NearShort:
                    assert(localmatchLen >= 2);
                    assert(localmatchLen <= 5);
                    assert(j - localmatchPos >= 2);
                    assert(j - localmatchPos <= 9);
                    dio << byte_t(byte_t(DzType::NearShort)
                        | ((localmatchLen - 2) << 3)
                        | ((j - localmatchPos - 2) << 5)
                    );
                    DzType_Trace("%zd NearShort %zd %zd\n", j, j - localmatchPos, localmatchLen);
                    break;
                case DzType::Far1Short:
                    assert(localmatchLen >= 2);
                    assert(localmatchLen <= 33);
                    assert(j - localmatchPos >= 2);
                    assert(j - localmatchPos <= 257);
                    dio << byte_t(byte_t(DzType::Far1Short) | ((localmatchLen - 2) << 3));
                    dio << byte_t(j - localmatchPos - 2);
                    DzType_Trace("%zd Far1Short %zd %zd\n", j, j - localmatchPos, localmatchLen);
                    break;
                case DzType::Far2Short:
                    assert(localmatchLen >= 2);
                    assert(localmatchLen <= 33);
                    assert(j - localmatchPos >= 258);
                    assert(j - localmatchPos <= 258 + 65535);
                    dio << byte_t(byte_t(DzType::Far2Short) | ((localmatchLen - 2) << 3));
                    dio << uint16_t(j - localmatchPos - 258);
                    DzType_Trace("%zd Far2Short %zd %zd\n", j, j - localmatchPos, localmatchLen);
                    break;
                case DzType::Far2Long:
                    assert(localmatchLen >= 2);
                    assert(j - localmatchPos <= 65535);
                    if (terark_likely(localmatchLen <= 34 + 30)) {
                        dio << byte_t(byte_t(DzType::Far2Long) | ((localmatchLen - 34) << 3));
                    }
                    else {
                        dio << byte_t(byte_t(DzType::Far2Long) | (31 << 3));
                        dio << var_size_t(localmatchLen - 65);
                    }
                    dio << uint16_t(j - localmatchPos);
                    DzType_Trace("%zd Far2Long %zd %zd\n", j, j - localmatchPos, localmatchLen);
                    break;
                case DzType::Far3Long:
                    assert(j - localmatchPos < 1L << 24);
                    if (terark_likely(localmatchLen <= 35)) {
                        dio << byte_t(byte_t(DzType::Far3Long) | ((localmatchLen - 5) << 3));
                    }
                    else {
                        dio << byte_t(byte_t(DzType::Far3Long) | (31 << 3));
                        dio << var_size_t(localmatchLen - 36);
                    }
                    WriteUint<3>(dio, j - localmatchPos);
                    DzType_Trace("%zd Far3Long %zd %zd\n", j, j - localmatchPos, localmatchLen);
                    break;
                }
                j += localmatchLen;
            }
        }
    }
    emitLiteral(rSize);
    DzType_Flush(stdout);
#if !defined(NDEBUG) && 0
    valvec<byte_t> tmpBuf(rSize, valvec_reserve());
    DoUnzip(dio.begin() + old_dio_pos, dio.current(),
        &tmpBuf, m_strDict.data(), m_strDict.size(), 1);
    const byte* p2 = tmpBuf.data();
    for (size_t i = 0; i < rSize; ++i) {
        assert(rData[i] == p2[i]);
    }
    assert(tmpBuf.size() == rSize);
#endif
}

DictZipBlobStore::ZipBuilder::~ZipBuilder() {}

DictZipBlobStore::Options::Options() {
	checksumLevel = 1;
	maxMatchProbe = getEnvLong("DictZipBlobStore_MAX_PROBE", 0);
	entropyAlgo = kNoEntropy;
	sampleSort = kSortNone;
	useSuffixArrayLocalMatch = false;
	useNewRefEncoding = true;
	compressGlobalDict = false;
    entropyInterleaved = (uint8_t)getEnvLong("Entropy_interleaved", 8);
	offsetArrayBlockUnits = 0; // default use UintVecMin0
    entropyZipRatioRequire = (float)getEnvDouble("Entropy_zipRatioRequire", 0.95);
    embeddedDict = false;
    enableLake = false;
}

DictZipBlobStore::ZipStat::ZipStat() {
	sampleTime = 0;
	dictBuildTime = 0;
	dictZipTime = 0;
	dictFileTime = 0;
	entropyBuildTime = 0;
	entropyZipTime = 0;
	pipelineThroughBytes = 0;
}

void DictZipBlobStore::ZipStat::print(FILE* fp) const {
	double  sum = 0
				+ sampleTime
				+ dictBuildTime
				+ dictZipTime
				+ dictFileTime
				+ entropyBuildTime
				+ entropyZipTime
                + embedDictTime
				;
	fprintf(fp, "DictZipBlobStore::ZipState: timing(seconds):\n");
	fprintf(fp, "             time seconds           %%\n");
	fprintf(fp, "  sample        %9.3f     %6.2f%%\n", sampleTime      , 100*sampleTime      /sum);
	fprintf(fp, "  dictBuild     %9.3f     %6.2f%%\n", dictBuildTime   , 100*dictBuildTime   /sum);
	fprintf(fp, "  dictZip       %9.3f     %6.2f%%\n", dictZipTime     , 100*dictZipTime     /sum);
	fprintf(fp, "  dictFile      %9.3f     %6.2f%%\n", dictFileTime    , 100*dictFileTime    /sum);
	fprintf(fp, "  entropyBuild  %9.3f     %6.2f%%\n", entropyBuildTime, 100*entropyBuildTime/sum);
	fprintf(fp, "  entropyZip    %9.3f     %6.2f%%\n", entropyZipTime  , 100*entropyZipTime  /sum);
    fprintf(fp, "  embedDict     %9.3f     %6.2f%%\n", embedDictTime   , 100*embedDictTime   /sum);
	fprintf(fp, "  sum of all    %9.3f     %6.2f%%\n", sum, 100.0);
	fprintf(fp, "-----------------------------------------\n");
}

///@param crc32cLevel
///          0: crc nothing
///          1: crc header and offset array
///          2: crc header and offset array, and each record
DictZipBlobStore::ZipBuilder*
DictZipBlobStore::createZipBuilder(const Options& opt) {
#if 0
	fprintf(stderr
		, "DictZipBlobStore: checksumLevel = %d, useSuffixArrayLocalMatch = %d, MAX_PROBE = %d\n"
		, opt.checksumLevel, opt.useSuffixArrayLocalMatch, opt.maxMatchProbe
		);
#endif
	// g_zipThreads == 0 indicate SingleThread
	if (g_zipThreads)
		return new DictZipBlobStoreBuilder::MultiThread(opt);
	else
		return new DictZipBlobStoreBuilder::SingleThread(opt);
}

DictZipBlobStore::MyBuilder::~MyBuilder() {
}

AbstractBlobStore::Builder*
DictZipBlobStore::MyBuilder::getPreBuilder() const {
    abort(); // not implemented
    return NULL;
}

void DictZipBlobStore::MyBuilder::addRecord(fstring rec) {
    abort(); // not implemented
}

void DictZipBlobStore::MyBuilder::finish() {
    abort(); // not implemented
}

// |--------------------------------------|
// | herder                               |
// |--------------------------------------|
// | data                                 |
// |--------------------------------------|
// | offset                               |
// |--------------------------------------|
// | entropy bitmap (if entropyAlgo != 0) |
// |--------------------------------------|
// | entropy DTable (if entropyAlgo != 0) |
// |--------------------------------------|
// | dict      (if embeddedDictType != 0) |
// |--------------------------------------|
// | footer                               |
// |--------------------------------------|

#pragma pack(push,8)
struct DictZipBlobStore::FileHeader : public FileHeaderBase {
	uint64_t offsetArrayBytes;
	uint64_t ptrListBytes;
    uint08_t embeddedDict : 4;
    uint08_t embeddedDictAligned : 4;
    uint08_t pad0[3];
	uint32_t entropyTableSize;
	uint08_t offsetsUintBits;
	uint08_t crc32cLevel;
	uint08_t entropyAlgo;
	uint08_t isNewRefEncoding : 1;
	uint08_t compressGlobalDictReserved : 1;
	uint08_t pad1 : 2;
	uint08_t zipOffsets_log2_blockUnits : 4; // 6 or 7
	uint32_t entropyTableCRC;
	uint64_t dictXXHash;
	uint32_t offsetsCRC;
	uint32_t headerCRC;

	FileHeader&
	patchCRC(const UintVecMin0& offsets, fstring entropyBitmap, fstring entropy, size_t maxOffsetEnt) {
        if (entropyAlgo != Options::kNoEntropy) {
            entropyTableCRC = Crc32c_update(0, entropyBitmap.data(), entropyBitmap.size());
            entropyTableCRC = Crc32c_update(entropyTableCRC, entropy.data(), entropy.size());
        }
        auto& suv = reinterpret_cast<const SortedUintVec&>(offsets);
        if (suv.isSortedUintVec()) {
            this->zipOffsets_log2_blockUnits = suv.log2_block_units();
        }
        records = offsets.size() - 1;
        offsetArrayBytes = offsets.mem_size();
        offsetsUintBits = maxOffsetEnt ? 1 + terark_bsr_u64(maxOffsetEnt) : 0;
        fileSize = computeFileSize();
        offsetsCRC = Crc32c_update(0, offsets.data(), offsets.mem_size());
        headerCRC = Crc32c_update(0, this, sizeof(*this) - 4);
        return *this;
	}

	FileHeader(const DictZipBlobStore* store, size_t zipDataSize1, Dictionary dict
             , const UintVecMin0& offsets, fstring entropyBitmap, fstring entropyTab
             , size_t maxOffsetEnt) {
		assert(dict.memory.size() > 0);
		memset(this, 0, sizeof(*this));
		magic_len = MagicStrLen;
		strcpy(magic, MagicString);
		strcpy(className, "DictZipBlobStore");
		formatVersion = 1;
		unzipSize  = store->m_unzipSize;
		ptrListBytes = (zipDataSize1 + 15) & ~uint64_t(15);
        embeddedDict = 0;
        embeddedDictAligned = 0;
        entropyTableSize = entropyTab.size();
		crc32cLevel = byte_t(store->m_checksumLevel);
		entropyAlgo = byte_t(store->m_entropyAlgo);
		isNewRefEncoding = 1; // now always 1
		globalDictSize = dict.memory.size();
		dictXXHash = dict.xxhash;
		patchCRC(offsets, entropyBitmap, entropyTab, maxOffsetEnt);
	}

    void setEmbeddedDictType(size_t dictSize, EmbeddedDictType embeddedDictType) {
        size_t alignedDictSize = align_up(dictSize, 16);
        fileSize += alignedDictSize;
        embeddedDict = (uint8_t)embeddedDictType;
        embeddedDictAligned = alignedDictSize - dictSize;
        headerCRC = Crc32c_update(0, this, sizeof(*this) - 4);
    }

	uint64_t computeFileSize() const {
		size_t offsetBytes = UintVecMin0::compute_mem_size(offsetsUintBits, records+1);
		if (zipOffsets_log2_blockUnits) {
			offsetBytes = offsetArrayBytes;
		} else {
			assert(offsetArrayBytes == offsetBytes);
		}
        size_t entropyTableAlignedSize = align_up(entropyTableSize, 16);
        size_t entropyBitmapSIze = entropyAlgo == Options::kNoEntropy ? 0 :
            febitvec::s_mem_size(align_up(records, 16 * 8));
        return sizeof(FileHeader) + ptrListBytes + offsetBytes + entropyBitmapSIze
            + entropyTableAlignedSize + sizeof(BlobStoreFileFooter);
	}

    fstring getEmbeddedDict() const {
        size_t alignedSize = fileSize - computeFileSize();
        size_t size = alignedSize - embeddedDictAligned;
        byte_t* end = (byte_t*)(this) + fileSize;
        return fstring(end - alignedSize - sizeof(BlobStoreFileFooter), size);
    }

	BlobStoreFileFooter* getFileFooter() const {
		return (BlobStoreFileFooter*)((byte_t*)(this) + fileSize) - 1;
	}
};
#pragma pack(pop)
BOOST_STATIC_ASSERT(sizeof(DictZipBlobStore::FileHeader) == 128);

void DictZipBlobStoreBuilder::prepare(size_t records, FileMemIO& mem) {
    assert(!m_strDict.empty());
    m_prepareStartTime = g_pf.now();
    prepareDict();
    m_fpOffset = 0;
    m_fpath.clear();
    m_fpathForLength.clear();
    m_memStream = SeekableStreamWrapper<FileMemIO*>(&mem);
    m_fpWriter.attach(&m_memStream);
    assert(!m_fpDelta.isOpen());
    m_memLengthStream.rewind();
    m_lengthCount = 0;
    m_lengthWriter.attach(&m_memLengthStream);

    const static char zeros[sizeof(FileHeader)] = "";
    m_fpWriter.ensureWrite(&zeros, sizeof(zeros));
    initWarn();
    if (m_huffman_encoder) {
        delete m_huffman_encoder;
        m_huffman_encoder = nullptr;
    }
    if (m_freq_hist) {
        delete m_freq_hist;
        m_freq_hist = nullptr;
    }
    if (m_opt.entropyAlgo != DictZipBlobStore::Options::kNoEntropy) {
        m_freq_hist = new freq_hist_o1();
    }
    m_unzipSize = 0;
    m_zipDataSize = 0;
    m_entropyZipDataBase = nullptr;
    m_dictZipStartTime = g_pf.now();
    m_dataThroughBytesAtBegin = g_dataThroughBytes;
    prepareZip();
}

void DictZipBlobStoreBuilder::prepare(size_t records, fstring fpath) {
    prepare(records, fpath, 0);
}

void DictZipBlobStoreBuilder::prepare(size_t records, fstring fpath, size_t offset) {
    assert(!m_strDict.empty());
    m_prepareStartTime = g_pf.now();
    prepareDict();
    m_fpOffset = offset;
    m_fpath.assign(fpath.data(), fpath.size());
    m_fpathForLength = m_fpath + ".offset";
    if (m_fpOffset == 0) {
        m_fp.open(m_fpath.c_str(), "wb");
    }
    else {
        m_fp.open(m_fpath.c_str(), "rb+");
        m_fp.seek(m_fpOffset);
    }
    m_fp.disbuf();
    m_fpWriter.attach(&m_fp);
    assert(!m_fpDelta.isOpen());
    m_fpDelta.open(m_fpathForLength.c_str(), "wb+");
    m_fpDelta.disbuf();
    m_lengthCount = 0;
    m_lengthWriter.attach(&m_fpDelta);

    const static char zeros[sizeof(FileHeader)] = "";
    m_fpWriter.ensureWrite(&zeros, sizeof(zeros));
    initWarn();
    if (m_huffman_encoder) {
        delete m_huffman_encoder;
        m_huffman_encoder = nullptr;
    }
    if (m_freq_hist) {
        delete m_freq_hist;
        m_freq_hist = nullptr;
    }
    if (m_opt.entropyAlgo != DictZipBlobStore::Options::kNoEntropy) {
        m_freq_hist = new freq_hist_o1();
    }
    m_unzipSize = 0;
    m_zipDataSize = 0;
    m_entropyZipDataBase = nullptr;
    m_dictZipStartTime = g_pf.now();
    m_dataThroughBytesAtBegin = g_dataThroughBytes;
    prepareZip();
}

static
std::pair<bool, size_t>
WriteDict(fstring filename, size_t offset, fstring data, bool compress) {
    FileStream(filename, "ab+").chsize(offset + data.size());
    MmapWholeFile dictFile(filename, true);
    if (compress) {
        size_t zstd_size = ZSTD_compress((byte_t*)dictFile.base + offset, data.size(),
                                         data.data(), data.size(), 0);
        if (!ZSTD_isError(zstd_size)) {
            MmapWholeFile().swap(dictFile);
            FileStream(filename, "rb+").chsize(offset + zstd_size);
            return { true, zstd_size };
        }
    }
    memcpy((byte_t*)dictFile.base + offset, data.data(), data.size());
    return { false, data.size() };
}

static 
AbstractBlobStore::MemoryCloseType
ReadDict(fstring mem, AbstractBlobStore::Dictionary& dict, fstring dictFile) {
    auto MmapColdizeBytes = [](const void* addr, size_t len) {
        size_t low = terark::align_up(size_t(addr), 4096);
        size_t hig = terark::align_down(size_t(addr) + len, 4096);
        if (low < hig) {
            size_t size = hig - low;
#ifdef POSIX_MADV_DONTNEED
            posix_madvise((void*)low, size, POSIX_MADV_DONTNEED);
#elif defined(_MSC_VER) // defined(_WIN32) || defined(_WIN64)
            //VirtualFree((void*)low, size, MEM_DECOMMIT);
#endif
        }
    };
    auto mmapBase = (const DictZipBlobStore::FileHeader*)mem.data();
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::External) {
        if (!dict.memory.empty()) {
            return AbstractBlobStore::MemoryCloseType::RiskRelease;
        }
        if (dictFile.empty()) {
            THROW_STD(logic_error
                , "DictZipBlobStore::ReadDict() missing dict");
        }
        MmapWholeFile dictMmap(dictFile);
        do {
            unsigned long long raw_size = ZSTD_getDecompressedSize(dictMmap.base, dictMmap.size);
            if (raw_size != mmapBase->globalDictSize) {
                break;
            }
            valvec<byte_t> output_dict;
            use_hugepage_resize_no_init(&output_dict, raw_size);
            size_t size = ZSTD_decompress(output_dict.data(), raw_size, dictMmap.base, dictMmap.size);
            if (ZSTD_isError(size)) {
                break;
            }
            assert(size == raw_size);
            dict = AbstractBlobStore::Dictionary(output_dict);
            if (mmapBase->formatVersion > 0 &&
                mmapBase->dictXXHash != dict.xxhash) {
                break;
            }
            output_dict.risk_release_ownership();
            return AbstractBlobStore::MemoryCloseType::Clear;
        } while (false);
        dict = AbstractBlobStore::Dictionary(dictMmap.memory());
        dictMmap.base = nullptr;
        return AbstractBlobStore::MemoryCloseType::MmapClose;
    }
    fstring dictMem = mmapBase->getEmbeddedDict();
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::Raw) {
        assert(dictMem.size() == mmapBase->globalDictSize);
        dict = AbstractBlobStore::Dictionary(dictMem);
        return AbstractBlobStore::MemoryCloseType::RiskRelease;
    }
    if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::ZSTD) {
        THROW_STD(logic_error
            , "DictZipBlobStore::ReadDict() bad embeddedDict type");
    }
    valvec<byte_t> output_dict;
    unsigned long long raw_size = ZSTD_getDecompressedSize(dictMem.data(), dictMem.size());
    if (raw_size == 0) {
        THROW_STD(logic_error
            , "DictZipBlobStore::ReadDict() Load global dict error: zstd get raw size fail");
    }
    use_hugepage_resize_no_init(&output_dict, raw_size);
    size_t size = ZSTD_decompress(output_dict.data(), raw_size, dictMem.data(), dictMem.size());
    if (ZSTD_isError(size)) {
        THROW_STD(logic_error
            , "DictZipBlobStore::ReadDict() Load global dict ZSTD error: %s"
            , ZSTD_getErrorName(size));
    }
    assert(size == raw_size);
    MmapColdizeBytes(dictMem.data(), dictMem.size());
    dict = AbstractBlobStore::Dictionary(output_dict);
    output_dict.risk_release_ownership();
    return AbstractBlobStore::MemoryCloseType::Clear;
}

struct InplaceUpdateBuffer {
    byte_t* m_begin;
    byte_t* m_end;
    byte_t* m_pos;
    struct RingBuffer {
        valvec<byte_t> m_buffer;
        size_t begin;
        size_t end;
    } m_buffer;
};

void DictZipBlobStoreBuilder::entropyStore(std::unique_ptr<terark::DictZipBlobStore> &store, terark::ullong &t2, terark::ullong &t3)
{
    TERARK_SCOPE_EXIT(
        if (m_fse_gtable) {
            FSE_freeCTable(m_fse_gtable);
            m_fse_gtable = NULL;
        }
        if (m_huffman_encoder) {
            delete m_huffman_encoder;
            m_huffman_encoder = NULL;
        }
        m_entropyBitmap.clear();
    );
    const bool writable = true;
    MmapWholeFile fmmap;
    if (!m_fpath.empty()) {
        MmapWholeFile(m_fpath, writable).swap(fmmap);
        store->setDataMemory((byte*)fmmap.base + m_fpOffset, fmmap.size - m_fpOffset);
    }
    else {
        store->setDataMemory(m_memStream.stream()->begin(), m_memStream.size());
    }
    t2 = g_pf.now();
    byte* storeBase = m_entropyZipDataBase = store->m_ptrList.data();
    assert(m_freq_hist);
    m_freq_hist->finish();
    bool unnecessary = false;
    if (m_opt.entropyAlgo == DictZipBlobStore::Options::kFSE) {
        struct context_t {
            unsigned freq[256];
            short norm[256];
            freq_hist::histogram_t hist;
        };
        std::unique_ptr<context_t> c(new context_t);
        unsigned maxSym = 255;
        unsigned maxCnt = 0;
        unsigned tabLog = (unsigned)getEnvLong("DictZipBlobStore_FSETableLog", 14);
        c->hist = m_freq_hist->histogram();
        if (c->hist.o0_size > std::numeric_limits<unsigned>::max()) {
            freq_hist::normalise_hist(c->hist.o0, c->hist.o0_size, std::numeric_limits<unsigned>::max());
        }
        size_t dataSize = c->hist.o0_size;
        uint64_t* o0 = c->hist.o0;
        for (size_t i = 0; i < 256; ++i) {
            c->freq[i] = o0[i];
            maxCnt = std::max<size_t>(maxCnt, o0[i]);
            if (o0[i] > 0) { maxSym = i; }
        }
        if (maxCnt == 1 // each symbol present maximum once => not compressible
            || maxCnt < (dataSize >> 7) // Heuristic : not compressible enough
            ) {
            unnecessary = true;
        }
        else {
            tabLog = FSE_optimalTableLog(tabLog, dataSize, maxSym);
            size_t err = FSE_normalizeCount(c->norm, tabLog, c->freq, dataSize, maxSym);
            if (FSE_isError(err)) {
                unnecessary = true;
            }
            else {
                m_fse_gtable = FSE_createCTable(tabLog, maxSym);
                err = FSE_buildCTable(m_fse_gtable, c->norm, maxSym, tabLog);
                if (FSE_isError(err)) {
                    fprintf(stderr
                        , "WARN: FSE_buildCTable() = %s, global entropy table will be disabled\n"
                        , FSE_getErrorName(err));
                    FSE_freeCTable(m_fse_gtable);
                    m_fse_gtable = NULL;
                    unnecessary = true;
                }
                else {
                    size_t size = align_up(FSE_NCountWriteBound(maxSym, tabLog), 16);
                    m_entropyTableData.resize_no_init(size);
                    m_entropyTableData.resize_no_init(
                        FSE_writeNCount(m_entropyTableData.data(), size, c->norm, maxSym, tabLog)
                    );
                }
            }
        }
    }
    if (m_opt.entropyAlgo == DictZipBlobStore::Options::kHuffmanO1) {
        size_t estimate_size = freq_hist_o1::estimate_size(m_freq_hist->histogram());
        if (estimate_size >= store->total_data_size() * m_opt.entropyZipRatioRequire) {
            unnecessary = true;
        }
        else {
            m_freq_hist->normalise(Huffman::NORMALISE);
            m_huffman_encoder = new Huffman::encoder_o1(m_freq_hist->histogram());
            m_huffman_encoder->take_table(&m_entropyTableData);
            switch (m_opt.entropyInterleaved) {
            case 1 : case 2: case 4: case 8:
                store->m_entropyInterleaved = m_opt.entropyInterleaved;
                break;
            default:
                store->m_entropyInterleaved = 8;
                break;
            }
            m_entropyTableData.push_back(store->m_entropyInterleaved);
        }
    }
    delete m_freq_hist;
    m_freq_hist = nullptr;
    if (unnecessary) {
        store->destroyMe();
        MmapWholeFile().swap(fmmap);
        return;
    }
    assert(m_entropyBitmap.size() == 0);
    assert(!m_fpDelta.isOpen());
    if (!m_fpath.empty()) {
        m_fpDelta.open(m_fpathForLength.c_str(), "wb+");
        m_fpDelta.disbuf();
        m_lengthCount = 0;
        m_lengthWriter.attach(&m_fpDelta);
    }
    else {
        m_memLengthStream.rewind();
        m_lengthCount = 0;
        m_lengthWriter.attach(&m_memLengthStream);
    }
    m_unzipSize = 0;
    m_zipDataSize = 0;
    t3 = g_pf.now();
    prepareZip();
    bool isOffsetsZipped = store->offsetsIsSortedUintVec();
    for (size_t recId = 0; recId < store->m_numRecords; ++recId) {
        size_t BegEnd[2];
        store->offsetGet2(recId, BegEnd, isOffsetsZipped);
        this->addRecord(storeBase + BegEnd[0], BegEnd[1] - BegEnd[0]);
    }
    this->finishZip();
    if (g_printEntropyCount) {
        size_t entropy_count = m_entropyBitmap.popcnt();
        fprintf(stderr, "DictZipBlobStoreBuilder::entropyStore():"
            "record = %zd , raw = %zd , entropy = %zd\n",
            store->m_numRecords, store->m_numRecords - entropy_count, entropy_count);
        fflush(stderr);
    }
    size_t pos = m_zipDataSize;
    if (pos % 16) {
        memset(storeBase + pos, 0, 16 - pos % 16);
        m_xxhash64.update(storeBase + pos, 16 - pos % 16);
        pos = (pos + 15) & ~15;
    }
    assert(m_lengthCount == m_entropyBitmap.size());
    m_entropyBitmap.resize(align_up(store->m_numRecords, 16 * 8));

    m_lengthWriter.flush_buffer();
    store->destroyMe();
    uint64_t maxOffsetEnt;
    UintVecMin0 zoffsets;
    TERARK_SCOPE_EXIT(zoffsets.risk_release_ownership());
    size_t storeSize = 0
        + sizeof(FileHeader)
        + pos // align_up(m_zipDataSize, 16)
        + m_entropyBitmap.mem_size()
        + align_up(m_entropyTableData.size(), 16)
        + sizeof(BlobStoreFileFooter);
    if (!m_fpath.empty()) {
        MmapWholeFile().swap(fmmap);
        m_fp.open(m_fpath, "rb+");
        m_fp.chsize(m_fpOffset + storeSize);
        m_fp.disbuf();
        m_fp.seek(m_fpOffset + sizeof(FileHeader) + pos);
        m_fpWriter.attach(&m_fp);
    }
    else {
        m_memStream.stream()->resize(storeSize);
        m_memStream.seek(sizeof(FileHeader) + pos);
        m_fpWriter.attach(&m_memStream);
    }
    {
        NativeDataInput<InputBuffer> input;
        if (!m_fpath.empty()) {
            m_fpDelta.rewind();
            input.attach(&m_fpDelta);
        }
        else {
            m_memLengthStream.rewind();
            input.attach(&m_memLengthStream);
        }
        size_t offsetBase = 0;
        var_uint64_t delta;
        if (!isOffsetsZipped) {
            std::unique_ptr<UintVecMin0::Builder> builder(
                UintVecMin0::create_builder_by_max_value(m_zipDataSize, &m_fpWriter));
            for (size_t i = 0; i < m_lengthCount; ++i) {
                builder->push_back(offsetBase);
                input >> delta;
                offsetBase += delta;
            }
            builder->push_back(maxOffsetEnt = offsetBase);
            auto result = builder->finish();
            storeSize += result.mem_size;
            m_fpWriter.flush_buffer();
            if (!m_fpath.empty()) {
                m_fp.chsize(m_fpOffset + storeSize);
                m_fp.close();
                MmapWholeFile(m_fpath, true).swap(fmmap);
                zoffsets.risk_set_data(
                    (byte_t*)fmmap.base + m_fpOffset + sizeof(FileHeader) + pos,
                    result.size, result.uintbits);
            }
            else {
                m_memStream.stream()->resize(storeSize);
                zoffsets.risk_set_data(
                    m_memStream.stream()->begin() + sizeof(FileHeader) + pos,
                    result.size, result.uintbits);
            }
        }
        else {
            std::unique_ptr<SortedUintVec::Builder> builder(
                SortedUintVec::createBuilder(m_opt.offsetArrayBlockUnits, &m_fpWriter));
            for (size_t i = 0; i < m_lengthCount; ++i) {
                builder->push_back(offsetBase);
                input >> delta;
                offsetBase += delta;
            }
            builder->push_back(maxOffsetEnt = offsetBase);
            size_t fileSize = builder->finish(nullptr).mem_size;
            storeSize += fileSize;
            m_fpWriter.flush_buffer();
            SortedUintVec szoffsets;
            if (!m_fpath.empty()) {
                m_fp.chsize(m_fpOffset + storeSize);
                m_fp.close();
                MmapWholeFile(m_fpath, true).swap(fmmap);
                szoffsets.risk_set_data(
                    (byte_t*)fmmap.base + m_fpOffset + sizeof(FileHeader) + pos,
                    fileSize);
            }
            else {
                m_memStream.stream()->resize(storeSize);
                szoffsets.risk_set_data(
                    m_memStream.stream()->begin() + sizeof(FileHeader) + pos,
                    fileSize);
            }
            memcpy(&zoffsets, &szoffsets, sizeof szoffsets);
            szoffsets.risk_release_ownership();
        }
        if (!m_fpath.empty()) {
            m_fpDelta.close();
            ::remove(m_fpathForLength.c_str());
        }
        else {
            m_memLengthStream.stream()->clear();
        }
    }
    assert(zoffsets.mem_size() % 16 == 0);
    FileHeader* hp;
    if (!m_fpath.empty()) {
        hp = (FileHeader*)((byte_t*)fmmap.base + m_fpOffset);
    }
    else {
        hp = (FileHeader*)m_memStream.stream()->begin();
    }
    storeBase = (byte_t*)hp + sizeof(FileHeader);
    hp->fileSize = storeSize;
    hp->offsetsUintBits = zoffsets.uintbits();
    hp->entropyAlgo = byte(m_opt.entropyAlgo);
    // febitvec
    //
    hp->entropyTableSize = m_entropyTableData.size();
    hp->ptrListBytes = align_up(m_zipDataSize, 16);
    hp->crc32cLevel = m_opt.checksumLevel;
    pos += zoffsets.mem_size();
    assert(pos % 16 == 0);
    //	m_entropyTableData.resize(align_up(m_entropyTableData.size(), 16), 0);
    memcpy(storeBase + pos, m_entropyBitmap.data(), m_entropyBitmap.mem_size());
    pos += m_entropyBitmap.mem_size();
    memcpy(storeBase + pos, m_entropyTableData.data(), m_entropyTableData.size());
    pos += hp->entropyTableSize;
    while (pos & 15)
        storeBase[pos++] = 0; // zero padding
    hp->patchCRC(zoffsets, fstring((char*)m_entropyBitmap.data(), m_entropyBitmap.mem_size()),
                 m_entropyTableData, maxOffsetEnt);
    auto foot = hp->getFileFooter();
    foot->reset();
    foot->zipDataXXHash = m_xxhash64.digest();
    assert(hp->computeFileSize() == storeSize);
    assert((byte_t*)(foot + 0) == storeBase + pos);
    assert((byte_t*)(foot + 1) == (byte_t*)(hp)+storeSize);
    if (!m_fpath.empty()) {
        MmapWholeFile().swap(fmmap); // destroy fmmap
        assert(FileStream(m_fpath, "rb+").fsize() == storeSize + m_fpOffset);
    }
    else {
        assert(m_memStream.size() == storeSize);
    }
}

void DictZipBlobStoreBuilder::EmbedDict(std::unique_ptr<terark::DictZipBlobStore> &store) {
    if (!m_fpath.empty()) {
        MmapWholeFile fmmap(m_fpath);
        FileHeader* hp = (FileHeader*)((byte_t*)fmmap.base + m_fpOffset);
        BlobStoreFileFooter footer = *hp->getFileFooter();
        size_t fileSize = m_fpOffset + hp->fileSize;
        MmapWholeFile().swap(fmmap);
        assert(fileSize == FileStream(m_fpath, "rb").fsize());
        auto result = WriteDict(m_fpath, fileSize - sizeof(BlobStoreFileFooter),
            m_strDict, m_opt.compressGlobalDict);
        fileSize += align_up(result.second, 16);
        FileStream(m_fpath, "rb+").chsize(fileSize);
        MmapWholeFile(m_fpath, true).swap(fmmap);
        hp = (FileHeader*)((byte_t*)fmmap.base + m_fpOffset);
        hp->setEmbeddedDictType(result.second,
            result.first ? EmbeddedDictType::ZSTD : EmbeddedDictType::Raw);
        *hp->getFileFooter() = footer;
    }
    else {
        size_t fileSize = m_memStream.size() + m_strDict.size();
        m_memStream.stream()->resize(fileSize);
        auto hp = (FileHeader*)m_memStream.stream()->begin();
        BlobStoreFileFooter footer = *hp->getFileFooter();
        assert(fileSize == hp->fileSize + m_strDict.size());
        if (m_opt.compressGlobalDict) {
            size_t zstd_size = ZSTD_compress((byte_t*)hp->getFileFooter(), m_strDict.size(),
                m_strDict.data(), m_strDict.size(), 0);
            if (!ZSTD_isError(zstd_size)) {
                hp->setEmbeddedDictType(zstd_size, EmbeddedDictType::ZSTD);
                *hp->getFileFooter() = footer;
                m_memStream.stream()->resize(hp->fileSize);
                return;
            }
        }
        memcpy((byte_t*)hp->getFileFooter(), m_strDict.data(), m_strDict.size());
        hp->setEmbeddedDictType(m_strDict.size(), EmbeddedDictType::Raw);
        m_memStream.stream()->resize(hp->fileSize);
        *hp->getFileFooter() = footer;
    }
}

void DictZipBlobStoreBuilder::finish(int flag) {
	finishZip();
	m_zipStat.pipelineThroughBytes = g_dataThroughBytes - m_dataThroughBytesAtBegin;
	ullong t1 = g_pf.now();
	std::unique_ptr<DictZipBlobStore> store(new DictZipBlobStore());
    if (flag & DictZipBlobStoreBuilder::FinishFreeDict) {
        m_dict.reset(); // free large memory
    }
    PadzeroForAlign<16>(m_fpWriter, m_xxhash64, m_zipDataSize);
    m_fpWriter.flush_buffer();
    m_lengthWriter.flush();
    size_t maxOffsetEnt;
    {
        size_t finalSize = m_fpOffset
            + sizeof(FileHeader)
            + align_up(m_zipDataSize, 16)
            + sizeof(BlobStoreFileFooter);
        MmapWholeFile mmapStore;
        TERARK_SCOPE_EXIT(
            store->m_offsets.risk_release_ownership();
        );
        NativeDataInput<InputBuffer> input;
        if (!m_fpath.empty()) {
            m_fpDelta.rewind();
            input.attach(&m_fpDelta);
        }
        else {
            m_memLengthStream.rewind();
            input.attach(&m_memLengthStream);
        }
        size_t offsetBase = 0;
        var_uint64_t delta;
        if (m_opt.offsetArrayBlockUnits) {
            std::unique_ptr<SortedUintVec::Builder> builder(
                SortedUintVec::createBuilder(m_opt.offsetArrayBlockUnits, &m_fpWriter));
            for (size_t i = 0; i < m_lengthCount; ++i) {
                builder->push_back(offsetBase);
                input >> delta;
                offsetBase += delta;
            }
            builder->push_back(offsetBase);
            size_t fileSize = builder->finish(nullptr).mem_size;
            finalSize += fileSize;
            m_fpWriter.flush_buffer();
            new(&store->m_zOffsets)SortedUintVec();
            if (!m_fpath.empty()) {
                m_fp.chsize(finalSize);
                m_fp.close();
                MmapWholeFile(m_fpath, true).swap(mmapStore);
                store->m_zOffsets.risk_set_data(
                    (byte_t*)mmapStore.base + m_fpOffset + sizeof(FileHeader) + align_up(m_zipDataSize, 16),
                    fileSize);
            }
            else {
                m_memStream.stream()->resize(finalSize);
                store->m_zOffsets.risk_set_data(
                    m_memStream.stream()->begin() + sizeof(FileHeader) + align_up(m_zipDataSize, 16),
                    fileSize);
            }
        }
        else {
            std::unique_ptr<UintVecMin0::Builder> builder(
                UintVecMin0::create_builder_by_max_value(m_zipDataSize, &m_fpWriter));
            for (size_t i = 0; i < m_lengthCount; ++i) {
                builder->push_back(offsetBase);
                input >> delta;
                offsetBase += delta;
            }
            builder->push_back(offsetBase);
            auto result = builder->finish();
            finalSize += result.mem_size;
            m_fpWriter.flush_buffer();
            new(&store->m_offsets)UintVecMin0();
            if (!m_fpath.empty()) {
                m_fp.chsize(finalSize);
                m_fp.close();
                MmapWholeFile(m_fpath, true).swap(mmapStore);
                store->m_offsets.risk_set_data(
                    (byte_t*)mmapStore.base + m_fpOffset + sizeof(FileHeader) + align_up(m_zipDataSize, 16),
                    result.size, result.uintbits);
            }
            else {
                m_memStream.stream()->resize(finalSize);
                store->m_offsets.risk_set_data(
                    m_memStream.stream()->begin() + sizeof(FileHeader) + align_up(m_zipDataSize, 16),
                    result.size, result.uintbits);
            }
        }
        maxOffsetEnt = offsetBase;
        if (!m_fpath.empty()) {
            m_fpDelta.close();
            ::remove(m_fpathForLength.c_str());
        }
        else {
            m_memLengthStream.stream()->clear();
        }

        store->m_entropyAlgo = DictZipBlobStore::Options::kNoEntropy;
        if (m_opt.checksumLevel <= 2 || m_opt.kNoEntropy == m_opt.entropyAlgo) {
            store->m_checksumLevel = m_opt.checksumLevel;
        }
        else {
            store->m_checksumLevel = 1;
        }
        store->m_fpath = m_fpath;
        store->m_unzipSize = m_unzipSize;
        store->m_numRecords = m_lengthCount;
        store->m_entropyInterleaved = m_opt.entropyInterleaved;

        assert(store->m_offsets.mem_size() % 16 == 0);
        if (flag & DictZipBlobStoreBuilder::FinishWriteDictFile && !m_opt.embeddedDict) {
            WriteDict(m_fpath + "-dict", 0, m_strDict, m_opt.compressGlobalDict);
        }
        fstring empty(""); // no entropy table now
        FileHeader* hp;
        if (!m_fpath.empty()) {
            hp = (FileHeader*)((byte_t*)mmapStore.base + m_fpOffset);
        }
        else {
            hp = (FileHeader*)m_memStream.stream()->begin();
        }
        *hp = FileHeader(&*store
            , m_zipDataSize
            , AbstractBlobStore::Dictionary(m_strDict.size(), m_dictXXHash)
            , store->m_offsets, empty, empty, maxOffsetEnt);
        BlobStoreFileFooter foot;
        if (m_opt.kNoEntropy == m_opt.entropyAlgo && m_opt.checksumLevel >= 3) {
            foot.zipDataXXHash = m_xxhash64.digest();
        }
        *hp->getFileFooter() = foot;
    }
	store->m_strDict.clear();
	store->m_offsets.clear();
	store->m_ptrList.clear();

	ullong t2 = 0, t3 = 0;

    if (DictZipBlobStore::Options::kNoEntropy != m_opt.entropyAlgo) {
        entropyStore(store, t2, t3);
    }
    else {
        t3 = t2 = g_pf.now();
    }
	ullong t4 = g_pf.now();
    if (m_opt.embeddedDict) {
        EmbedDict(store);
    }
    ullong t5 = g_pf.now();

	m_zipStat.dictBuildTime    = g_pf.sf(m_prepareStartTime, m_dictZipStartTime);
	m_zipStat.dictZipTime      = g_pf.sf(m_dictZipStartTime, t1);
	m_zipStat.dictFileTime     = g_pf.sf(t1, t2);
	m_zipStat.entropyBuildTime = g_pf.sf(t2, t3);
	m_zipStat.entropyZipTime   = g_pf.sf(t3, t4);
    m_zipStat.embedDictTime    = g_pf.sf(t4, t5);
}

void DictZipBlobStoreBuilder::abandon() {
    finishZip();
}

// when using user memory, disable global dict compression
void DictZipBlobStore::init_from_memory(fstring dataMem, Dictionary dict) {
    destroyMe();
    m_dictCloseType = ReadDict(dataMem, dict, m_fpath + "-dict");
    m_strDict.risk_set_data((byte*)dict.memory.data(), dict.memory.size());
    auto mmapBase = (const FileHeader*)dataMem.data();
    if (mmapBase->globalDictSize != dict.memory.size()) {
        THROW_STD(invalid_argument
            , "DictZipBlobStore bad dict size: wire = %lld , real = %lld]"
            , llong(mmapBase->globalDictSize)
            , llong(dict.memory.size())
        );
    }
    if (isChecksumVerifyEnabled() && mmapBase->formatVersion >= 1 &&
        mmapBase->dictXXHash != dict.xxhash) {
        THROW_STD(invalid_argument
            , "DictZipBlobStore xxhash mismatch: wire = %llX , real = %llX"
            , llong(mmapBase->dictXXHash), llong(dict.xxhash)
        );
    }
    setDataMemory((byte*)dataMem.data(), dataMem.size());
}

void DictZipBlobStore::setDataMemory(const void* base, size_t size) {
	auto mmapBase = (const FileHeader*)base;
    m_mmapBase = mmapBase;

#ifndef NDEBUG
	size_t size2 = mmapBase->computeFileSize();
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::External) {
        assert(size2 == size);
    }
	assert(mmapBase->ptrListBytes % 16 == 0);
#endif
	m_unzipSize = mmapBase->unzipSize;
	m_numRecords = mmapBase->records;
	m_ptrList.risk_set_data((byte*)(mmapBase + 1) , mmapBase->ptrListBytes);

    // auto grow expected capacity is 1.5x of real size
    if (m_ptrList.size() != 0) {
        m_reserveOutputMultiplier = ceiled_div(m_unzipSize, m_ptrList.size()) * 3 / 2;
    }
	if (mmapBase->zipOffsets_log2_blockUnits) {
		new(&m_zOffsets)SortedUintVec();
		m_zOffsets.risk_set_data((byte*)(mmapBase + 1) + mmapBase->ptrListBytes
							, mmapBase->offsetArrayBytes);
		assert(m_zOffsets.mem_size() == mmapBase->offsetArrayBytes);
		assert(m_zOffsets.size() == mmapBase->records + 1);
	}
	else {
		new(&m_offsets)UintVecMin0();
		m_offsets.risk_set_data((byte*)(mmapBase + 1) + mmapBase->ptrListBytes
						   , mmapBase->records + 1
						   , mmapBase->offsetsUintBits);
	// allowing read old data format??
	// old format will fail
		assert(m_offsets.mem_size() == mmapBase->offsetArrayBytes);
	}
    if (terark_unlikely(!mmapBase->isNewRefEncoding)) {
        THROW_STD(logic_error, "isNewRefEncoding must be true, but is false");
    }
	m_checksumLevel = mmapBase->crc32cLevel;
	assert(m_offsets.mem_size() % 16 == 0);
	assert(sizeof(FileHeader)
		 + m_offsets.mem_size() + mmapBase->ptrListBytes <= size);

	if (m_checksumLevel >= 1 && isChecksumVerifyEnabled()) {
		uint32_t hCRC = Crc32c_update(0, base, sizeof(FileHeader)-4);
		if (hCRC != mmapBase->headerCRC) {
			throw BadCrc32cException("DictZipBlobStore::headerCRC",
				mmapBase->headerCRC, hCRC);
		}
		uint32_t offsetsCRC =
			Crc32c_update(0, m_offsets.data(), m_offsets.mem_size());
		if (offsetsCRC != mmapBase->offsetsCRC) {
			throw BadCrc32cException("DictZipBlobStore::offsetsCRC",
				mmapBase->offsetsCRC, offsetsCRC);
		}
	}

	m_entropyAlgo = Options::EntropyAlgo(mmapBase->entropyAlgo);
    m_entropyInterleaved = 1;
	bool hasEntropyZip = Options::kNoEntropy != m_entropyAlgo;
	if (hasEntropyZip) {
		auto mem = m_offsets.data() + m_offsets.mem_size();
		auto len = mmapBase->entropyTableSize;
        size_t entropyBitmapSize = febitvec::s_mem_size(align_up(m_numRecords, 16 * 8));
		if (m_checksumLevel >= 1 && isChecksumVerifyEnabled()) {
            uint32_t eCRC = Crc32c_update(0, mem, entropyBitmapSize + len);
			if (mmapBase->entropyTableCRC != eCRC) {
				throw BadCrc32cException("DictZipBlobStore::entropyTableCRC",
					mmapBase->entropyTableCRC, eCRC);
			}
		}
        if (m_mmapBase->fileSize <
            mem - (const byte_t*)m_mmapBase + entropyBitmapSize + align_up(len, 16) + sizeof(BlobStoreFileFooter)) {
            THROW_STD(logic_error, "entropyTableSize error");
        }
        m_entropyBitmap.risk_mmap_from((unsigned char*)mem, entropyBitmapSize);
        mem += entropyBitmapSize;
        if (m_entropyAlgo == Options::kFSE) {
            AutoFree<short> norm(256, 0);
            unsigned maxSym = 255;
            unsigned tableLog = 12; // default
            size_t err = FSE_readNCount(norm, &maxSym, &tableLog, mem, len + 1);
            if (FSE_isError(err)) {
                THROW_STD(logic_error
                    , "FSE_readNCount() = %s", FSE_getErrorName(err));
            }
            if (err > len) {
                THROW_STD(logic_error, "FSE_readNCount() = Unknow error");
            }
            FSE_DTable* dtab = FSE_createDTable(tableLog);
            err = FSE_buildDTable(dtab, norm, maxSym, tableLog);
            if (FSE_isError(err)) {
                FSE_freeDTable(dtab);
                THROW_STD(logic_error
                    , "FSE_buildDTable() = %s", FSE_getErrorName(err));
            }
            m_globalEntropyTableObject = dtab;
        }
        else if (m_entropyAlgo == Options::kHuffmanO1) {
            m_entropyInterleaved = mem[--len];
            if (m_entropyInterleaved <= 8 &&
                    fast_popcount32(m_entropyInterleaved) == 1) {
                // ok: 1,2,4,8
            } else {
                THROW_STD(logic_error, "bad m_entropyInterleaved = %d"
                    , m_entropyInterleaved);
            }
            m_huffman_decoder = new Huffman::decoder_o1(fstring(mem, len));
        }
	}

	if (m_checksumLevel >= 3 && isChecksumVerifyEnabled()) {
		auto foot = mmapBase->getFileFooter();
		uint64_t computed = XXHash64(g_dzbsnark_seed)(m_ptrList);
		uint64_t saved = foot->zipDataXXHash;
		if (saved != computed) {
			throw BadChecksumException("DictZipBlobStore::zipDataXXHash",
				saved, computed);
		}
	}

    m_gOffsetBits = My_bsr_size_t(m_strDict.size() - gMinLen) + 1;
    set_func_ptr();

#if !defined(NDEBUG)
	if (offsetsIsSortedUintVec()) {
		for(size_t i = 0; i < m_numRecords; ++i) {
			size_t BegEnd[2]; m_zOffsets.get2(i, BegEnd);
            size_t offsetBeg = BegEnd[0];
            size_t offsetEnd = BegEnd[1];
			assert(offsetBeg <= offsetEnd);
		}
		size_t maxOffsets = m_zOffsets[m_numRecords];
		assert(align_up(maxOffsets, 16) == m_ptrList.size());
	}
	else {
		for(size_t i = 0; i < m_numRecords; ++i) {
            size_t offsetBeg = m_offsets[i + 0];
            size_t offsetEnd = m_offsets[i + 1];
			assert(offsetBeg <= offsetEnd);
		}
        size_t maxOffsets = m_offsets[m_numRecords];
		assert(align_up(maxOffsets, 16) == m_ptrList.size());
	}
#endif
}

void DictZipBlobStore::load_mmap(fstring fpath) {
    load_mmap_with_dict_memory(fpath, {});
}

void DictZipBlobStore::load_mmap_with_dict_memory(fstring fpath, Dictionary dict) {
    set_fpath(fpath);
    MmapWholeFile fmmap(fpath);
    init_from_memory({(const char*)fmmap.base, (ptrdiff_t)fmmap.size}, dict);
    fmmap.base = nullptr;
    m_isMmapData = true;
}

void DictZipBlobStore::save_mmap(fstring fpath) const {
    if (fpath == m_fpath) {
        return;
    }
    auto mmapBase = (const FileHeader*)m_mmapBase;
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::External) {
        std::string newDictFname = fpath + "-dict";
        std::string oldDictFname = m_fpath + "-dict";
        FileStream dictFp(newDictFname, "wb");
        dictFp.disbuf();
        dictFp.cat(oldDictFname);
    }
    FileStream fp(fpath, "wb");
    assert(nullptr != m_mmapBase);
    fp.ensureWrite(m_mmapBase, m_mmapBase->fileSize);
}

void DictZipBlobStore::save_mmap(function<void(const void*, size_t)> write)
const {
    write(m_mmapBase, m_mmapBase->fileSize);
}

AbstractBlobStore::Dictionary DictZipBlobStore::get_dict() const {
    auto mmapBase = (const FileHeader*)m_mmapBase;
    return Dictionary(m_strDict, mmapBase->dictXXHash);
}

void DictZipBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_strDict.data(), m_strDict.size());
    blocks->emplace_back(m_offsets.data(), m_offsets.mem_size());
}

void DictZipBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_ptrList);
}

void DictZipBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    assert(blocks.size() == 2);
    auto dict_mem = blocks.front();
    auto offset_mem = blocks.back();
    assert(dict_mem.size() == m_strDict.size());
    assert(offset_mem.size() == m_offsets.mem_size());
    switch (m_dictCloseType) {
    case MemoryCloseType::Clear:
        m_strDict.clear();
        break;
    case MemoryCloseType::MmapClose:
        mmap_close(m_strDict.data(), m_strDict.size());
        // fall through
    case MemoryCloseType::RiskRelease:
        m_strDict.risk_release_ownership();
        break;
    }
    m_strDict.risk_set_data((byte_t*)dict_mem.data(), dict_mem.size());
    auto mmapBase = ((const FileHeader*)m_mmapBase);
    if (mmapBase->zipOffsets_log2_blockUnits) {
        if (m_mmapBase) {
            m_zOffsets.risk_release_ownership();
        } else {
            m_zOffsets.clear();
        }
        m_zOffsets.risk_set_data((byte*)offset_mem.data(), offset_mem.size());
    } else {
        if (m_mmapBase) {
            m_offsets.risk_release_ownership();
        } else {
            m_offsets.clear();
        }
        m_offsets.risk_set_data((byte*)offset_mem.data(), mmapBase->records + 1,
            mmapBase->offsetsUintBits);
    }
    m_isDetachMeta = true;
}

size_t DictZipBlobStore::mem_size() const {
    if (m_mmapBase) {
        return m_mmapBase->fileSize + m_strDict.size();
    }
    return m_strDict.size() + m_offsets.mem_size() + m_ptrList.size();
}

static inline void CopyForward(const byte* src, byte* op, size_t len) {
    assert(len > 0);
    do {
        *op++ = *src++;
    } while (--len > 0);
}

#if defined(TERARK_DICT_ZIP_USE_SYS_MEMCPY)
  #define small_memcpy memcpy
#elif 1
  #define small_memcpy small_memcpy_align_1
#else
  #define small_memcpy(dst, src, len) CopyForward
#endif

template<bool ZipOffset, int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave>
terark_flatten void
DictZipBlobStore::get_record_append_tpl(size_t recId, valvec<byte_t>* recData)
const {
    auto readRaw = [this](size_t offset, size_t length) {
        return (const byte_t*)this->m_mmapBase + offset;
    };
    read_record_append_tpl<ZipOffset, CheckSumLevel,
        Entropy, EntropyInterLeave>(recId, recData, readRaw);
}

template<int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave>
terark_flatten void
DictZipBlobStore::get_record_append_CacheOffsets_tpl(size_t recId, CacheOffsets* co)
const {
    auto readRaw = [this](size_t offset, size_t length) {
        return (const byte_t*)this->m_mmapBase + offset;
    };
    read_record_append_CacheOffsets_tpl<CheckSumLevel,
        Entropy, EntropyInterLeave>(recId, co, readRaw);
}

struct LruCachePosRead {
    LruReadonlyCache::Buffer b;
    LruReadonlyCache* cache;
    intptr_t fi;
    size_t baseOffset;

    LruCachePosRead(valvec<byte_t>* rb) : b(rb) {}
    const byte_t* operator()(size_t offset, size_t zipLen) {
        return cache->pread(fi, baseOffset + offset, zipLen, &b);
    }
};

template<bool ZipOffset, int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave>
terark_no_inline terark_flatten void
DictZipBlobStore::pread_record_append_tpl(LruReadonlyCache* cache,
                                          intptr_t fd,
                                          size_t baseOffset,
                                          size_t recId,
                                          valvec<byte_t>* recData,
                                          valvec<byte_t>* rdbuf)
const {
    if (terark_unlikely(fd < 0)) {
        THROW_STD(invalid_argument, "bad fd = %zd", fd);
    }
    if (cache) {
        LruCachePosRead readRaw(rdbuf);
        readRaw.cache      = cache;
        readRaw.fi         = fd; // fd is really fi for cache
        readRaw.baseOffset = baseOffset;
        read_record_append_tpl<ZipOffset, CheckSumLevel,
            Entropy, EntropyInterLeave,
            LruCachePosRead&>(recId, recData, readRaw);
    }
    else {
        fspread_record_append_tpl<ZipOffset, CheckSumLevel,
            Entropy, EntropyInterLeave>(&os_fspread, (void*)fd,
                baseOffset, recId, recData, rdbuf);
    }
}

template<bool ZipOffset, int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave>
terark_no_inline terark_flatten void
DictZipBlobStore::fspread_record_append_tpl(pread_func_t fspread,
                                            void* lambda,
                                            size_t baseOffset,
                                            size_t recID,
                                            valvec<byte_t>* recData,
                                            valvec<byte_t>* rdbuf)
const {
    auto readRaw = [=](size_t offset, size_t zipLen) {
        fspread(lambda, baseOffset + offset, zipLen, rdbuf);
        return rdbuf->data();
    };
    read_record_append_tpl<ZipOffset, CheckSumLevel,
        Entropy, EntropyInterLeave>(recID, recData, readRaw);
}

static byte_t*
TERARK_IF_MSVC(,__attribute__((noinline)))
UpdateOutputPtrAfterGrowCapacity(valvec<byte_t>* buf, size_t len, byte_t*& output) {
	size_t oldsize = output - buf->data();
	buf->ensure_capacity(oldsize + len);
	output = buf->data() + oldsize;
	return   buf->data() + buf->capacity(); // outEnd
}

TERARK_IF_DEBUG(static thread_local size_t tg_dicLen = 0,);

#define DoUnzipFuncName DoUnzipSwitchAutoGrow
#define UnzipUseThreading  0
#define UnzipReserveBuffer 0
#define UnzipDelayGlobalMatch 0
#include "dict_zip_blob_store_unzip_func.hpp"

#define DoUnzipFuncName DoUnzipSwitchPreserve
#define UnzipUseThreading  0
#define UnzipReserveBuffer 1
#define UnzipDelayGlobalMatch 0
#include "dict_zip_blob_store_unzip_func.hpp"

#if defined(__GNUC__)
#define DoUnzipFuncName DoUnzipThreadAutoGrow
#define UnzipUseThreading  1
#define UnzipReserveBuffer 0
#define UnzipDelayGlobalMatch 0
#include "dict_zip_blob_store_unzip_func.hpp"

#define DoUnzipFuncName DoUnzipThreadPreserve
#define UnzipUseThreading  1
#define UnzipReserveBuffer 1
#define UnzipDelayGlobalMatch 0
#include "dict_zip_blob_store_unzip_func.hpp"
#else
#define DoUnzipThreadAutoGrow DoUnzipSwitchAutoGrow
#define DoUnzipThreadPreserve DoUnzipSwitchPreserve
#endif

#define DoUnzipFuncName DoUnzipDelayGAutoGrow
#define UnzipUseThreading  0
#define UnzipReserveBuffer 0
#define UnzipDelayGlobalMatch 1
#include "dict_zip_blob_store_unzip_func.hpp"

#define DoUnzipFuncName DoUnzipDelayGPreserve
#define UnzipUseThreading  0
#define UnzipReserveBuffer 1
#define UnzipDelayGlobalMatch 1
#include "dict_zip_blob_store_unzip_func.hpp"

static int const DefaultUnzipImp = 1; // DoUnzipSwitchPreserve
static int init_get_UnzipImp() {
	int val = (int)getEnvLong("TerarkDictZipUnzipImp", DefaultUnzipImp);
	if (val < 0 || val > 5) {
		val = DefaultUnzipImp;
	}
//	fprintf(stderr, "TerarkDictZipUnzipImp=%d\n", val);
	return val;
}
static const int g_DictZipUnzipImp = init_get_UnzipImp();

#if !defined(NDEBUG)
template<int gOffsetBytes>
static inline void
DoUnzipImp(const byte_t* pos, const byte_t* end, valvec<byte_t>* recData,
           const byte_t* dic, size_t gOffsetBits,
           size_t reserveOutputMultiplier)
{
    const static decltype(&DoUnzipSwitchPreserve<gOffsetBytes>) tab[] = {
        &DoUnzipSwitchAutoGrow<gOffsetBytes>, // 0, // 0b00
        &DoUnzipSwitchPreserve<gOffsetBytes>, // 1, // 0b01
        &DoUnzipThreadAutoGrow<gOffsetBytes>, // 2, // 0b10
        &DoUnzipThreadPreserve<gOffsetBytes>, // 3, // 0b11
        &DoUnzipDelayGAutoGrow<gOffsetBytes>, // 4, // 0100
        &DoUnzipDelayGPreserve<gOffsetBytes>, // 5, // 0101
    };
    assert(int(g_DictZipUnzipImp) >= 0 && int(g_DictZipUnzipImp) <= 5);
    tab[g_DictZipUnzipImp](pos, end, recData, dic,
                           gOffsetBits, reserveOutputMultiplier);
}

static inline void
DoUnzip(const byte_t* pos, const byte_t* end, valvec<byte_t>* recData,
        const byte_t* dic, size_t dicLen,
        size_t reserveOutputMultiplier) {
    TERARK_IF_DEBUG(tg_dicLen = dicLen,);
    auto gOffsetBits = My_bsr_size_t(dicLen - gMinLen) + 1;
    if (gOffsetBits <= 24) {
        DoUnzipImp<3>(pos, end, recData, dic, gOffsetBits, reserveOutputMultiplier);
    }
    else {
        DoUnzipImp<4>(pos, end, recData, dic, gOffsetBits, reserveOutputMultiplier);
    }
}
#endif

template<int UnzipPolicy, int gOffsetBytes>
struct DoUnzipHelper;

#define GenDoUnzipHelper(Index, FuncTpl)              \
template<int gOffsetBytes>                            \
struct DoUnzipHelper<Index, gOffsetBytes> {           \
  static const decltype(&FuncTpl<gOffsetBytes>) unzip;\
};                                                    \
template<int gOffsetBytes>                            \
const decltype(&FuncTpl<gOffsetBytes>)                \
DoUnzipHelper<Index, gOffsetBytes>::unzip = &FuncTpl<gOffsetBytes>
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
GenDoUnzipHelper(0, DoUnzipSwitchAutoGrow);
GenDoUnzipHelper(1, DoUnzipSwitchPreserve);

#if defined(_MSC_VER)
GenDoUnzipHelper(2, DoUnzipSwitchAutoGrow);
GenDoUnzipHelper(3, DoUnzipSwitchPreserve);
#else
GenDoUnzipHelper(2, DoUnzipThreadAutoGrow);
GenDoUnzipHelper(3, DoUnzipThreadPreserve);
#endif
GenDoUnzipHelper(4, DoUnzipDelayGAutoGrow);
GenDoUnzipHelper(5, DoUnzipDelayGPreserve);

template<DictZipBlobStore::EntropyAlgo Entropy, int EntropyInterLeave>
terark_no_inline void
DictZipBlobStore::read_record_append_entropy(const byte_t* zpos, size_t zlen,
                                             size_t recId,
                                             valvec<byte_t>* recData)
const {
    if (m_entropyBitmap[recId]) {
    #if defined(__DARWIN_C_LEVEL) || defined(__GNUC__) && __GNUC__*1000 + __GNUC_MINOR__ < 4008
        EntropyContext tls;
    #else
        auto& tls = *GetTlsEntropyContext();
    #endif
        tls.data.ensure_capacity((zlen + 1024) * 4);
        if (Entropy == Options::kHuffmanO1) {
            bool success = false;
            switch (EntropyInterLeave) {
            case 1: success = m_huffman_decoder->decode_x1(fstring(zpos, zlen), &tls.data, &tls); break;
            case 2: success = m_huffman_decoder->decode_x2(fstring(zpos, zlen), &tls.data, &tls); break;
            case 4: success = m_huffman_decoder->decode_x4(fstring(zpos, zlen), &tls.data, &tls); break;
            case 8: success = m_huffman_decoder->decode_x8(fstring(zpos, zlen), &tls.data, &tls); break;
            default: success = false; break;
            }
            if (!success) {
                THROW_STD(logic_error, "Huffman decode error");
            }
            zlen = tls.data.size();
        }
        else if (Entropy == Options::kFSE) {
            tls.data.risk_set_size(tls.data.capacity());
            zlen = FSE_unzip(zpos, zlen, tls.data, m_globalEntropyTableObject);
            if (FSE_isError(zlen)) {
                THROW_STD(logic_error, "FSE_unzip() = %s", FSE_getErrorName(zlen));
            }
        }
        m_unzip(tls.data.data(), tls.data.data() + zlen, recData,
                m_strDict.data(), m_gOffsetBits, m_reserveOutputMultiplier);
        if (tls.data.capacity() > 512 * 1024) {
            tls.data.clear(); // free large thread local memory
            tls.buffer.clear();
        }
        TERARK_IF_DEBUG(zlen = zlen, ;);
    }
    else {
    	  const byte_t* dic = m_strDict.data();
        const byte_t* end = zpos + zlen;
        m_unzip(zpos, end, recData, dic, m_gOffsetBits, m_reserveOutputMultiplier);
    }
}

template<bool ZipOffset, int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave,
         class ReadRaw>
inline
void DictZipBlobStore::read_record_append_tpl(size_t recId, valvec<byte_t>* recData, ReadRaw readRaw)
const {
	assert(recId + 1 < m_offsets.size());
	size_t BegEnd[2];
	offsetGet2(recId, BegEnd, ZipOffset);
	assert(BegEnd[0] <= BegEnd[1]);
	assert(BegEnd[1] <= m_ptrList.size());
	assert(m_ptrList.data() == (const byte_t*)((FileHeader*)m_mmapBase + 1));
	size_t offset = sizeof(FileHeader) + BegEnd[0];
	size_t zipLen = BegEnd[1] - BegEnd[0];
	const byte* pos = readRaw(offset, zipLen);
	if (CheckSumLevel == 2) {
        if (BegEnd[0] == BegEnd[1]) {
            return; // empty
        }
		if (zipLen <= 4) {
			THROW_STD(logic_error
				, "CRC check failed: recId = %zd, zlen = %zd"
				, recId, zipLen);
		}
        zipLen -= 4; // exclude trailing crc32
		uint32_t crc2 = Crc32c_update(0, pos, zipLen);
		uint32_t crc1 = unaligned_load<uint32_t>(pos + zipLen);
		if (crc2 != crc1) {
			THROW_STD(logic_error, "CRC check failed: recId = %zd", recId);
		}
	}
	TERARK_IF_DEBUG(tg_dicLen = m_strDict.size(),);
    if (Options::kNoEntropy != Entropy) {
        read_record_append_entropy<Entropy, EntropyInterLeave>(pos, zipLen,
            recId, recData);
    }
    else {
    	const byte_t* dic = m_strDict.data();
        const byte_t* end = pos + zipLen;
        m_unzip(pos, end, recData, dic, m_gOffsetBits, m_reserveOutputMultiplier);
    }
}

template<int CheckSumLevel,
         DictZipBlobStore::EntropyAlgo Entropy,
         int EntropyInterLeave,
         class ReadRaw>
inline
void DictZipBlobStore::read_record_append_CacheOffsets_tpl(
                       size_t recId, CacheOffsets* co, ReadRaw readRaw)
const {
	assert(recId + 1 < m_offsets.size());
    size_t log2 = m_zOffsets.log2_block_units(); // must be 6 or 7
    size_t mask = (size_t(1) << log2) - 1;
    if (terark_unlikely(recId >> log2 != co->blockId)) {
        // cache miss, load the block
        size_t blockIdx = recId >> log2;
        m_zOffsets.get_block(blockIdx, co->offsets);
        co->offsets[mask+1] = m_zOffsets.get_block_min_val(blockIdx+1);
        co->blockId = blockIdx;
    }
    size_t inBlockID = recId & mask;
    size_t BegEnd[2] = { co->offsets[inBlockID], co->offsets[inBlockID+1] };
    // code below is based on copy of read_record_append_tpl
	assert(BegEnd[0] <= BegEnd[1]);
	assert(BegEnd[1] <= m_ptrList.size());
	assert(m_ptrList.data() == (const byte_t*)((FileHeader*)m_mmapBase + 1));
	size_t offset = sizeof(FileHeader) + BegEnd[0];
	size_t zipLen = BegEnd[1] - BegEnd[0];
	const byte* pos = readRaw(offset, zipLen);
	if (CheckSumLevel == 2) {
        if (BegEnd[0] == BegEnd[1]) {
            return; // empty
        }
		if (zipLen <= 4) {
			THROW_STD(logic_error
				, "CRC check failed: recId = %zd, zlen = %zd"
				, recId, zipLen);
		}
        zipLen -= 4; // exclude trailing crc32
		uint32_t crc2 = Crc32c_update(0, pos, zipLen);
		uint32_t crc1 = unaligned_load<uint32_t>(pos + zipLen);
		if (crc2 != crc1) {
			THROW_STD(logic_error, "CRC check failed: recId = %zd", recId);
		}
	}
	TERARK_IF_DEBUG(tg_dicLen = m_strDict.size(),);
    if (Options::kNoEntropy != Entropy) {
        read_record_append_entropy<Entropy, EntropyInterLeave>(pos, zipLen,
            recId, &co->recData);
    }
    else {
    	const byte_t* dic = m_strDict.data();
        const byte_t* end = pos + zipLen;
        m_unzip(pos, end, &co->recData, dic, m_gOffsetBits, m_reserveOutputMultiplier);
    }
}

void DictZipBlobStore::set_func_ptr() {
  if (terark_unlikely(nullptr == m_mmapBase)) {
    THROW_STD(invalid_argument, "m_mmapBase must not null");
  }
  m_get_record_append = NULL;
  switch (m_entropyAlgo) {
  default: THROW_STD(logic_error, "bad EntropyAlgo = %d", m_entropyAlgo);
  case kNoEntropy: break;
  case kHuffmanO1: break;
  case kFSE:       break;
  }

// CacheOffsets::recData is first field, so:
// get_record_append can be used as
// get_record_append_CacheOffsets
#define CacheOffsetFunc(ZipOffset, b, c, d)                       \
ZipOffset                                                         \
?        static_cast<get_record_append_CacheOffsets_func_t>(      \
  &DictZipBlobStore::get_record_append_CacheOffsets_tpl<b, c, d>) \
:   reinterpret_cast<get_record_append_CacheOffsets_func_t>(      \
  &DictZipBlobStore::get_record_append_tpl  <ZipOffset, b, c, d>)

#define SetFunc(a,b,c,d) \
  m_get_record_append = static_cast<get_record_append_func_t> \
  (&DictZipBlobStore::get_record_append_tpl<a,b,c,d>); \
  m_get_record_append_CacheOffsets = CacheOffsetFunc(a,b,c,d); \
  m_pread_record_append = static_cast<pread_record_append_func_t> \
  (&DictZipBlobStore::pread_record_append_tpl<a,b,c,d>); \
  m_fspread_record_append = static_cast<fspread_record_append_func_t> \
  (&DictZipBlobStore::fspread_record_append_tpl<a,b,c,d>); \
  break

#define TemplateArgsAre(a, b) \
    a == ZipOffset && b == ChecksumLevel

  assert(int(g_DictZipUnzipImp) >= 0 && int(g_DictZipUnzipImp) <= 5);
  const bool ZipOffset = offsetsIsSortedUintVec();
  const int  ChecksumLevel = 2 == m_checksumLevel ? 2 : 0; // non-2 as 0
  const int  UnzipPolicy = std::min(g_DictZipUnzipImp&7,5);//tolerate bad value
  const int  gOffsetBytes = m_gOffsetBits <= 24 ? 3 : 4;
  const int  EI = m_entropyInterleaved;

// UnzipID is a perfect hash function to compute a unique id for switch-case
#define      UnzipID(UnzipPolicy, gOffsetBytes) UnzipPolicy*2 + gOffsetBytes-3
#define case_UnzipID(UnzipPolicy, gOffsetBytes)  \
        case UnzipID(UnzipPolicy, gOffsetBytes): \
      m_unzip = DoUnzipHelper<UnzipPolicy, gOffsetBytes>::unzip; break
  switch (UnzipID(UnzipPolicy, gOffsetBytes)) {
     case_UnzipID(0, 3);
     case_UnzipID(0, 4);
     case_UnzipID(1, 3);
     case_UnzipID(1, 4);
     case_UnzipID(2, 3);
     case_UnzipID(2, 4);
     case_UnzipID(3, 3);
     case_UnzipID(3, 4);
     case_UnzipID(4, 3);
     case_UnzipID(4, 4);
     case_UnzipID(5, 3);
     case_UnzipID(5, 4);
     default: assert(false); abort(); break;
  }

  if (false) {
  } else if (TemplateArgsAre(0, 0)) { switch (m_entropyAlgo) {
    case kFSE      : SetFunc(0, 0, kFSE      , 0);
    case kNoEntropy: SetFunc(0, 0, kNoEntropy, 0); case kHuffmanO1: switch (EI) {
             case 1: SetFunc(0, 0, kHuffmanO1, 1);
             case 2: SetFunc(0, 0, kHuffmanO1, 2);
             case 4: SetFunc(0, 0, kHuffmanO1, 4);
             case 8: SetFunc(0, 0, kHuffmanO1, 8); } break;
    }
  } else if (TemplateArgsAre(0, 2)) { switch (m_entropyAlgo) {
    case kFSE      : SetFunc(0, 2, kFSE      , 0);
    case kNoEntropy: SetFunc(0, 2, kNoEntropy, 0); case kHuffmanO1: switch (EI) {
             case 1: SetFunc(0, 2, kHuffmanO1, 1);
             case 2: SetFunc(0, 2, kHuffmanO1, 2);
             case 4: SetFunc(0, 2, kHuffmanO1, 4);
             case 8: SetFunc(0, 2, kHuffmanO1, 8); } break;
    }

  } else if (TemplateArgsAre(1, 0)) { switch (m_entropyAlgo) {
    case kFSE      : SetFunc(1, 0, kFSE      , 0);
    case kNoEntropy: SetFunc(1, 0, kNoEntropy, 0); case kHuffmanO1: switch (EI) {
             case 1: SetFunc(1, 0, kHuffmanO1, 1);
             case 2: SetFunc(1, 0, kHuffmanO1, 2);
             case 4: SetFunc(1, 0, kHuffmanO1, 4);
             case 8: SetFunc(1, 0, kHuffmanO1, 8); } break;
    }
  } else if (TemplateArgsAre(1, 2)) { switch (m_entropyAlgo) {
    case kFSE      : SetFunc(1, 2, kFSE      , 0);
    case kNoEntropy: SetFunc(1, 2, kNoEntropy, 0); case kHuffmanO1: switch (EI) {
             case 1: SetFunc(1, 2, kHuffmanO1, 1);
             case 2: SetFunc(1, 2, kHuffmanO1, 2);
             case 4: SetFunc(1, 2, kHuffmanO1, 4);
             case 8: SetFunc(1, 2, kHuffmanO1, 8); } break;
    }
  }
  assert(NULL != m_get_record_append);
}

///@param newToOld length must be this->num_records()
void
DictZipBlobStore::reorder_and_load(ZReorderMap& newToOld,
								   fstring newFile,
								   bool keepOldFile) {
    auto mmapBase = (const FileHeader*)m_mmapBase;
	FileStream fp(newFile.c_str(), "wb");
	fp.chsize(mmapBase->computeFileSize());
	reorder_zip_data(newToOld, [&fp](const void* data, size_t size) {
		fp.ensureWrite(data, size);
	}, newFile + ".reorder-tmp");
	assert(fp.tell() == mmapBase->computeFileSize());
	fp.close();
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::External) {
        std::string newDictFname = newFile + "-dict";
        std::string oldDictFname = m_fpath + "-dict";
        if (keepOldFile) {
            FileStream dictFp(newDictFname, "wb");
            dictFp.disbuf();
            dictFp.cat(oldDictFname);
            destroyMe();
        }
        else {
            destroyMe();
            if (::rename(oldDictFname.c_str(), newDictFname.c_str()) < 0) {
                fprintf(stderr, "ERROR: DictZipBlobStore::reorder_and_load()");
            }
            ::remove(m_fpath.c_str());
        }
    }
    this->load_mmap(newFile);
}

void DictZipBlobStore::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {

	assert(nullptr != m_mmapBase);
	TERARK_IF_DEBUG(febitvec rbits(m_numRecords),;);
	const bool isOffsetsZipped = offsetsIsSortedUintVec();
    const bool hasEntropy = m_entropyAlgo != Options::kNoEntropy;
	size_t recNum = m_numRecords;
	size_t offset = 0;
	size_t maxOffsetEnt = isOffsetsZipped ? m_zOffsets[recNum] : m_offsets[recNum];
    MmapWholeFile mmapOffset;
    UintVecMin0 newOffsets;
    SortedUintVec newZipOffsets;
    febitvec newEntropyBitmap;
    if (hasEntropy) {
        newEntropyBitmap.resize(recNum);
    }
    if (isOffsetsZipped) {
        auto zipOffsetBuilder = std::unique_ptr<SortedUintVec::Builder>(
            SortedUintVec::createBuilder(m_zOffsets.block_units(), tmpFile.c_str()));
        for(assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
            size_t newId = newToOld.index();
            size_t oldId = *newToOld;
            assert(oldId < recNum);
            size_t BegEnd[2];
            offsetGet2(oldId, BegEnd, isOffsetsZipped);
            zipOffsetBuilder->push_back(offset);
            assert(BegEnd[0] <= BegEnd[1]);
            offset += BegEnd[1] - BegEnd[0];
            if (hasEntropy) {
                newEntropyBitmap.set(newId, m_entropyBitmap[oldId]);
            }
        }
        assert(offset == maxOffsetEnt);
        zipOffsetBuilder->push_back(maxOffsetEnt);
        zipOffsetBuilder->finish(nullptr);
        zipOffsetBuilder.reset();
        MmapWholeFile(tmpFile).swap(mmapOffset);
        newZipOffsets.risk_set_data(mmapOffset.base, mmapOffset.size);
    }
    else {
        FileStream m_file_offset(tmpFile.c_str(), "wb+");
        NativeDataOutput<OutputBuffer> m_writer_offset(&m_file_offset);
        m_file_offset.disbuf();
        static const size_t offset_flush_size = 128;
        UintVecMin0 tmpOffsets(offset_flush_size, maxOffsetEnt);
        size_t flush_count = 0;
        assert(align_up(maxOffsetEnt, 16) == m_ptrList.size());
        assert(tmpOffsets.uintbits() == m_offsets.uintbits());
        for (assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
            size_t newId = newToOld.index() - flush_count;
            size_t oldId = *newToOld;
            assert(oldId < recNum);
            if (newId == offset_flush_size) {
                size_t byte_count = tmpOffsets.uintbits() * offset_flush_size / 8;
                m_writer_offset.ensureWrite(tmpOffsets.data(), byte_count);
                flush_count += offset_flush_size;
                newId = 0;
            }
            size_t BegEnd[2];
            offsetGet2(oldId, BegEnd, isOffsetsZipped);
            tmpOffsets.set_wire(newId, offset);
            size_t zippedLen = BegEnd[1] - BegEnd[0];
            assert(BegEnd[0] <= BegEnd[1]);
            offset += zippedLen;
            if (hasEntropy) {
                newEntropyBitmap.set(newToOld.index(), m_entropyBitmap[oldId]);
            }
        }
        assert(offset == maxOffsetEnt);
        tmpOffsets.resize(recNum - flush_count + 1);
        tmpOffsets.set_wire(recNum - flush_count, maxOffsetEnt);
        tmpOffsets.shrink_to_fit();
        m_writer_offset.ensureWrite(tmpOffsets.data(), tmpOffsets.mem_size());
        m_writer_offset.flush_buffer();
        m_file_offset.close();
        MmapWholeFile(tmpFile).swap(mmapOffset);
        newOffsets.risk_set_data((byte_t*)mmapOffset.base, recNum + 1, tmpOffsets.uintbits());
    }
    auto mmapBase = (const FileHeader*)m_mmapBase;
    if (hasEntropy) {
        newEntropyBitmap.resize(align_up(recNum, 16 * 8));
        auto entropyMem = m_offsets.data() + m_offsets.mem_size() + newEntropyBitmap.mem_size();
        auto entropyLen = ((const FileHeader*)m_mmapBase)->entropyTableSize;
        FileHeader h(this, offset,
            Dictionary(m_strDict.size(), ((const FileHeader*)m_mmapBase)->dictXXHash),
            // UintVecMin0 & SortedUintVec same layour ...
            isOffsetsZipped ? (UintVecMin0&)newZipOffsets : newOffsets,
            fstring((char*)newEntropyBitmap.data(), newEntropyBitmap.mem_size()),
            fstring(entropyMem, entropyLen), maxOffsetEnt);
        if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
            h.setEmbeddedDictType(mmapBase->getEmbeddedDict().size(),
                                  (EmbeddedDictType)mmapBase->embeddedDict);
        }
        writeAppend(&h, sizeof(h));
    }
    else {
        fstring empty("");
        FileHeader h(this, offset,
            Dictionary(m_strDict.size(), ((const FileHeader*)m_mmapBase)->dictXXHash),
            // UintVecMin0 & SortedUintVec have same layout ...
            isOffsetsZipped ? (UintVecMin0&)newZipOffsets : newOffsets,
            empty, empty, maxOffsetEnt);
        if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
            h.setEmbeddedDictType(mmapBase->getEmbeddedDict().size(),
                                  (EmbeddedDictType)mmapBase->embeddedDict);
        }
        writeAppend(&h, sizeof(h));
    }
    XXHash64 xxhash64(g_dzbsnark_seed);
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        size_t oldId = *newToOld;
		size_t BegEnd[2];
		offsetGet2(oldId, BegEnd, isOffsetsZipped);
		size_t zippedLen = BegEnd[1] - BegEnd[0];
		assert(BegEnd[0] <= BegEnd[1]);
		const byte* beg = m_ptrList.data() + BegEnd[0];
		xxhash64.update(beg, zippedLen);
		writeAppend(beg, zippedLen);
		assert(rbits.is0(oldId));
		TERARK_IF_DEBUG(rbits.set1(oldId), ;);
	}
    static const byte zeros[16] = { 0 };
	if (offset % 16 != 0) {
		xxhash64.update(zeros, 16 - offset % 16);
		writeAppend(zeros, 16 - offset % 16);
	}
    if (isOffsetsZipped) {
        writeAppend(newZipOffsets.data(), newZipOffsets.mem_size());
        assert(newZipOffsets.mem_size() % 16 == 0);
        newZipOffsets.risk_release_ownership();
    }
    else {
        writeAppend(newOffsets.data(), newOffsets.mem_size());
        assert(newOffsets.mem_size() % 16 == 0);
        newOffsets.risk_release_ownership();
    }
    MmapWholeFile().swap(mmapOffset);
    ::remove(tmpFile.c_str());

	if (hasEntropy) {
        auto entropyMem = m_offsets.data() + m_offsets.mem_size() + newEntropyBitmap.mem_size();
        auto entropyLen = ((const FileHeader*)m_mmapBase)->entropyTableSize;
		assert(entropyLen > 0);
        writeAppend(newEntropyBitmap.data(), newEntropyBitmap.mem_size());
        writeAppend(entropyMem, align_up(entropyLen, 16));
	}
    if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
        fstring embeddedDict = mmapBase->getEmbeddedDict();
        writeAppend(embeddedDict.data(), embeddedDict.size());
        if (embeddedDict.size() % 16 != 0)
            writeAppend(zeros, 16 - embeddedDict.size() % 16);
    }
	BlobStoreFileFooter foot;
	foot.zipDataXXHash = xxhash64.digest();
	writeAppend(&foot, sizeof(foot));
}

void DictZipBlobStore::purge_and_load(const bm_uint_t* isDel,
									  size_t baseId_of_isDel,
									  fstring newFile,
									  bool keepOldFile) {
    auto mmapBase = (const FileHeader*)m_mmapBase;
	FileStream fp(newFile.c_str(), "wb");
	fp.chsize(mmapBase->computeFileSize());
	purge_zip_data(isDel, baseId_of_isDel, [&fp](const void* data, size_t size) {
		fp.ensureWrite(data, size);
	});
	fp.chsize(fp.tell());
	fp.close();
    if (mmapBase->embeddedDict == (uint8_t)EmbeddedDictType::External) {
        std::string newDictFname = newFile + "-dict";
        std::string oldDictFname = m_fpath + "-dict";
        if (keepOldFile) {
            FileStream dictFp(newDictFname, "wb");
            dictFp.disbuf();
            dictFp.cat(oldDictFname);
            destroyMe();
        }
        else {
            destroyMe();
            if (::rename(oldDictFname.c_str(), newDictFname.c_str()) < 0) {
                fprintf(stderr
                    , "ERROR: DictZipBlobStore::purge_and_load(): rename(%s, %s) = %s\n"
                    , oldDictFname.c_str(), newDictFname.c_str()
                    , strerror(errno));
            }
            ::remove(m_fpath.c_str());
        }
    }
	this->load_mmap(newFile);
}

// purge deleted records, keep the dictionary unchanged
void DictZipBlobStore::purge_zip_data(const bm_uint_t* isDel,
		size_t baseId_of_isDel,
		function<void(const void* data, size_t size)> writeAppend)
const {
	purge_zip_data_impl(
		[=](size_t id){ return terark_bit_test(isDel, baseId_of_isDel + id); },
		writeAppend);
}


void DictZipBlobStore::purge_zip_data(function<bool(size_t id)> isDel,
		function<void(const void* data, size_t size)> writeAppend)
const {
	purge_zip_data_impl(isDel, writeAppend);
}

template<class IsDel>
void DictZipBlobStore::purge_zip_data_impl(IsDel isDel,
		function<void(const void* data, size_t size)> writeAppend)
const {
	assert(nullptr != m_mmapBase);
	size_t recNum = m_numRecords;
	size_t offset = 0;
	const bool isOffsetsZipped = offsetsIsSortedUintVec();
    const bool hasEntropy = Options::kNoEntropy != m_entropyAlgo;
	size_t maxOffsetEnt = isOffsetsZipped ? m_zOffsets[recNum] : m_offsets[recNum];

#if !defined(NDEBUG)
	size_t maxOffset = maxOffsetEnt;
	assert(align_up(maxOffset, 16) == m_ptrList.size());
#endif
	UintVecMin0 newOffsets(recNum + 1, maxOffsetEnt);
    febitvec newEntropyBitmap;
    if (hasEntropy) {
        newEntropyBitmap.reserve(recNum);
    }
	assert(newOffsets.uintbits() <= m_offsets.uintbits());
	assert(newOffsets.mem_size() <= m_offsets.mem_size());
	size_t newId = 0;
	for(size_t oldId = 0; oldId < recNum; oldId++) {
		if (!isDel(oldId)) {
			size_t BegEnd[2];
			offsetGet2(oldId, BegEnd, isOffsetsZipped);
			newOffsets.set_wire(newId, offset);
			size_t zippedLen = BegEnd[1] - BegEnd[0];
			assert(BegEnd[0] <= BegEnd[1]);
			offset += zippedLen;
            if (hasEntropy) {
                newEntropyBitmap.push_back(m_entropyBitmap[oldId]);
            }
			newId++;
		}
	}
	assert(offset <= maxOffset);
	size_t newNum = newId;
	newOffsets.resize(newNum+1);
	newOffsets.set_wire(newNum, offset);
    auto mmapBase = (const FileHeader*)m_mmapBase;

    if (hasEntropy) {
        newEntropyBitmap.resize(align_up(newNum, 16 * 8));
        auto entropyMem = m_offsets.data() + m_offsets.mem_size()
                        + febitvec::s_mem_size(align_up(recNum, 16 * 8));
        auto entropyLen = ((const FileHeader*)m_mmapBase)->entropyTableSize;
        fstring entropy(entropyMem, entropyLen);
        Dictionary dict(m_strDict.size(), ((const FileHeader*)m_mmapBase)->dictXXHash);
        FileHeader h(this, offset, dict, newOffsets,
                     fstring((char*)newEntropyBitmap.data(), newEntropyBitmap.mem_size()),
                     entropy, maxOffsetEnt);
        if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
            h.setEmbeddedDictType(mmapBase->getEmbeddedDict().size(),
                                  (EmbeddedDictType)mmapBase->embeddedDict);
        }
        writeAppend(&h, sizeof(h));
    }
    else {
        fstring empty("");
        Dictionary dict(m_strDict.size(), ((const FileHeader*)m_mmapBase)->dictXXHash);
        FileHeader h(this, offset, dict, newOffsets, empty, empty, maxOffsetEnt);
        if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
            h.setEmbeddedDictType(mmapBase->getEmbeddedDict().size(),
                                  (EmbeddedDictType)mmapBase->embeddedDict);
        }
        writeAppend(&h, sizeof(h));
    }
	XXHash64 xxhash64(g_dzbsnark_seed);
	newId = 0;
	offset = 0;
	for(size_t oldId = 0; oldId < recNum; oldId++) {
		if (!isDel(oldId)) {
			size_t BegEnd[2];
			offsetGet2(oldId, BegEnd, isOffsetsZipped);
			newOffsets.set_wire(newId, offset);
			size_t zippedLen = BegEnd[1] - BegEnd[0];
			assert(BegEnd[0] <= BegEnd[1]);
			const  byte* beg = m_ptrList.data() + BegEnd[0];
			xxhash64.update(beg, zippedLen);
			writeAppend(beg, zippedLen);
			offset += zippedLen;
			newId++;
		}
	}
	assert(newId == newNum);
	if (newId != newNum) {
		THROW_STD(logic_error, "isDel was changed during purge");
	}
	static const byte zeros[16] = {0};
	if (offset % 16 != 0) {
		xxhash64.update(zeros, 16 - offset % 16);
		writeAppend(zeros, 16 - offset % 16);
	}
	writeAppend(newOffsets.data(), newOffsets.mem_size());
    if (hasEntropy) {
        auto entropyMem = m_offsets.data() + m_offsets.mem_size()
                        + febitvec::s_mem_size(align_up(recNum, 16 * 8));
        auto entropyLen = mmapBase->entropyTableSize;
        writeAppend(newEntropyBitmap.data(), newEntropyBitmap.mem_size());
		writeAppend(entropyMem, align_up(entropyLen, 16));
	}
    if (mmapBase->embeddedDict != (uint8_t)EmbeddedDictType::External) {
        fstring embeddedDict = mmapBase->getEmbeddedDict();
        writeAppend(embeddedDict.data(), embeddedDict.size());
        writeAppend(zeros, 16 - embeddedDict.size() % 16);
    }
	BlobStoreFileFooter foot;
	foot.zipDataXXHash = xxhash64.digest();
	writeAppend(&foot, sizeof(foot));
}

} // namespace terark
