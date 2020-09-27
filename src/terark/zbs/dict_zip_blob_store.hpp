#pragma once
#include <terark/int_vector.hpp>
#include <terark/io/FileMemStream.hpp>
#include <terark/zbs/abstract_blob_store.hpp>
#include <terark/util/function.hpp>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/entropy/huffman_encoding.hpp>

namespace terark {

/*************************UPDATE LOG*********************************
 ** formatVersion 0 -> 1 :
 **     FileHeader add dictXXHash for verify dict
 **/

class TERARK_DLL_EXPORT DictZipBlobStore : public AbstractBlobStore {
public:
	struct FileHeader;  friend struct FileHeader;
	struct TERARK_DLL_EXPORT Options {
		enum EntropyAlgo : byte_t {
			kNoEntropy,
			kHuffmanO1,
			kFSE,
		};
		enum SampleSortPolicy {
			kSortNone,
			kSortLeft,
			kSortRight,
			kSortBoth,
		};
		int checksumLevel; // default 1
		int maxMatchProbe; // default 5 for local hash
						   //        30 for local suffix array
		EntropyAlgo entropyAlgo; // default kNoEntropy
		SampleSortPolicy sampleSort;
		bool useSuffixArrayLocalMatch;
		bool useNewRefEncoding; // now unused
		bool compressGlobalDict;
        uint8_t entropyInterleaved;
		int  offsetArrayBlockUnits; // 0 for no compress
        float entropyZipRatioRequire;
        bool embeddedDict;
        bool enableLake;
        bool inputIsPerm;
        int  recordsPerBatch;
        int  bytesPerBatch;

		Options();
	};
    typedef Options::EntropyAlgo EntropyAlgo;
    static const EntropyAlgo kNoEntropy = Options::kNoEntropy;
    static const EntropyAlgo kHuffmanO1 = Options::kHuffmanO1;
    static const EntropyAlgo kFSE       = Options::kFSE;

	struct TERARK_DLL_EXPORT ZipStat {
		// all time are in seconds
		double sampleTime;
		double dictBuildTime;
		double dictZipTime;
		double dictFileTime;
		double entropyBuildTime;
		double entropyZipTime;
        double embedDictTime;
		ullong pipelineThroughBytes; // including other ZipBuilder's
		ZipStat();
		void print(FILE*) const;
	};

private:
    typedef void
    (*UnzipFuncPtr)(const byte_t* pos, const byte_t* end,
                    valvec<byte_t>* recData, const byte_t* dic,
                    size_t gOffsetBits, size_t reserveOutputMultiplier);
    UnzipFuncPtr  m_unzip;
	valvec<byte>  m_strDict;
	valvec<byte>  m_ptrList;
    febitvec      m_entropyBitmap;

	union {
		// layout of UintVecMin0 is compatible to SortedUintVec
		static_assert(sizeof(UintVecMin0)==sizeof(SortedUintVec),
					 "UintVecMin0 and SortedUintVec should be compatible");
		UintVecMin0   m_offsets;
		SortedUintVec m_zOffsets;
	};
    size_t        m_reserveOutputMultiplier;
	void*         m_globalEntropyTableObject;
    const Huffman::decoder_o1* m_huffman_decoder;
	Options::EntropyAlgo m_entropyAlgo;
	bool          m_isNewRefEncoding; // now unused
    byte_t        m_entropyInterleaved;
    byte_t        m_gOffsetBits; // = My_bsr_size_t(dicLen - gMinLen) + 1;
    bool          m_dict_verified;

	bool offsetsIsSortedUintVec() const {
		return m_zOffsets.isSortedUintVec();
	}

	void init();
	void destroyMe();
	void setDataMemory(const void* base, size_t size);

	void offsetGet2(size_t recId, size_t BegEnd[2], bool isZipped) const;

public:
	/// usage:
	/// @code
	/// DictZipBlobStore::Options opt;
	/// opt.checksumLevel = ...; // default 1
	///
	/// std::unique_ptr<DictZipBlobStore::ZipBuilder>
	///         builder(DictZipBlobStore::createZipBuilder(opt));
	/// ...
	/// builder->addSample(...);
	/// ...
	/// builder->prepare(records);
	/// ...
	/// builder->addRecord(...);
	/// ...
	/// AbstractBlobStore* store = builder->finish();
	/// @endcode
	class TERARK_DLL_EXPORT ZipBuilder {
	public:
        enum FinishFlags {
            FinishNone          = 0,
            FinishFreeDict      = 1 << 0,
            FinishWriteDictFile = 1 << 1,
        };
		virtual ~ZipBuilder();
		virtual void addRecord(const byte* rData, size_t rSize) = 0;
		virtual void addSample(const byte* rData, size_t rSize) = 0;
		virtual void useSample(valvec<byte>& sample) = 0;
		virtual void finishSample() = 0;
        virtual Dictionary getDictionary() const = 0;
        virtual void prepareDict() = 0;
        virtual void prepare(size_t records, FileMemIO& mem) = 0;
		virtual void prepare(size_t records, fstring fpath) = 0;
        virtual void prepare(size_t records, fstring fpath, size_t offset) = 0;
		void addRecord(fstring rec) { addRecord(rec.udata(), rec.size()); }
		void addSample(fstring rec) { addSample(rec.udata(), rec.size()); }
        // FinishFreeDict free dict memory
        // FinishFreeDict output dict file
        virtual void finish(int flags) = 0;
        virtual void abandon() = 0;
		virtual const ZipStat& getZipStat() = 0;
        virtual void freeDict() = 0;
		virtual void dictSwapOut(fstring fname) = 0;
		virtual void dictSwapIn(fstring fname) = 0;
        virtual byte_t* addRecord(size_t rSize) = 0;
        virtual bool isMultiThread() const = 0;

        function<void(fstring raw, fstring zip)> m_onFinishEachValue;
	};
	friend class DictZipBlobStoreBuilder;
	static ZipBuilder* createZipBuilder(const Options&);

    class TERARK_DLL_EXPORT MyBuilder : public AbstractBlobStore::Builder {
        ~MyBuilder();
        Builder* getPreBuilder() const override;
        void addRecord(fstring rec) override;
        void finish() override;
    };

	DictZipBlobStore();
	~DictZipBlobStore();

	void swap(DictZipBlobStore&);

    void init_from_memory(fstring dataMem, Dictionary dict) override;

    void load_mmap(fstring fpath);
    void load_mmap_with_dict_memory(fstring fpath, Dictionary dict);
    void save_mmap(fstring fpath) const override;
    void save_mmap(function<void(const void*, size_t)> write) const override;

    Dictionary get_dict() const override;
    const UintVecMin0& get_index() const { return m_offsets; }

    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
    void detach_meta_blocks(const valvec<fstring>& blocks) override;

	size_t mem_size() const override;

private:
    template<bool ZipOffset, int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave>
	void get_record_append_tpl(size_t recId, valvec<byte_t>* recData) const;

    template<int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave>
    void get_record_append_CacheOffsets_tpl(size_t recId, CacheOffsets*) const;

    template<bool ZipOffset, int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave>
	void pread_record_append_tpl(LruReadonlyCache*, intptr_t fd, size_t baseOffset, size_t recID, valvec<byte_t>* recData, valvec<byte_t>* buf) const;
    template<bool ZipOffset, int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave>
	void fspread_record_append_tpl(pread_func_t, void* lambdaObj, size_t baseOffset, size_t recID, valvec<byte_t>* recData, valvec<byte_t>* buf) const;

	template<bool ZipOffset, int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave, class ReadRaw>
	void read_record_append_tpl(size_t recId, valvec<byte_t>* recData, ReadRaw) const;

    template<int CheckSumLevel, EntropyAlgo Entropy, int EntropyInterLeave, class ReadRaw>
    void read_record_append_CacheOffsets_tpl(size_t recId, CacheOffsets*, ReadRaw) const;

	template<EntropyAlgo Entropy, int EntropyInterLeave>
	void read_record_append_entropy(const byte_t* zdata, size_t zlen,size_t recId, valvec<byte_t>* recData) const;

    void set_func_ptr();

public:
	void reorder_and_load(ZReorderMap& newToOld, fstring newFile, bool keepOldFile);
    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;

	void purge_and_load(const bm_uint_t* isDel, size_t baseId_of_isDel, fstring newFile, bool keepOldFile);
	void purge_zip_data(const bm_uint_t* isDel, size_t baseId_of_isDel,
			function<void(const void* data, size_t size)> writeAppend
		 ) const;
	void purge_zip_data(function<bool(size_t id)> isDel,
			function<void(const void* data, size_t size)> writeAppend
		 ) const;
private:
	template<class IsDel>
	void purge_zip_data_impl(IsDel isDel,
			function<void(const void* data, size_t size)> writeAppend
		 ) const;
};

} // namespace terark

