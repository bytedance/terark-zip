#pragma once
#include <string>
#include <terark/int_vector.hpp>
#include <terark/zbs/abstract_blob_store.hpp>
#include <terark/rank_select.hpp>

namespace terark {

template<class rank_select_t>
class TERARK_DLL_EXPORT MixedLenBlobStoreTpl : public AbstractBlobStore {
	struct FileHeader; friend struct FileHeader;
	size_t      m_fixedLen;
    size_t      m_fixedLenWithoutCRC;
	size_t      m_fixedNum;
    rank_select_t          m_isFixedLen;
	terark::valvec<byte_t> m_fixedLenValues;
	terark::valvec<byte_t> m_varLenValues;
	terark::UintVecMin0    m_varLenOffsets;

	void getFixLenRecordAppend(size_t fixLenRecID, valvec<byte_t>* recData) const;
	void getVarLenRecordAppend(size_t varLenRecID, valvec<byte_t>* recData) const;

    void set_func_ptr();
    void get_record_append_has_fixed_rs(size_t recID, valvec<byte_t>* recData) const;

    void fspread_record_append_has_fixed_rs(
                        pread_func_t fspread, void* lambda,
                        size_t baseOffset, size_t recID,
                        valvec<byte_t>* recData,
                        valvec<byte_t>* rdbuf) const;
    void fspread_FixLenRecordAppend(pread_func_t fspread, void* lambda,
                                    size_t baseOffset, size_t recID,
                                    valvec<byte_t>* recData,
                                    valvec<byte_t>* buf) const;
    void fspread_VarLenRecordAppend(pread_func_t fspread, void* lambda,
                                    size_t baseOffset, size_t recID,
                                    valvec<byte_t>* recData,
                                    valvec<byte_t>* rdbuf) const;

public:
    void init_from_memory(fstring dataMem, Dictionary dict) override;
    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
    void detach_meta_blocks(const valvec<fstring>& blocks) override;
    void save_mmap(function<void(const void*, size_t)> write) const override;
    using AbstractBlobStore::save_mmap;
    size_t mem_size() const override;

    MixedLenBlobStoreTpl();
	~MixedLenBlobStoreTpl();

    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;

    struct TERARK_DLL_EXPORT MyBuilder : public AbstractBlobStore::Builder {
        class TERARK_DLL_EXPORT Impl; Impl* impl;
    public:
        MyBuilder(size_t fixedLen, size_t varLenContentSize, size_t varLenContentCnt, fstring fpath, size_t offset = 0,
                  int checksumLevel = 3, int checksumType = 0);
        virtual ~MyBuilder();
        void addRecord(fstring rec) override;
        void finish() override;
    };
};

typedef MixedLenBlobStoreTpl<terark::rank_select_il> MixedLenBlobStore;
typedef MixedLenBlobStoreTpl<terark::rank_select_se_512_64> MixedLenBlobStore64;

} // namespace terark
