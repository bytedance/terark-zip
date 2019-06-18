#pragma once
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/util/function.hpp>
#include <terark/util/refcount.hpp>

namespace terark {

class LruReadonlyCache;

template<bool ZipOffset>
struct BlobStoreRecBuffer;

class TERARK_DLL_EXPORT BlobStore : public RefCounter {
public:
    static BlobStore* load_from_mmap(fstring fpath, bool mmapPopulate);
    static BlobStore* load_from_user_memory(fstring dataMem);

    virtual const char* name() const = 0;
    virtual void get_meta_blocks(valvec<fstring>* blocks) const = 0;
    virtual void get_data_blocks(valvec<fstring>* blocks) const = 0;
    virtual void detach_meta_blocks(const valvec<fstring>& blocks) = 0;
    valvec<fstring> get_meta_blocks() const;
    valvec<fstring> get_data_blocks() const;

    BlobStore();
    virtual ~BlobStore();
    size_t num_records() const { return m_numRecords; }
    uint64_t total_data_size() const { return m_unzipSize; }
    virtual size_t mem_size() const = 0;

    terark_forceinline
    void get_record_append(size_t recID, valvec<byte_t>* recData) const {
        (this->*m_get_record_append)(recID, recData);
    }
    terark_forceinline
    void get_record(size_t recID, valvec<byte_t>* recData) const {
        recData->erase_all();
        (this->*m_get_record_append)(recID, recData);
    }
    terark_forceinline
    valvec<byte_t> get_record(size_t recID) const {
        valvec<byte_t> recData;
        (this->*m_get_record_append)(recID, &recData);
        return recData;
    }

    /// Used for optimizing Iterator
    struct CacheOffsets {
        valvec<byte_t> recData;
        size_t blockId = size_t(-1);
        size_t offsets[129]; // bind to SortedUintVec implementation
        // BlockSize can only be 64 or 128
        // offsets[BlockSize+1] store next block's first offset

        void invalidate_offsets_cache() { blockId = size_t(-1); }
    };
    terark_forceinline
    void get_record_append(size_t recID, CacheOffsets* co) const {
        (this->*m_get_record_append_CacheOffsets)(recID, co);
    }
    terark_forceinline
    void get_record(size_t recID, CacheOffsets* co) const {
        co->recData.erase_all();
        (this->*m_get_record_append_CacheOffsets)(recID, co);
    }
    virtual size_t lower_bound(size_t lo, size_t hi, fstring target, CacheOffsets* co) const;
    virtual size_t q(size_t lo, size_t hi, fstring target, valvec<byte_t>* recData) const;
    terark_forceinline
    bool is_offsets_zipped() const {
        return reinterpret_cast<get_record_append_func_t>
                             (m_get_record_append_CacheOffsets)
                          !=  m_get_record_append;
    }

    // fd is HANDLE for windows, so use type intptr_t
    // if cache is not NULL, fd is `fi` for cache
    terark_forceinline
    void pread_record_append(LruReadonlyCache* cache, intptr_t fi,
                             size_t baseOffset, size_t recID,
                             valvec<byte_t>* recData,
                             valvec<byte_t>* rdbuf) const {
        (this->*m_pread_record_append)(cache, fi, baseOffset, recID, recData, rdbuf);
    }
    void pread_record_append(LruReadonlyCache*, intptr_t fi,
                             size_t baseOffset, size_t recID,
                             valvec<byte_t>* recData) const;
    terark_forceinline
    void pread_record(LruReadonlyCache* cache, intptr_t fi,
                      size_t baseOffset, size_t recID,
                      valvec<byte_t>* recData,
                      valvec<byte_t>* rdbuf) const {
        recData->risk_set_size(0);
        (this->*m_pread_record_append)(cache, fi, baseOffset, recID, recData, rdbuf);
    }
    terark_forceinline
    void pread_record(LruReadonlyCache* cache, intptr_t fi,
                      size_t baseOffset, size_t recID,
                      valvec<byte_t>* recData) const {
        recData->risk_set_size(0);
        pread_record_append(cache, fi, baseOffset, recID, recData);
    }

    // pread_func_t should return rdbuf->data() by default, can return
    // other pointers if possible, such as BlobStoreLruCachePosRead
    typedef const byte_t* (*pread_func_t)(void* lambda, size_t offset, size_t len, valvec<byte_t>* rdbuf);
    terark_forceinline
    void fspread_record_append(pread_func_t fspread, void* lambda,
                               size_t baseOffset, size_t recID,
                               valvec<byte_t>* recData,
                               valvec<byte_t>* rdbuf) const {
        (this->*m_fspread_record_append)(fspread, lambda, baseOffset, recID, recData, rdbuf);
    }
    void fspread_record_append(pread_func_t fspread, void* lambda,
                               size_t baseOffset, size_t recID,
                               valvec<byte_t>* recData) const;
    terark_forceinline
    void fspread_record(pread_func_t fspread, void* lambda,
                        size_t baseOffset, size_t recID,
                        valvec<byte_t>* recData,
                        valvec<byte_t>* rdbuf) const {
        recData->risk_set_size(0);
        (this->*m_fspread_record_append)(fspread, lambda, baseOffset, recID, recData, rdbuf);
    }
    terark_forceinline
    void fspread_record(pread_func_t fspread, void* lambda,
                        size_t baseOffset, size_t recID,
                        valvec<byte_t>* recData) const {
        recData->risk_set_size(0);
        fspread_record_append(fspread, lambda, baseOffset, recID, recData);
    }

protected:
    size_t      m_numRecords;
    uint64_t    m_unzipSize;

    typedef void (BlobStore::*get_record_append_func_t)(size_t recID, valvec<byte_t>* recData) const;
    get_record_append_func_t m_get_record_append;

    typedef void (BlobStore::*get_record_append_CacheOffsets_func_t)(size_t recID, CacheOffsets*) const;
    get_record_append_CacheOffsets_func_t m_get_record_append_CacheOffsets;

    typedef void (BlobStore::*fspread_record_append_func_t)(
                        pread_func_t,
                        void* lambdaObj,
                        size_t baseOffset,
                        size_t recID,
                        valvec<byte_t>* recData,
                        valvec<byte_t>* buf) const;
    fspread_record_append_func_t m_fspread_record_append;

    typedef void (BlobStore::*pread_record_append_func_t)(
                        LruReadonlyCache* cache,
                        intptr_t fd,
                        size_t baseOffset,
                        size_t recID,
                        valvec<byte_t>* recData,
                        valvec<byte_t>* buf) const;
    pread_record_append_func_t m_pread_record_append;

    void pread_record_append_default_impl(
                        LruReadonlyCache* cache,
                        intptr_t fd,
                        size_t baseOffset,
                        size_t recID,
                        valvec<byte_t>* recData,
                        valvec<byte_t>* buf) const;

    static const byte_t* os_fspread(void* lambda, size_t offset, size_t len,
                                    valvec<byte_t>* rdbuf);
};

template<> struct BlobStoreRecBuffer<true> : BlobStore::CacheOffsets {
    const
    valvec<byte_t>& getRecData() const { return this->recData; }
    valvec<byte_t>& getRecData()       { return this->recData; }
};
template<> struct BlobStoreRecBuffer<false> : valvec<byte_t> {
    const
    valvec<byte_t>& getRecData() const { return *this; }
    valvec<byte_t>& getRecData()       { return *this; }
    void invalidate_offsets_cache() {} // do nothing
};

} // namespace terark

