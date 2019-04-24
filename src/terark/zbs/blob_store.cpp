#include "abstract_blob_store.hpp"
#include "lru_page_cache.hpp"
#include <terark/util/function.hpp>

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

BlobStore::BlobStore() {
    m_numRecords = size_t(-1);
    m_unzipSize = uint64_t(-1);
    m_get_record_append = NULL;
    m_get_record_append_CacheOffsets = NULL;
    m_fspread_record_append = NULL;
    m_pread_record_append = &BlobStore::pread_record_append_default_impl;
}

BlobStore::~BlobStore() {
}

BlobStore* BlobStore::load_from_mmap(fstring fpath, bool mmapPopulate) {
    return AbstractBlobStore::load_from_mmap(fpath, mmapPopulate);
}

BlobStore* BlobStore::load_from_user_memory(fstring dataMem) {
    return AbstractBlobStore::load_from_user_memory(dataMem);
}

valvec<fstring> BlobStore::get_meta_blocks() const {
  valvec<fstring> blocks;
  this->get_meta_blocks(&blocks);
  return blocks;
}

valvec<fstring> BlobStore::get_data_blocks() const {
  valvec<fstring> blocks;
  this->get_data_blocks(&blocks);
  return blocks;
}

size_t BlobStore::lower_bound(size_t lo, size_t hi, fstring target,
                              CacheOffsets* co) const {
    assert(lo <= hi);
    assert(hi <= m_numRecords);
    struct {
        fstring operator[](ptrdiff_t i) {
            co_->recData.erase_all();
            (this_->*call_)(i, co_);
            last_ = i;
            return co_->recData;
        }
        const BlobStore * this_;
        get_record_append_CacheOffsets_func_t call_;
        CacheOffsets* co_;
        size_t last_;
    } it{ this, m_get_record_append_CacheOffsets, co, hi };
    size_t recId = lower_bound_n(it, lo, hi, target);
    if (recId != it.last_) {
        it[recId];
    }
    return recId;
}

static thread_local valvec<byte_t> tg_buf;

void BlobStore::pread_record_append(LruReadonlyCache* cache,
                                    intptr_t fd, size_t baseOffset,
                                    size_t recID, valvec<byte_t>* recData)
const {
    pread_record_append(cache, fd, baseOffset, recID, recData, &tg_buf);
}

void BlobStore::fspread_record_append(pread_func_t fspread, void* lambda,
                                      size_t baseOffset,
                                      size_t recID, valvec<byte_t>* recData)
const {
    fspread_record_append(fspread, lambda, baseOffset, recID, recData, &tg_buf);
}

const byte_t* BlobStore::os_fspread(void* lambda, size_t offset, size_t len,
                                    valvec<byte_t>* rdbuf) {
    rdbuf->resize_no_init(len);
    byte_t* buf = rdbuf->data();
#if defined(_MSC_VER)
    // lambda is HANDLE, which opened by `CreateFile`, not `open`
    OVERLAPPED ol; memset(&ol, 0, sizeof(ol));
    ol.Offset = (DWORD)offset;
    ol.OffsetHigh = (DWORD)(uint64_t(offset) >> 32);
    DWORD r = 0;
    HANDLE hFile = (HANDLE)lambda;
    if (!ReadFile(hFile, buf, len, &r, &ol) || r != len) {
        int err = GetLastError();
        THROW_STD(logic_error
            , "ReadFile(offset = %zd, len = %zd) = %u, GetLastError() = %d(0x%X)"
            , offset, len, r, err, err);
    }
#else
    int fd = (int)(intptr_t)lambda;
    size_t r = pread(fd, buf, len, offset);
    if (r != len) {
        THROW_STD(logic_error
            , "pread(offset = %zd, len = %zd) = %zd, err = %s"
            , offset, len, r, strerror(errno));
    }
#endif
    return buf;
}

struct BlobStoreLruCachePosRead {
    LruReadonlyCache::Buffer b;
    LruReadonlyCache* cache;
    intptr_t fi;
    size_t baseOffset;

    BlobStoreLruCachePosRead(valvec<byte_t>* rb) : b(rb) {
    }
    const byte_t*
    operator()(size_t offset, size_t len, valvec<byte_t>* buf) {
        return cache->pread(fi, baseOffset + offset, len, &b);
    }
};

// default implementation just use get_record_append
void BlobStore::pread_record_append_default_impl(
                    LruReadonlyCache* cache,
                    intptr_t fd,
                    size_t baseOffset,
                    size_t recID,
                    valvec<byte_t>* recData,
                    valvec<byte_t>* rdbuf)
const {
    if (cache) { // fd is really fi for cache
        BlobStoreLruCachePosRead fspread(rdbuf);
        fspread.cache = cache;
        fspread.fi    = fd; // fd is really fi for cache
        fspread.baseOffset = baseOffset;
        (this->*m_fspread_record_append)(c_callback(fspread), &fspread, baseOffset, recID, recData, rdbuf);
    }
    else {
        (this->*m_fspread_record_append)(&os_fspread, (void*)fd, baseOffset, recID, recData, rdbuf);
    }
}


} // namespace terark

