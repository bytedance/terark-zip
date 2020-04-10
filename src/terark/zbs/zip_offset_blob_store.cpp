/*
 * zip_offset_blob_store.cpp
 *
 *  Created on: 2017-06-01
 *      Author: leipeng
 */

#include "zip_offset_blob_store.hpp"
#include "blob_store_file_header.hpp"
#include "zip_reorder_map.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/IStreamWrapper.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/checksum_exception.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/int_vector.hpp>
#include <zstd/zstd.h>

namespace terark {

REGISTER_BlobStore(ZipOffsetBlobStore, "ZipOffsetBlobStore");

static const uint64_t g_dpbsnark_seed = 0x5342426e69616c50ull; // echo PlainBBS | od -t x8

struct ZipOffsetBlobStore::FileHeader : public FileHeaderBase {
    uint64_t  contentBytes;
    uint64_t  offsetsBytes; // same as footer.indexBytes
    uint08_t  offsets_log2_blockUnits; // 6 or 7
    uint08_t  checksumLevel;
    uint08_t  compressLevel;
    uint08_t  padding21[5];
    uint64_t  padding22[3];

    void init() {
        BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
        memset(this, 0, sizeof(*this));
        magic_len = MagicStrLen;
        strcpy(magic, MagicString);
        strcpy(className, "ZipOffsetBlobStore");
    }
    FileHeader(fstring mem, size_t content_size, size_t offsets_size, Options _options) {
        init();
        fileSize = mem.size();
        assert(fileSize == 0
            + sizeof(FileHeader)
            + align_up(content_size, 16)
            + offsets_size
            + sizeof(BlobStoreFileFooter));
        SortedUintVec offsets;
        offsets.risk_set_data(mem.data() + sizeof(FileHeader) + align_up(content_size, 16), offsets_size);
        unzipSize = content_size;
        records = offsets.size() - 1;
        contentBytes = content_size;
        offsetsBytes = offsets_size;
        offsets_log2_blockUnits = offsets.log2_block_units();
        offsets.risk_release_ownership();
        checksumLevel = static_cast<uint08_t>(_options.checksum_level);
        checksumType = static_cast<uint08_t>(_options.checksum_type);
        compressLevel = static_cast<uint08_t>(_options.compress_level);
    }
    FileHeader(const ZipOffsetBlobStore* store, const SortedUintVec& offsets) {
        init();
        fileSize = 0
            + sizeof(FileHeader)
            + align_up(store->m_content.size(), 16)
            + offsets.mem_size()
            + sizeof(BlobStoreFileFooter);
        unzipSize = store->m_content.size();
        records = store->m_numRecords;
        contentBytes = store->m_content.size();
        offsetsBytes = offsets.mem_size();
        offsets_log2_blockUnits = offsets.log2_block_units();
        checksumLevel = static_cast<uint08_t>(store->m_checksumLevel);
        checksumType = static_cast<uint08_t>(store->m_checksumType);
        compressLevel = static_cast<uint08_t>(store->m_compressLevel);
    }
};

void ZipOffsetBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
    auto mmapBase = (const FileHeader*)dataMem.p;
    m_mmapBase = mmapBase;
    m_numRecords = mmapBase->records;
    m_unzipSize = mmapBase->contentBytes;
    m_checksumLevel = mmapBase->checksumLevel;
    m_checksumType = mmapBase->checksumType;
    m_compressLevel = mmapBase->compressLevel;
    if (m_checksumLevel == 3 && isChecksumVerifyEnabled()) {
        XXHash64 hash(g_dpbsnark_seed);
        hash.update(mmapBase, mmapBase->fileSize - sizeof(BlobStoreFileFooter));
        const uint64_t hashVal = hash.digest();
        auto &footer = ((const BlobStoreFileFooter*)((const byte_t*)(mmapBase) + mmapBase->fileSize))[-1];
        if (hashVal != footer.fileXXHash) {
            std::string msg = "ZipOffsetBlobStore::load_mmap(\"" + m_fpath + "\")";
            throw BadChecksumException(msg, footer.fileXXHash, hashVal);
        }
    }
    m_content.risk_set_data((byte_t*)(mmapBase + 1), mmapBase->contentBytes);
    m_offsets.risk_set_data(m_content.data() + align_up(m_content.size(), 16), mmapBase->offsetsBytes);
    assert(m_offsets.size() == mmapBase->records+1);
    assert(m_offsets.mem_size() == mmapBase->offsetsBytes);
    if (m_offsets.size() != mmapBase->records+1) {
        TERARK_THROW(std::length_error
            , "m_offsets.size() = %zd, mmapBase->records+1 = %lld, must be equal"
            ,  m_offsets.size(), llong(mmapBase->records+1)
        );
    }
    if (m_offsets.mem_size() != mmapBase->offsetsBytes) {
        TERARK_THROW(std::length_error
            , "m_offsets.mem_size() = %zd, footer.indexBytes = %lld, must be equal"
            ,  m_offsets.mem_size(), llong(mmapBase->offsetsBytes)
        );
    }
}

void ZipOffsetBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_offsets.data(), m_offsets.mem_size());
}

void ZipOffsetBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_content);
}

void ZipOffsetBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    assert(blocks.size() == 1);
    auto offset_mem = blocks.front();
    assert(offset_mem.size() == m_offsets.mem_size());
    if (m_isUserMem) {
        m_offsets.risk_release_ownership();
    } else {
        m_offsets.clear();
    }
    m_offsets.risk_set_data((byte_t*)offset_mem.data(), offset_mem.size());
    m_isDetachMeta = true;
}

void ZipOffsetBlobStore::save_mmap(function<void(const void*, size_t)> write) const {
    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);

    assert(m_offsets.mem_size() % 16 == 0);
    XXHash64 xxhash64(g_dpbsnark_seed);
    FileHeader header(this, m_offsets);

    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof(header));

    xxhash64.update(m_content.data(), m_content.size());
    buffer.ensureWrite(m_content.data(), m_content.size());

    PadzeroForAlign<16>(buffer, xxhash64, m_content.size());

    xxhash64.update(m_offsets.data(), m_offsets.mem_size());
    buffer.ensureWrite(m_offsets.data(), m_offsets.mem_size());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

ZipOffsetBlobStore::ZipOffsetBlobStore() {
    m_checksumLevel = 3; // check all data
    m_checksumType = 0;  // crc32c
    m_get_record_append = static_cast<get_record_append_func_t>
                (&ZipOffsetBlobStore::get_record_append_imp);
    m_fspread_record_append = static_cast<fspread_record_append_func_t>
                (&ZipOffsetBlobStore::fspread_record_append_imp);
    m_get_record_append_CacheOffsets =
        static_cast<get_record_append_CacheOffsets_func_t>
        (&ZipOffsetBlobStore::get_record_append_CacheOffsets);
}

ZipOffsetBlobStore::~ZipOffsetBlobStore() {
    if (m_isDetachMeta) {
        m_offsets.risk_release_ownership();
    }
    if (m_isUserMem) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
        m_isUserMem = false;
        m_content.risk_release_ownership();
        m_offsets.risk_release_ownership();
    }
    else {
        m_content.clear();
        m_offsets.clear();
    }
}


void ZipOffsetBlobStore::swap(ZipOffsetBlobStore& other) {
  AbstractBlobStore::risk_swap(other);
  std::swap(m_compressLevel, other.m_compressLevel);
  m_content.swap(other.m_content);
  m_offsets.swap(other.m_offsets);
}

size_t ZipOffsetBlobStore::mem_size() const {
    return m_content.size() + m_offsets.mem_size();
}

static void ZipOffsetBlobStore_AppendDecompress(size_t id, const byte_t* data, size_t size, valvec<byte_t>* output) {
    unsigned long long raw_size = ZSTD_getDecompressedSize(data, size);
    size_t curr_size = output->size();
    output->resize_no_init(curr_size + raw_size);
    size = ZSTD_decompress(output->data() + curr_size, raw_size, data, size);
    if (ZSTD_isError(size)) {
      TERARK_THROW(std::logic_error
          , "ZipOffsetBlobStore_AppendDecompress: rec_id = %zd, error %s"
          , id, ZSTD_getErrorName(size));
    }
  output->risk_set_size(curr_size + size);
}

void
ZipOffsetBlobStore::get_record_append_imp(size_t recID, valvec<byte_t>* recData)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    assert(BegEnd[1] <= m_content.size());
    size_t len = BegEnd[1] - BegEnd[0];
    const byte_t* pData = m_content.data() + BegEnd[0];
    if (m_compressLevel > 0) {
        ZipOffsetBlobStore_AppendDecompress(recID, pData, len, recData);
        return;
    }
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            len -= sizeof(uint16_t);
            uint16_t crc1 = unaligned_load<uint16_t>(pData + len);
            uint16_t crc2 = Crc16c_update(0, pData, len);
            if (crc2 != crc1) {
                throw BadCrc16cException(
                        "ZipOffsetBlobStore::get_record_append_imp", crc1, crc2);
            }
        } else {
            len -= sizeof(uint32_t);
            uint32_t crc1 = unaligned_load<uint32_t>(pData + len);
            uint32_t crc2 = Crc32c_update(0, pData, len);
            if (crc2 != crc1) {
                throw BadCrc32cException(
                        "ZipOffsetBlobStore::get_record_append_imp", crc1, crc2);
            }
        }
    }
    recData->append(m_content.data() + BegEnd[0], len);
}

void
ZipOffsetBlobStore::get_record_append_CacheOffsets(size_t recID, CacheOffsets* co)
const {
    assert(recID + 1 < m_offsets.size());
    size_t log2 = m_offsets.log2_block_units(); // must be 6 or 7
    size_t mask = (size_t(1) << log2) - 1;
    if (terark_unlikely(recID >> log2 != co->blockId)) {
        // cache miss, load the block
        size_t blockIdx = recID >> log2;
        m_offsets.get_block(blockIdx, co->offsets);
        co->offsets[mask+1] = m_offsets.get_block_min_val(blockIdx+1);
        co->blockId = blockIdx;
    }
    size_t inBlockID = recID & mask;
    size_t BegEnd[2] = { co->offsets[inBlockID], co->offsets[inBlockID+1] };
    size_t len = BegEnd[1] - BegEnd[0];
    const byte_t* pData = m_content.data() + BegEnd[0];
    if (m_compressLevel > 0) {
        ZipOffsetBlobStore_AppendDecompress(recID, pData, len, &co->recData);
        return;
    }
     if (2 == m_checksumLevel) {
         if (kCRC16C == m_checksumType) {
             len -= sizeof(uint16_t);
             uint16_t crc1 = unaligned_load<uint16_t>(pData + len);
             uint16_t crc2 = Crc16c_update(0, pData, len);
             if (crc2 != crc1) {
                 throw BadCrc16cException(
                         "ZipOffsetBlobStore::get_record_append_CacheOffsets", crc1, crc2);
             }
         } else {
             len -= sizeof(uint32_t);
             uint32_t crc1 = unaligned_load<uint32_t>(pData + len);
             uint32_t crc2 = Crc32c_update(0, pData, len);
             if (crc2 != crc1) {
                 throw BadCrc32cException(
                         "ZipOffsetBlobStore::get_record_append_CacheOffsets", crc1, crc2);
             }
         }
    }
    co->recData.append(pData, len);
}

void
ZipOffsetBlobStore::fspread_record_append_imp(
                    pread_func_t fspread, void* lambda,
                    size_t baseOffset, size_t recID,
                    valvec<byte_t>* recData,
                    valvec<byte_t>* rdbuf)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    assert(BegEnd[1] <= m_content.size());
    size_t len = BegEnd[1] - BegEnd[0];
    size_t offset = sizeof(FileHeader) + BegEnd[0];
    auto pData = fspread(lambda, baseOffset + offset, len, rdbuf);
    assert(NULL != pData);
    if (m_compressLevel > 0) {
        ZipOffsetBlobStore_AppendDecompress(recID, pData, len, recData);
        return;
    }
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            len -= sizeof(uint16_t);
            uint16_t crc1 = unaligned_load<uint16_t>(pData + len);
            uint16_t crc2 = Crc16c_update(0, pData, len);
            if (crc2 != crc1) {
                throw BadCrc16cException(
                        "ZipOffsetBlobStore::fspread_record_append_imp", crc1, crc2);
            }
        } else {
            len -= sizeof(uint32_t);
            uint32_t crc1 = unaligned_load<uint32_t>(pData + len);
            uint32_t crc2 = Crc32c_update(0, pData, len);
            if (crc2 != crc1) {
                throw BadCrc32cException(
                        "ZipOffsetBlobStore::fspread_record_append_imp", crc1, crc2);
            }
        }
    }
    recData->append(pData, len);
}

void ZipOffsetBlobStore::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {
    FunctionAdaptBuffer adaptBuffer(writeAppend);
    OutputBuffer buffer(&adaptBuffer);
    size_t recNum = m_numRecords;
    TERARK_UNUSED_VAR(recNum);
    size_t offset = 0;
    auto zipOffsetBuilder = std::unique_ptr<SortedUintVec::Builder>(
        SortedUintVec::createBuilder(m_offsets.block_units(), tmpFile.c_str()));
#if !defined(NDEBUG)
    size_t maxOffsetEnt = m_offsets[recNum];
#endif
    for (assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
        size_t oldId = *newToOld;
        assert(oldId < recNum);
        size_t BegEnd[2];
        m_offsets.get2(oldId, BegEnd);
        zipOffsetBuilder->push_back(offset);
        assert(BegEnd[0] <= BegEnd[1]);
        offset += BegEnd[1] - BegEnd[0];
    }
    assert(offset == maxOffsetEnt);
    zipOffsetBuilder->push_back(offset);
    zipOffsetBuilder->finish(nullptr);
    zipOffsetBuilder.reset();
    XXHash64 xxhash64(g_dpbsnark_seed);
    MmapWholeFile mmapOffset(tmpFile);
    SortedUintVec newZipOffsets;
    newZipOffsets.risk_set_data(mmapOffset.base, mmapOffset.size);
    TERARK_SCOPE_EXIT(newZipOffsets.risk_release_ownership());
    FileHeader header(this, newZipOffsets);
    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof header);
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
        size_t oldId = *newToOld;
        size_t BegEnd[2];
        m_offsets.get2(oldId, BegEnd);
        size_t len = BegEnd[1] - BegEnd[0];
        const byte* beg = m_content.data() + BegEnd[0];
        xxhash64.update(beg, len);
        buffer.ensureWrite(beg, len);
    }
    PadzeroForAlign<16>(buffer, xxhash64, offset);

    xxhash64.update(newZipOffsets.data(), newZipOffsets.mem_size());
    buffer.ensureWrite(newZipOffsets.data(), newZipOffsets.mem_size());

    MmapWholeFile().swap(mmapOffset);
    ::remove(tmpFile.c_str());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

///////////////////////////////////////////////////////////////////////////
class ZipOffsetBlobStore::MyBuilder::Impl : boost::noncopyable {
    std::string m_fpath;
    std::string m_fpath_offset;
    std::unique_ptr<SortedUintVec::Builder> m_builder;
    FileStream m_file;
    SeekableOutputStreamWrapper<FileMemIO*> m_memStream;
    NativeDataOutput<OutputBuffer> m_writer;
    valvec<byte_t> m_compressBuffer;
    size_t m_offset;
    size_t m_content_size;
    Options m_options;
public:
    Impl(fstring fpath, size_t offset, Options options)
        : m_fpath(fpath.begin(), fpath.end())
        , m_fpath_offset(fpath + ".offset")
        , m_builder(SortedUintVec::createBuilder(options.block_units, m_fpath_offset.c_str()))
        , m_file()
        , m_memStream(nullptr)
        , m_writer(&m_file)
        , m_offset(offset)
        , m_content_size(0)
        , m_options(options) {
        assert(offset % 8 == 0);
        if (offset == 0) {
          m_file.open(fpath, "wb");
        }
        else {
          m_file.open(fpath, "rb+");
          m_file.seek(offset);
        }
        m_file.disbuf();
        std::aligned_storage<sizeof(FileHeader)>::type header;
        memset(&header, 0, sizeof header);
        m_writer.ensureWrite(&header, sizeof header);
    }
    Impl(FileMemIO& mem, Options options)
        : m_fpath()
        , m_fpath_offset()
        , m_builder(SortedUintVec::createBuilder(options.block_units))
        , m_file()
        , m_memStream(&mem)
        , m_writer(&m_memStream)
        , m_offset(0)
        , m_content_size(0)
        , m_options(options) {
        std::aligned_storage<sizeof(FileHeader)>::type header;
        memset(&header, 0, sizeof header);
        m_writer.ensureWrite(&header, sizeof header);
    }
    void add_record(fstring rec) {
        if (m_options.compress_level > 0) {
            m_compressBuffer.resize_no_init(ZSTD_compressBound(rec.size()));
            size_t zstd_size = ZSTD_compress(m_compressBuffer.data(), m_compressBuffer.size(), rec.data(), rec.size(),
                                             m_options.compress_level - 1);
            if (ZSTD_isError(zstd_size)) {
                  TERARK_THROW(std::logic_error
                      , "ZipOffsetBlobStore::MyBuilder::add_record: error %s"
                      , ZSTD_getErrorName(zstd_size));
            }
            rec = fstring(m_compressBuffer.data(), zstd_size);
        }
        m_builder->push_back(m_content_size);
        m_writer.ensureWrite(rec.data(), rec.size());
        m_content_size += rec.size();
        if (2 == m_options.checksum_level) {
            if (kCRC16C == m_options.checksum_type) {
                uint16_t crc = Crc16c_update(0, rec.data(), rec.size());
                m_writer.ensureWrite(&crc, sizeof(crc));
                m_content_size += sizeof(crc);
            } else {
                uint32_t crc = Crc32c_update(0, rec.data(), rec.size());
                m_writer.ensureWrite(&crc, sizeof(crc));
                m_content_size += sizeof(crc);
            }
        }
    }
    void finish() {
        PadzeroForAlign<16>(m_writer, m_content_size);
        m_builder->push_back(m_content_size);
        if (m_file.fp() == nullptr) {
            SortedUintVec vec;
            m_builder->finish(&vec);
            m_builder.reset();

            size_t offsets_size = vec.mem_size();
            m_writer.ensureWrite(vec.data(), offsets_size);
            m_writer.flush_buffer();

            size_t file_size = m_offset
                + sizeof(FileHeader)
                + align_up(m_content_size, 16)
                + offsets_size
                + sizeof(BlobStoreFileFooter);

            assert(m_memStream.size() == file_size - sizeof(BlobStoreFileFooter));
            m_memStream.stream()->resize(file_size);
            *(FileHeader*)m_memStream.stream()->begin() =
                FileHeader(fstring(m_memStream.stream()->begin(), m_memStream.size()), m_content_size, offsets_size,
                                   m_options);

            XXHash64 xxhash64(g_dpbsnark_seed);
            xxhash64.update(m_memStream.stream()->begin(), m_memStream.size() - sizeof(BlobStoreFileFooter));

            BlobStoreFileFooter footer;
            footer.fileXXHash = xxhash64.digest();
            m_memStream.stream()->resize(file_size);
            ((BlobStoreFileFooter*)(m_memStream.stream()->end()))[-1] = footer;
        } else {
            m_builder->finish(nullptr);
            m_builder.reset();

            FileStream offset(m_fpath_offset, "rb");
            size_t offsets_size = offset.fsize();
            m_writer.flush_buffer();
            m_file.cat(offset);
            offset.close();
            ::remove(m_fpath_offset.c_str());
            m_file.close();

            size_t file_size = m_offset
                + sizeof(FileHeader)
                + align_up(m_content_size, 16)
                + offsets_size
                + sizeof(BlobStoreFileFooter);
            assert(FileStream(m_fpath, "rb+").fsize() == file_size - sizeof(BlobStoreFileFooter));
            FileStream(m_fpath, "rb+").chsize(file_size);
            MmapWholeFile mmap(m_fpath, true);
            fstring mem((const char*)mmap.base + m_offset, (ptrdiff_t)(file_size - m_offset));
            *(FileHeader*)((byte_t*)mmap.base + m_offset) = FileHeader(mem, m_content_size, offsets_size, 
                                                                       m_options);

            XXHash64 xxhash64(g_dpbsnark_seed);
            xxhash64.update(mem.data(), mem.size() - sizeof(BlobStoreFileFooter));

            BlobStoreFileFooter footer;
            footer.fileXXHash = xxhash64.digest();
            ((BlobStoreFileFooter*)(mem.data() + mem.size()))[-1] = footer;
        }
    }
};

ZipOffsetBlobStore::MyBuilder::~MyBuilder() {
    delete impl;
}
ZipOffsetBlobStore::MyBuilder::MyBuilder(fstring fpath, size_t offset, Options options) {
    if (options.compress_level > 0) {
        options.checksum_level = 1;
        options.checksum_type = 0;
    }
    impl = new Impl(fpath, offset, options);
}
ZipOffsetBlobStore::MyBuilder::MyBuilder(FileMemIO& mem, Options options) {
    if (options.compress_level > 0) {
        options.checksum_level = 1;
        options.checksum_type = 0;
    }
    impl = new Impl(mem, options);
}
void ZipOffsetBlobStore::MyBuilder::addRecord(fstring rec) {
    assert(NULL != impl);
    impl->add_record(rec);
}
void ZipOffsetBlobStore::MyBuilder::finish() {
    assert(NULL != impl);
    return impl->finish();
}


} // namespace terark
