/*
 * entropy_zip_blob_store.cpp
 *
 *  Created on: 2019-05-28
 *      Author: zhaoming
 */

#include "entropy_zip_blob_store.hpp"
#include "blob_store_file_header.hpp"
#include "zip_reorder_map.hpp"
#include <terark/entropy/huffman_encoding.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/IStreamWrapper.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/checksum_exception.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/int_vector.hpp>
#include <utility>

namespace terark {

REGISTER_BlobStore(EntropyZipBlobStore, "EntropyZipBlobStore");

static const uint64_t g_debsnark_seed = 0x5342425f5a617445ull; // echo EtaZ_BBS | od -t x8

static size_t AlignEntropyZipSize(size_t bits, size_t table) {
  return (bits + table * 8 + 127) / 128 * 16;
}
struct EntropyZipBlobStore::FileHeader : public FileHeaderBase {
    uint64_t  contentBits;
    uint64_t  offsetsBytes; // same as footer.indexBytes
    uint08_t  offsets_log2_blockUnits; // 6 or 7
    uint08_t  entropyOrder;
    uint08_t  checksumLevel;
    // resue one-byte's pad space for entropyFlags
    uint08_t  entropyFlags;
    uint08_t  padding21[4];
    uint64_t  tableBytes;
    uint64_t  padding22[2];

    void init() {
        BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
        memset(this, 0, sizeof(*this));
        magic_len = MagicStrLen;
        strcpy(magic, MagicString);
        strcpy(className, "EntropyZipBlobStore");
    }
    FileHeader(fstring mem, size_t entropy_order, size_t raw_size,
            size_t entropy_bits, size_t offsets_size, size_t table_size,
            int _checksumLevel, int _checksumType, uint08_t _entropyFlags) {
        init();
        fileSize = mem.size();
        assert(fileSize == 0
            + sizeof(FileHeader)
            + AlignEntropyZipSize(entropy_bits, table_size)
            + offsets_size
            + sizeof(BlobStoreFileFooter));
        SortedUintVec offsets;
        offsets.risk_set_data(mem.data() + mem.size() - sizeof(BlobStoreFileFooter) - offsets_size, offsets_size);
        unzipSize = raw_size;
        records = offsets.size() - 1;
        contentBits = entropy_bits;
        offsetsBytes = offsets_size;
        offsets_log2_blockUnits = offsets.log2_block_units();
        entropyOrder = entropy_order;
        offsets.risk_release_ownership();
        tableBytes = table_size;
        checksumLevel = static_cast<uint08_t>(_checksumLevel);
        checksumType = static_cast<uint08_t>(_checksumType);
        entropyFlags = _entropyFlags;
    }
    FileHeader(const EntropyZipBlobStore* store, const SortedUintVec& offsets) {
        init();
        contentBits = offsets[offsets.size() - 1];
        tableBytes = store->m_table.size();
        fileSize = 0
            + sizeof(FileHeader)
            + AlignEntropyZipSize(contentBits, tableBytes)
            + offsets.mem_size()
            + sizeof(BlobStoreFileFooter);
        unzipSize = store->m_unzipSize;
        records = store->m_numRecords;
        offsetsBytes = offsets.mem_size();
        offsets_log2_blockUnits = offsets.log2_block_units();
        entropyOrder = store->m_decoder_o0 ? 0 : 1;
        checksumLevel = static_cast<uint08_t>(store->m_checksumLevel);
        checksumType = static_cast<uint08_t>(store->m_checksumType);
        entropyFlags = store->get_entropy_flags();
    }
};

uint08_t EntropyZipBlobStore::get_entropy_flags() const {
        return ((const FileHeader*)m_mmapBase)->entropyFlags;
}
void EntropyZipBlobStore::init_get_calls(size_t order) {
    if (order == 0) {
      m_get_record_append = static_cast<get_record_append_func_t>
          (&EntropyZipBlobStore::get_record_append_imp<0>);
      m_fspread_record_append = static_cast<fspread_record_append_func_t>
          (&EntropyZipBlobStore::fspread_record_append_imp<0>);
      m_get_record_append_CacheOffsets =
          static_cast<get_record_append_CacheOffsets_func_t>
          (&EntropyZipBlobStore::get_record_append_CacheOffsets<0>);
    } else {
        m_get_record_append = static_cast<get_record_append_func_t>
            (&EntropyZipBlobStore::get_record_append_imp<1>);
        m_fspread_record_append = static_cast<fspread_record_append_func_t>
            (&EntropyZipBlobStore::fspread_record_append_imp<1>);
        m_get_record_append_CacheOffsets =
            static_cast<get_record_append_CacheOffsets_func_t>
            (&EntropyZipBlobStore::get_record_append_CacheOffsets<1>);
    }
}

void EntropyZipBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
    auto mmapBase = (const FileHeader*)dataMem.p;
    m_mmapBase = mmapBase;
    m_numRecords = mmapBase->records;
    m_unzipSize = mmapBase->unzipSize;
    m_checksumLevel = mmapBase->checksumLevel;
    m_checksumType = mmapBase->checksumType;
    if (m_checksumLevel == 3 && isChecksumVerifyEnabled()) {
        XXHash64 hash(g_debsnark_seed);
        hash.update(mmapBase, mmapBase->fileSize - sizeof(BlobStoreFileFooter));
        const uint64_t hashVal = hash.digest();
        auto &footer = ((const BlobStoreFileFooter*)((const byte_t*)(mmapBase) + mmapBase->fileSize))[-1];
        if (hashVal != footer.fileXXHash) {
            std::string msg = "EntropyZipBlobStore::load_mmap(\"" + m_fpath + "\")";
            throw BadChecksumException(msg, footer.fileXXHash, hashVal);
        }
    }
    m_content.risk_set_data((byte_t*)(mmapBase + 1), (mmapBase->contentBits + 7) / 8);
    m_table.risk_set_data(m_content.data() + m_content.size(), mmapBase->tableBytes);
    m_offsets.risk_set_data(m_content.data() +
        AlignEntropyZipSize(mmapBase->contentBits, mmapBase->tableBytes), mmapBase->offsetsBytes);
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
    size_t table_size;
    if (mmapBase->entropyOrder == 0) {
        if (mmapBase->entropyFlags & NOCOMPRESS_FLAG) {
            assert(mmapBase->tableBytes == sizeof(Huffman::decoder));
            m_decoder_o0 = reinterpret_cast<Huffman::decoder*>(m_table.data());
            m_is_mmap_decoder = true;
            table_size = mmapBase->tableBytes;
        }
        else {
            m_decoder_o0 = new Huffman::decoder(m_table, &table_size);
        }
    } else {
        if (mmapBase->entropyFlags & NOCOMPRESS_FLAG) {
            assert(mmapBase->tableBytes == sizeof(Huffman::decoder_o1));
            m_decoder_o1 = reinterpret_cast<Huffman::decoder_o1*>(m_table.data());
            m_is_mmap_decoder = true;
            table_size = mmapBase->tableBytes;
        }
        else {
            m_decoder_o1 = new Huffman::decoder_o1(m_table, &table_size);
        }
    }
    assert(table_size == mmapBase->tableBytes);
    init_get_calls(mmapBase->entropyOrder);
}

void EntropyZipBlobStore::init_from_components(
    SortedUintVec&& offset, valvec<byte_t>&& data,
    valvec<byte_t>&& table, size_t order, uint64_t raw_size,
    int checksumLevel, int checksumType) {
    // XXX for test, need delete
    assert(false);

    m_numRecords = offset.size() - 1;
    m_unzipSize = raw_size;
    m_checksumLevel = checksumLevel;
    m_checksumType = checksumType;
    m_content.swap(data);
    m_table.swap(table);
    m_offsets.swap(offset);
    size_t table_size;
    if (order == 0) {
        // m_decoder_o0.reset(new Huffman::decoder(m_table, &table_size));
        m_decoder_o0 = new Huffman::decoder(m_table, &table_size);
    } else {
        m_decoder_o1 = new Huffman::decoder_o1(m_table, &table_size);
    }
    assert(table_size == m_table.size());
    init_get_calls(order);
    m_isUserMem = true;
}

void EntropyZipBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_offsets.data(), m_offsets.mem_size());
    assert(!(m_decoder_o0 != nullptr && m_decoder_o1 != nullptr));
    if (m_decoder_o0!=nullptr) {
        blocks->emplace_back(
                reinterpret_cast<const char*>(m_decoder_o0),
                sizeof(Huffman::decoder));
    }
    else {
        blocks->emplace_back(
                reinterpret_cast<const char*>(m_decoder_o1),
                sizeof(Huffman::decoder_o1));
    }
}

void EntropyZipBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_content);
}

void EntropyZipBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    assert(blocks.size() == 2);
    auto offset_mem = blocks.front();
    auto decoder_mem = blocks.back();
    assert(offset_mem.size() == m_offsets.mem_size());
    assert(decoder_mem.size() == sizeof(Huffman::decoder) ||
            decoder_mem.size() == sizeof(Huffman::decoder_o1));
    if (m_isUserMem) {
        m_offsets.risk_release_ownership();
    } else {
        m_offsets.clear();
    }
    m_offsets.risk_set_data((byte_t*)offset_mem.data(), offset_mem.size());

    if (m_decoder_o0!=nullptr) {
        m_decoder_o0 = reinterpret_cast<const Huffman::decoder*>(decoder_mem.data());
    }
    else {
        m_decoder_o1 = reinterpret_cast<const Huffman::decoder_o1*>(decoder_mem.data());
    }

    m_isDetachMeta = true;
}

void EntropyZipBlobStore::save_mmap(function<void(const void*, size_t)> write) const {
    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);

    assert(m_offsets.mem_size() % 16 == 0);
    XXHash64 xxhash64(g_debsnark_seed);
    FileHeader header(this, m_offsets);

    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof(header));

    xxhash64.update(m_content.data(), m_content.size());
    buffer.ensureWrite(m_content.data(), m_content.size());

    xxhash64.update(m_table.data(), m_table.size());
    buffer.ensureWrite(m_table.data(), m_table.size());
    PadzeroForAlign<16>(buffer, xxhash64, m_content.size() + m_table.size());

    xxhash64.update(m_offsets.data(), m_offsets.mem_size());
    buffer.ensureWrite(m_offsets.data(), m_offsets.mem_size());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

EntropyZipBlobStore::EntropyZipBlobStore() {
    m_checksumLevel = 3; // check all data
    m_checksumType = 0;  // crc32c
    m_decoder_o0 = nullptr;
    m_decoder_o1 = nullptr;
    m_is_mmap_decoder = false;
    init_get_calls(0);
}

EntropyZipBlobStore::~EntropyZipBlobStore() {
    if (m_isDetachMeta) {
        m_offsets.risk_release_ownership();
    }
    if (m_isUserMem) {
        if (m_decoder_o0!=nullptr && !m_is_mmap_decoder) {
            delete m_decoder_o0;
        }
        if (m_decoder_o1!=nullptr && !m_is_mmap_decoder) {
            delete m_decoder_o1;
        }
        m_decoder_o0 = nullptr;
        m_decoder_o1 = nullptr;

        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
        m_isUserMem = false;
        m_content.risk_release_ownership();
        m_offsets.risk_release_ownership();
        m_table.risk_release_ownership();
    }
    else {
        m_decoder_o0 = nullptr;
        m_decoder_o1 = nullptr;
        m_content.clear();
        m_offsets.clear();
        m_table.clear();
    }
}


void EntropyZipBlobStore::swap(EntropyZipBlobStore& other) {
  AbstractBlobStore::risk_swap(other);
  m_content.swap(other.m_content);
  m_offsets.swap(other.m_offsets);
  m_table.swap(other.m_table);
  std::swap(m_decoder_o0, other.m_decoder_o0);
  std::swap(m_decoder_o1, other.m_decoder_o1);
  m_is_mmap_decoder = other.m_is_mmap_decoder;
}

size_t EntropyZipBlobStore::mem_size() const {
    return m_content.size() + m_offsets.mem_size() + m_table.size();
}

template<size_t Order>
void
EntropyZipBlobStore::get_record_append_imp(size_t recID, valvec<byte_t>* recData)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    size_t len = BegEnd[1] - BegEnd[0];
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            len -= 16; // crc16c costs 8 * 2 = 16bits
        } else {
            len -= 32; // crc32c costs 8 * 4 = 32bits
        }
    }
    auto ctx = GetTlsTerarkContext();
    EntropyBits bits = {
        (byte_t*)m_content.data(), BegEnd[0], len, {}
    };
    auto ctx_data = ctx->alloc();
    bool ok;
    if (Order == 0) {
        ok = m_decoder_o0->bitwise_decode(bits, &ctx_data.get(), ctx);
    } else {
        ok = m_decoder_o1->bitwise_decode_x1(bits, &ctx_data.get(), ctx);
    }
    if (!ok) {
        THROW_STD(logic_error, "EntropyZipBlobStore Huffman decode error");
    }
    assert(ok); (void)ok;

    const auto& data = ctx_data.get();
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            uint16_t crc1 = load_uint16_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint16_t crc2 = Crc16c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                throw BadCrc16cException(
                        "EntropyZipBlobStore::get_record_append_imp", crc1, crc2);
            }
        } else {
            uint32_t crc1 = load_uint32_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint32_t crc2 = Crc32c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                 throw BadCrc32cException(
                        "EntropyZipBlobStore::get_record_append_imp", crc1, crc2);
            }
        }
    }
    recData->append(data);
}

template<size_t Order>
void
EntropyZipBlobStore::get_record_append_CacheOffsets(size_t recID, CacheOffsets* co)
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
    auto ctx = GetTlsTerarkContext();
    size_t len = BegEnd[1] - BegEnd[0];
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            len -= 16; // crc16c costs 8 * 2 = 16bits
        } else {
            len -= 32; // crc32c costs 8 * 4 = 32bits
        }
    }
    EntropyBits bits = {
        (byte_t*)m_content.data(), BegEnd[0], len, {}
    };
    auto ctx_data = ctx->alloc();
    bool ok;
    if (Order == 0) {
        ok = m_decoder_o0->bitwise_decode(bits, &ctx_data.get(), ctx);
    } else {
        ok = m_decoder_o1->bitwise_decode_x1(bits, &ctx_data.get(), ctx);
    }
    if (!ok) {
        THROW_STD(logic_error, "EntropyZipBlobStore Huffman decode error");
    }

    const auto& data = ctx_data.get();
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            uint16_t crc1 = load_uint16_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint16_t crc2 = Crc16c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                throw BadCrc16cException(
                        "EntropyZipBlobStore::get_record_append_CacheOffsets", crc1, crc2);
            }
        } else {
            uint32_t crc1 = load_uint32_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint32_t crc2 = Crc32c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                 throw BadCrc32cException(
                        "EntropyZipBlobStore::get_record_append_CacheOffsets", crc1, crc2);
            }
        }
    }
    co->recData.append(data);
}

template<size_t Order>
void
EntropyZipBlobStore::fspread_record_append_imp(
                    pread_func_t fspread, void* lambda,
                    size_t baseOffset, size_t recID,
                    valvec<byte_t>* recData,
                    valvec<byte_t>* rdbuf)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    size_t byte_beg = (BegEnd[0] - BegEnd[0] % 64) / 8;
    size_t byte_end = (BegEnd[1] + 63) / 64 * 8;
    size_t offset = sizeof(FileHeader) + byte_beg;
    auto pData = fspread(lambda, baseOffset + offset, byte_end - byte_beg, rdbuf);
    assert(NULL != pData);
    auto ctx = GetTlsTerarkContext();
    size_t len = BegEnd[1] - BegEnd[0];
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            len -= 16; // crc16c costs 8 * 2 = 16bits
        } else {
            len -= 32; // crc32c costs 8 * 4 = 32bits
        }
    }
    EntropyBits bits = {
        (byte_t*)pData, BegEnd[0] - byte_beg * 8, len, {}
    };
    auto ctx_data = ctx->alloc();
    bool ok;
    if (Order == 0) {
        ok = m_decoder_o0->bitwise_decode(bits, &ctx_data.get(), ctx);
    } else {
        ok = m_decoder_o1->bitwise_decode_x1(bits, &ctx_data.get(), ctx);
    }
    if (!ok) {
        THROW_STD(logic_error, "EntropyZipBlobStore Huffman decode error");
    }

    const auto& data = ctx_data.get();
    if (2 == m_checksumLevel) {
        if (kCRC16C == m_checksumType) {
            uint16_t crc1 = load_uint16_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint16_t crc2 = Crc16c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                throw BadCrc16cException(
                        "EntropyZipBlobStore::fspread_record_append_imp", crc1, crc2);
            }
        } else {
            uint32_t crc1 = load_uint32_from_bits((byte_t*)m_content.data(), BegEnd[0] + len);
            uint32_t crc2 = Crc32c_update(0, data.data(), data.size());
            if (crc2 != crc1) {
                 throw BadCrc32cException(
                        "EntropyZipBlobStore::fspread_record_append_imp", crc1, crc2);
            }
        }
    }
    recData->append(data);
}

void EntropyZipBlobStore::reorder_zip_data(ZReorderMap& newToOld,
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
    XXHash64 xxhash64(g_debsnark_seed);
    MmapWholeFile mmapOffset(tmpFile);
    SortedUintVec newZipOffsets;
    newZipOffsets.risk_set_data(mmapOffset.base, mmapOffset.size);
    TERARK_SCOPE_EXIT(newZipOffsets.risk_release_ownership());
    FileHeader header(this, newZipOffsets);
    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof header);
    offset = 0;
    auto output_data = [&offset, &buffer, &xxhash64](const void* data, size_t size) {
        offset += size;
        xxhash64.update(data, size);
        buffer.ensureWrite(data, size);
    };
    EntropyBitsWriter<decltype(output_data)> writer(output_data);
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
        size_t oldId = *newToOld;
        size_t BegEnd[2];
        m_offsets.get2(oldId, BegEnd);
        EntropyBits bits = {(byte_t*)m_content.data(), BegEnd[0], BegEnd[1] - BegEnd[0], {}};
        writer.write(bits);
    }
    writer.finish();

    xxhash64.update(m_table.data(), m_table.size());
    buffer.ensureWrite(m_table.data(), m_table.size());
    PadzeroForAlign<16>(buffer, xxhash64, offset + m_table.size());

    xxhash64.update(newZipOffsets.data(), newZipOffsets.mem_size());
    buffer.ensureWrite(newZipOffsets.data(), newZipOffsets.mem_size());

    MmapWholeFile().swap(mmapOffset);
    ::remove(tmpFile.c_str());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

///////////////////////////////////////////////////////////////////////////
class EntropyZipBlobStore::MyBuilder::Impl : boost::noncopyable {
    std::string m_fpath;
    std::string m_fpath_offset;
    std::unique_ptr<SortedUintVec::Builder> m_builder;
    FileStream m_file;
    SeekableOutputStreamWrapper<FileMemIO*> m_memStream;
    NativeDataOutput<OutputBuffer> m_writer;
    std::unique_ptr<Huffman::encoder> m_encoder_o0;
    std::unique_ptr<Huffman::encoder_o1> m_encoder_o1;
    std::function<void(const void*, size_t)> m_output;
    EntropyBitsWriter<std::function<void(const void*, size_t)>> m_bitWriter;
    TerarkContext m_ctx;
    size_t m_offset;
    size_t m_raw_size;
    size_t m_output_size;
    size_t m_entropy_bits;
    int m_checksumLevel;
    int m_checksumType;
    // XXX maybe expand to an option class future
    // now, just use one bit for entroytablenocompress
    uint08_t         m_entropyFlags;

public:
    Impl(freq_hist_o1& freq, size_t blockUnits, fstring fpath, size_t offset, int checksumLevel, int checksumType,
            uint08_t entropyFlags)
        : m_fpath(fpath.begin(), fpath.end())
        , m_fpath_offset(fpath + ".offset")
        , m_builder(SortedUintVec::createBuilder(blockUnits, m_fpath_offset.c_str()))
        , m_file()
        , m_memStream(nullptr)
        , m_writer(&m_file)
        , m_bitWriter(m_output)
        , m_offset(offset)
        , m_raw_size(0)
        , m_output_size(0)
        , m_entropy_bits(0)
        , m_checksumLevel(checksumLevel)
        , m_checksumType(checksumType)
        , m_entropyFlags(entropyFlags) {
        assert(offset % 8 == 0);
        if (offset == 0) {
          m_file.open(fpath, "wb");
        }
        else {
          m_file.open(fpath, "rb+");
          m_file.seek(offset);
        }
        m_file.disbuf();
        init(freq);
    }
    Impl(freq_hist_o1& freq, size_t blockUnits, FileMemIO& mem, int checksumLevel, int checksumType,
            uint08_t entropyFlags)
        : m_fpath()
        , m_fpath_offset()
        , m_builder(SortedUintVec::createBuilder(blockUnits))
        , m_file()
        , m_memStream(&mem)
        , m_writer(&m_memStream)
        , m_bitWriter(m_output)
        , m_offset(0)
        , m_raw_size(0)
        , m_output_size(0)
        , m_entropy_bits(0)
        , m_checksumLevel(checksumLevel)
        , m_checksumType(checksumType)
        , m_entropyFlags(entropyFlags) {
        init(freq);
    }
    void init(freq_hist_o1& freq) {
        size_t entropy_len_o0 = freq_hist::estimate_size(freq.histogram());
        size_t entropy_len_o1 = freq_hist_o1::estimate_size(freq.histogram());
        freq.normalise(Huffman::NORMALISE);
        if (entropy_len_o0 * 15 / 16 < entropy_len_o1) {
            m_encoder_o0.reset(new Huffman::encoder(freq.histogram()));
        } else {
            m_encoder_o1.reset(new Huffman::encoder_o1(freq.histogram()));
        }
        m_output = [this](const void* d, size_t s) {
            m_output_size += s;
            m_writer.ensureWrite(d, s);
        };
        std::aligned_storage<sizeof(FileHeader)>::type header;
        memset(&header, 0, sizeof header);
        m_writer.ensureWrite(&header, sizeof header);
    }
    void add_record(fstring rec) {
        EntropyBits bits;
        if (m_encoder_o0) {
            bits = m_encoder_o0->bitwise_encode(rec, &m_ctx);
        } else {
            bits = m_encoder_o1->bitwise_encode_x1(rec, &m_ctx);
        }
        m_builder->push_back(m_entropy_bits);
        m_bitWriter.write(bits);
        m_raw_size += rec.size();
        m_entropy_bits += bits.size;
        if (2 == m_checksumLevel) {
            if (kCRC16C == m_checksumType) {
                uint16_t crc = Crc16c_update(0, rec.data(), rec.size());
                bits = {reinterpret_cast<byte*>(&crc), 0, 16, {}};
                m_bitWriter.write(bits);
                m_raw_size += sizeof(crc);
                m_entropy_bits += bits.size;
            } else {
                uint32_t crc = Crc32c_update(0, rec.data(), rec.size());
                bits = {reinterpret_cast<byte*>(&crc), 0, 32, {}};
                m_bitWriter.write(bits);
                m_raw_size += sizeof(crc);
                m_entropy_bits += bits.size;
            }
        }
    }
    void finish() {
        auto bits = m_bitWriter.finish();
        assert(bits.size == m_entropy_bits); (void)bits;
        assert(m_output_size == (m_entropy_bits + 7) / 8);
        valvec<byte_t> table;
        size_t order;
        if (m_encoder_o0) {
            if (m_entropyFlags & NOCOMPRESS_FLAG) {
                // reset table from Ctable to Dtable
                table.ensure_capacity(sizeof(Huffman::decoder));
                new (table.data()) Huffman::decoder(fstring(m_encoder_o0->table().data(),
                                                        m_encoder_o0->table().size()));
                table.risk_set_size(sizeof(Huffman::decoder));
            } else {
                m_encoder_o0->take_table(&table);
            }
            m_encoder_o0.reset();
            order = 0;
        } else {
            if (m_entropyFlags & NOCOMPRESS_FLAG) {
                // reset table from Ctable to Dtable
                table.ensure_capacity(sizeof(Huffman::decoder_o1));
                new (table.data()) Huffman::decoder_o1(fstring(m_encoder_o1->table().data(),
                                                        m_encoder_o1->table().size()));
                table.risk_set_size(sizeof(Huffman::decoder_o1));
            } else {
                m_encoder_o1->take_table(&table);
            }
            m_encoder_o1.reset();
            order = 1;
        }
        m_writer.ensureWrite(table.data(), table.size());
        PadzeroForAlign<16>(m_writer, m_output_size + table.size());
        m_builder->push_back(m_entropy_bits);
        if (m_file.fp() == nullptr) {
            SortedUintVec vec;
            m_builder->finish(&vec);
            m_builder.reset();

            size_t offsets_size = vec.mem_size();
            m_writer.ensureWrite(vec.data(), offsets_size);
            m_writer.flush_buffer();

            size_t file_size = m_offset
                + sizeof(FileHeader)
                + AlignEntropyZipSize(m_entropy_bits, table.size())
                + offsets_size
                + sizeof(BlobStoreFileFooter);

            assert(m_memStream.size() == file_size - sizeof(BlobStoreFileFooter));
            m_memStream.stream()->resize(file_size);
            *(FileHeader*)m_memStream.stream()->begin() =
                FileHeader(fstring(m_memStream.stream()->begin(), m_memStream.size()),
                    order, m_raw_size, m_entropy_bits, offsets_size, table.size(),
                    m_checksumLevel, m_checksumType, m_entropyFlags);

            XXHash64 xxhash64(g_debsnark_seed);
            xxhash64.update(m_memStream.stream()->begin(), m_memStream.size() - sizeof(BlobStoreFileFooter));

            BlobStoreFileFooter footer;
            footer.fileXXHash = xxhash64.digest();
            m_memStream.stream()->resize(file_size);
            ((BlobStoreFileFooter*)(m_memStream.stream()->end()))[-1] = footer;
        }
        else {
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
                + AlignEntropyZipSize(m_entropy_bits, table.size())
                + offsets_size
                + sizeof(BlobStoreFileFooter);
            assert(FileStream(m_fpath, "rb+").fsize() == file_size - sizeof(BlobStoreFileFooter));
            FileStream(m_fpath, "rb+").chsize(file_size);
            MmapWholeFile mmap(m_fpath, true);
            fstring mem((const char*)mmap.base + m_offset, (ptrdiff_t)(file_size - m_offset));
            *(FileHeader*)mem.data() =
                FileHeader(mem, order, m_raw_size, m_entropy_bits, offsets_size, table.size(),
                           m_checksumLevel, m_checksumType, m_entropyFlags);

            XXHash64 xxhash64(g_debsnark_seed);
            xxhash64.update(mem.data(), mem.size() - sizeof(BlobStoreFileFooter));

            BlobStoreFileFooter footer;
            footer.fileXXHash = xxhash64.digest();
            ((BlobStoreFileFooter*)(mem.data() + mem.size()))[-1] = footer;
        }
    }
};

EntropyZipBlobStore::MyBuilder::~MyBuilder() {
    delete impl;
}
EntropyZipBlobStore::MyBuilder::MyBuilder(freq_hist_o1& freq, size_t blockUnits, fstring fpath, size_t offset,
                                          int checksumLevel, int checksumType, bool entropyTableNoCompress) {
    impl = new Impl(freq, blockUnits, fpath, offset,
            checksumLevel, checksumType,
            entropyTableNoCompress? (0x00 | NOCOMPRESS_FLAG):0x00);
}
EntropyZipBlobStore::MyBuilder::MyBuilder(freq_hist_o1& freq, size_t blockUnits, FileMemIO& mem,
                                          int checksumLevel, int checksumType, bool entropyTableNoCompress) {
    impl = new Impl(freq, blockUnits, mem, checksumLevel, checksumType,
            entropyTableNoCompress? (0x00 | NOCOMPRESS_FLAG):0x00);
}
void EntropyZipBlobStore::MyBuilder::addRecord(fstring rec) {
    assert(NULL != impl);
    impl->add_record(rec);
}
void EntropyZipBlobStore::MyBuilder::finish() {
    assert(NULL != impl);
    return impl->finish();
}


} // namespace terark
