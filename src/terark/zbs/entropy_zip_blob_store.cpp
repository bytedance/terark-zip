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
#include <terark/util/checksum_exception.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/int_vector.hpp>

namespace terark {

REGISTER_BlobStore(EntropyZipBlobStore, "EntropyZipBlobStore");

static const uint64_t g_debsnark_seed = 0x5342425f5a617445ull; // echo EtaZ_BBS | od -t x8

struct EntropyZipBlobStore::FileHeader : public FileHeaderBase {
    uint64_t  contentBytes;
    uint64_t  offsetsBytes; // same as footer.indexBytes
    uint08_t  offsets_log2_blockUnits; // 6 or 7
    uint08_t  padding21[7];
    uint64_t  tableBytes;
    uint64_t  padding22[2];

    void init() {
        BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
        memset(this, 0, sizeof(*this));
        magic_len = MagicStrLen;
        strcpy(magic, MagicString);
        strcpy(className, "EntropyZipBlobStore");
    }
    FileHeader(fstring mem, size_t content_size, size_t offsets_size, size_t table_size) {
        init();
        fileSize = mem.size();
        assert(fileSize == 0
            + sizeof(FileHeader)
            + align_up(content_size, 16)
            + offsets_size
            + align_up(table_size, 16)
            + sizeof(BlobStoreFileFooter));
        SortedUintVec offsets;
        offsets.risk_set_data(mem.data() + sizeof(FileHeader) + align_up(content_size, 16), offsets_size);
        unzipSize = content_size;
        records = offsets.size() - 1;
        contentBytes = content_size;
        offsetsBytes = offsets_size;
        offsets_log2_blockUnits = offsets.log2_block_units();
        offsets.risk_release_ownership();
        tableBytes = table_size;
    }
    FileHeader(const EntropyZipBlobStore* store, const SortedUintVec& offsets, size_t table_size) {
        init();
        fileSize = 0
            + sizeof(FileHeader)
            + align_up(store->m_content.size(), 16)
            + offsets.mem_size()
            + align_up(table_size, 16)
            + sizeof(BlobStoreFileFooter);
        unzipSize = store->m_content.size();
        records = store->m_numRecords;
        contentBytes = store->m_content.size();
        offsetsBytes = offsets.mem_size();
        offsets_log2_blockUnits = offsets.log2_block_units();
        tableBytes = table_size;
    }
};

void EntropyZipBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
    auto mmapBase = (const FileHeader*)dataMem.p;
    m_mmapBase = mmapBase;
    if (isChecksumVerifyEnabled()) {
        XXHash64 hash(g_debsnark_seed);
        hash.update(mmapBase, mmapBase->fileSize - sizeof(BlobStoreFileFooter));
        const uint64_t hashVal = hash.digest();
        auto &footer = ((const BlobStoreFileFooter*)((const byte_t*)(mmapBase) + mmapBase->fileSize))[-1];
        if (hashVal != footer.fileXXHash) {
            std::string msg = "EntropyZipBlobStore::load_mmap(\"" + m_fpath + "\")";
            throw BadChecksumException(msg, footer.fileXXHash, hashVal);
        }
    }
    m_numRecords = mmapBase->records;
    m_unzipSize = mmapBase->contentBytes;
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
    m_table.risk_set_data((byte_t*)m_offsets.data() + m_offsets.mem_size(), mmapBase->tableBytes);
    size_t table_size;
    m_decoder.reset(new Huffman::decoder_o1(m_table, &table_size));
    assert(table_size == mmapBase->tableBytes);
}

void EntropyZipBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_offsets.data(), m_offsets.mem_size());
}

void EntropyZipBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_content);
}

void EntropyZipBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    assert(blocks.size() == 1);
    auto offset_mem = blocks.front();
    assert(offset_mem.size() == m_offsets.mem_size());
    if (m_mmapBase) {
        m_offsets.risk_release_ownership();
    } else {
        m_offsets.clear();
    }
    m_offsets.risk_set_data((byte_t*)offset_mem.data(), offset_mem.size());
    m_isDetachMeta = true;
}

void EntropyZipBlobStore::save_mmap(function<void(const void*, size_t)> write) const {
    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);

    assert(m_offsets.mem_size() % 16 == 0);
    XXHash64 xxhash64(g_debsnark_seed);
    FileHeader header(this, m_offsets, m_table.size());

    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof(header));

    xxhash64.update(m_content.data(), m_content.size());
    buffer.ensureWrite(m_content.data(), m_content.size());

    PadzeroForAlign<16>(buffer, xxhash64, m_content.size());

    xxhash64.update(m_offsets.data(), m_offsets.mem_size());
    buffer.ensureWrite(m_offsets.data(), m_offsets.mem_size());

    xxhash64.update(m_table.data(), m_table.size());
    buffer.ensureWrite(m_table.data(), m_table.size());

    PadzeroForAlign<16>(buffer, xxhash64, m_table.size());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

EntropyZipBlobStore::EntropyZipBlobStore() {
    m_checksumLevel = 3; // check all data
    m_get_record_append = static_cast<get_record_append_func_t>
                (&EntropyZipBlobStore::get_record_append_imp);
    m_fspread_record_append = static_cast<fspread_record_append_func_t>
                (&EntropyZipBlobStore::fspread_record_append_imp);
    m_get_record_append_CacheOffsets =
        static_cast<get_record_append_CacheOffsets_func_t>
        (&EntropyZipBlobStore::get_record_append_CacheOffsets);
}

EntropyZipBlobStore::~EntropyZipBlobStore() {
    if (m_isDetachMeta) {
        m_offsets.risk_release_ownership();
    }
    if (m_mmapBase) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
        m_content.risk_release_ownership();
        m_offsets.risk_release_ownership();
        m_table.risk_release_ownership();
    }
    else {
        m_content.clear();
        m_offsets.clear();
        m_table.clear();
    }
    m_decoder.reset();
}


void EntropyZipBlobStore::swap(EntropyZipBlobStore& other) {
  AbstractBlobStore::risk_swap(other);
  m_content.swap(other.m_content);
  m_offsets.swap(other.m_offsets);
  m_table  .swap(other.m_table  );
  m_decoder.swap(other.m_decoder);
}

size_t EntropyZipBlobStore::mem_size() const {
    return m_content.size() + m_offsets.mem_size() + m_table.size();
}

void
EntropyZipBlobStore::get_record_append_imp(size_t recID, valvec<byte_t>* recData)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    auto ctx = GetTlsEntropyContext();
    EntropyBits bits = {
        (byte_t*)m_content.data(), BegEnd[0], BegEnd[1] - BegEnd[0]
    };
    bool ok = m_decoder->bitwise_decode_x1(bits, &ctx->data, ctx);
    assert(ok); (void)ok;
    recData->append(ctx->data);
}

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
    auto ctx = GetTlsEntropyContext();
    EntropyBits bits = {
        (byte_t*)m_content.data(), BegEnd[0], BegEnd[1] - BegEnd[0]
    };
    bool ok = m_decoder->bitwise_decode_x1(bits, &ctx->data, ctx);
    assert(ok); (void)ok;
    co->recData.append(ctx->data);
}

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
    assert(BegEnd[1] <= m_content.size());
    size_t byte_beg = (BegEnd[0] - BegEnd[0] % 64) / 8;
    size_t byte_end = (BegEnd[1] + 63) / 64 * 8;
    size_t offset = sizeof(FileHeader) + byte_beg;
    auto pData = fspread(lambda, baseOffset + offset, byte_end - byte_beg, rdbuf);
    assert(NULL != pData);
    auto ctx = GetTlsEntropyContext();
    EntropyBits bits = {
        (byte_t*)pData, BegEnd[0] - byte_beg * 8, BegEnd[1] - BegEnd[0]
    };
    bool ok = m_decoder->bitwise_decode_x1(bits, &ctx->data, ctx);
    assert(ok); (void)ok;
    recData->append(ctx->data);
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
    FileHeader header(this, newZipOffsets, m_table.size());
    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof header);
    auto output_data = [&buffer, &xxhash64](const void* data, size_t size) {
        xxhash64.update(data, size);
        buffer.ensureWrite(data, size);
    };
    EntropyBitsWriter<decltype(output_data)> writer(output_data);
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
        size_t oldId = *newToOld;
        size_t BegEnd[2];
        m_offsets.get2(oldId, BegEnd);
        EntropyBits bits = {(byte_t*)m_content.data(), BegEnd[0], BegEnd[1] - BegEnd[0]};
        writer.write(bits);
    }
    writer.finish();
    PadzeroForAlign<16>(buffer, xxhash64, offset);

    xxhash64.update(newZipOffsets.data(), newZipOffsets.mem_size());
    buffer.ensureWrite(newZipOffsets.data(), newZipOffsets.mem_size());

    MmapWholeFile().swap(mmapOffset);
    ::remove(tmpFile.c_str());

    xxhash64.update(m_table.data(), m_table.size());
    buffer.ensureWrite(m_table.data(), m_table.size());
    PadzeroForAlign<16>(buffer, xxhash64, m_table.size());

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
    std::unique_ptr<Huffman::encoder_o1> m_encoder;
    std::function<void(const void*, size_t)> m_output;
    EntropyBitsWriter<std::function<void(const void*, size_t)>> m_bitWriter;
    EntropyContext m_ctx;
    size_t m_offset;
    size_t m_content_size;
public:
    Impl(freq_hist_o1& freq, size_t blockUnits, fstring fpath, size_t offset)
        : m_fpath(fpath.begin(), fpath.end())
        , m_fpath_offset(fpath + ".offset")
        , m_builder(SortedUintVec::createBuilder(blockUnits, m_fpath_offset.c_str()))
        , m_file()
        , m_memStream(nullptr)
        , m_writer(&m_file)
        , m_bitWriter(m_output)
        , m_offset(offset)
        , m_content_size(0) {
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
    Impl(freq_hist_o1& freq, size_t blockUnits, FileMemIO& mem)
        : m_fpath()
        , m_fpath_offset()
        , m_builder(SortedUintVec::createBuilder(blockUnits))
        , m_file()
        , m_memStream(&mem)
        , m_writer(&m_memStream)
        , m_bitWriter(m_output)
        , m_offset(0)
        , m_content_size(0) {
        init(freq);
    }
    void init(freq_hist_o1& freq) {
        freq.normalise(Huffman::NORMALISE);
        m_encoder.reset(new Huffman::encoder_o1(freq.histogram()));
        m_output = [this](const void* d, size_t s) {
            m_writer.ensureWrite(d, s);
        };
        std::aligned_storage<sizeof(FileHeader)>::type header;
        memset(&header, 0, sizeof header);
        m_writer.ensureWrite(&header, sizeof header);
    }
    void add_record(fstring rec) {
        auto bits = m_encoder->bitwise_encode_x1(rec, &m_ctx);
        m_bitWriter.write(bits);
        m_builder->push_back(m_content_size);
        m_content_size += bits.size;
    }
    void finish() {
        auto bits = m_bitWriter.finish();
        PadzeroForAlign<16>(m_writer, m_content_size);
        assert(bits.size == m_content_size);
        m_builder->push_back(m_content_size);
        auto& table = m_encoder->table();
        if (m_file.fp() == nullptr) {
            SortedUintVec vec;
            m_builder->finish(&vec);
            m_builder.reset();

            size_t offsets_size = vec.mem_size();
            m_writer.ensureWrite(vec.data(), offsets_size);
            m_writer.ensureWrite(table.data(), table.size());
            PadzeroForAlign<16>(m_writer, table.size());
            m_writer.flush_buffer();

            size_t file_size = m_offset
                + sizeof(FileHeader)
                + align_up(m_content_size, 16)
                + offsets_size
                + align_up(table.size(), 16)
                + sizeof(BlobStoreFileFooter);

            assert(m_memStream.size() == file_size - sizeof(BlobStoreFileFooter));
            m_memStream.stream()->resize(file_size);
            *(FileHeader*)m_memStream.stream()->begin() =
                FileHeader(fstring(m_memStream.stream()->begin(), m_memStream.size()),
                    m_content_size, offsets_size, table.size());

            XXHash64 xxhash64(g_debsnark_seed);
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
                + align_up(table.size(), 16)
                + sizeof(BlobStoreFileFooter);
            assert(FileStream(m_fpath, "rb+").fsize() == file_size - sizeof(BlobStoreFileFooter));
            FileStream(m_fpath, "rb+").chsize(file_size);
            MmapWholeFile mmap(m_fpath, true);
            fstring mem((const char*)mmap.base + m_offset, (ptrdiff_t)(file_size - m_offset));
            *(FileHeader*)mem.data() = FileHeader(mem, m_content_size, offsets_size, table.size());

            size_t table_offset = m_offset + sizeof(FileHeader) + align_up(m_content_size, 16) + offsets_size;
            memcpy((byte_t*)mem.data() + table_offset, table.data(), table.size());
            if (table.size() % 16 != 0) {
                memset((byte_t*)mem.data() + table_offset + table.size(), 0, 16 - table.size() % 16);
            }

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
EntropyZipBlobStore::MyBuilder::MyBuilder(freq_hist_o1& freq, size_t blockUnits, fstring fpath, size_t offset) {
    impl = new Impl(freq, blockUnits, fpath, offset);
}
EntropyZipBlobStore::MyBuilder::MyBuilder(freq_hist_o1& freq, size_t blockUnits, FileMemIO& mem) {
    impl = new Impl(freq, blockUnits, mem);
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
