/*
 * plain_blob_store.cpp
 *
 *  Created on: 2017-02-10
 *      Author: leipeng
 */

#include "plain_blob_store.hpp"
#include "blob_store_file_header.hpp"
#include "zip_reorder_map.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/util/checksum_exception.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace terark {

REGISTER_BlobStore(PlainBlobStore, "PlainBlobStore");

static const uint64_t g_dpbsnark_seed = 0x5342426e69616c50ull; // echo PlainBBS | od -t x8

struct PlainBlobStore::FileHeader : public FileHeaderBase {
    uint64_t  contentBytes;
    uint64_t  offsetsBytes;
    uint08_t  offsetsUintBits;
    uint08_t  padding21[7];
    uint64_t  padding22[3];

    void init();
    FileHeader(fstring mem, size_t content_size, size_t offsets_size);
    FileHeader(const PlainBlobStore* store);
};


void PlainBlobStore::FileHeader::init() {
    BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
    memset(this, 0, sizeof(*this));
    magic_len = MagicStrLen;
    strcpy(magic, MagicString);
    strcpy(className, "PlainBlobStore");
}

PlainBlobStore::FileHeader::FileHeader(fstring mem, size_t content_size, size_t num_records) {
    init();
    fileSize = mem.size();
    size_t uintbits = UintVecMin0::compute_uintbits(content_size);
    assert(fileSize == 0
        + sizeof(FileHeader)
        + align_up(content_size, 16)
        + UintVecMin0::compute_mem_size_by_max_val(content_size, num_records + 1)
        + sizeof(BlobStoreFileFooter));
    UintVecMin0 offsets;
    offsets.risk_set_data((byte_t*)mem.data() + sizeof(FileHeader)
        + align_up(content_size, 16), num_records + 1, uintbits);
    records = num_records;
    contentBytes = content_size;
    offsetsBytes = offsets.mem_size();
    offsetsUintBits = offsets.uintbits();
    offsets.risk_release_ownership();
}

PlainBlobStore::FileHeader::FileHeader(const PlainBlobStore* store) {
    init();
    fileSize = 0
        + sizeof(FileHeader)
        + align_up(store->m_content.size(), 16)
        + store->m_offsets.mem_size()
        + sizeof(BlobStoreFileFooter);
    records = store->m_numRecords;
    contentBytes = store->m_content.size();
    offsetsBytes = store->m_offsets.mem_size();
    offsetsUintBits = store->m_offsets.uintbits();
}

void PlainBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
    auto mmapBase = (FileHeader*)dataMem.p;
    m_mmapBase = mmapBase;
    if (isChecksumVerifyEnabled()) {
        XXHash64 hash(g_dpbsnark_seed);
        hash.update(mmapBase, mmapBase->fileSize - sizeof(BlobStoreFileFooter));
        const uint64_t hashVal = hash.digest();
        auto& footer = ((const BlobStoreFileFooter*)((const byte_t*)(mmapBase) + mmapBase->fileSize))[-1];
        if (hashVal != footer.fileXXHash) {
            std::string msg = "PlainBlobStore::load_mmap(\"" + m_fpath + "\")";
            throw BadChecksumException(msg, footer.fileXXHash, hashVal);
        }
    }
    assert(mmapBase->offsetsUintBits == UintVecMin0::compute_uintbits(mmapBase->contentBytes));
    m_numRecords = mmapBase->records;
    m_unzipSize = mmapBase->contentBytes;
    m_content.risk_set_data((byte_t*)(mmapBase + 1), mmapBase->contentBytes);
    m_offsets.risk_set_data(m_content.data() + align_up(m_content.size(), 16),
        m_numRecords + 1, mmapBase->offsetsUintBits);
    assert(m_offsets.mem_size() == mmapBase->offsetsBytes);
}

void PlainBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_offsets.data(), m_offsets.mem_size());
}

void PlainBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_content);
}

void PlainBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    assert(blocks.size() == 1);
    auto offset_mem = blocks.front();
    assert(offset_mem.size() == m_offsets.mem_size());
    if (m_mmapBase) {
        m_offsets.risk_release_ownership();
    } else {
        m_offsets.clear();
    }
    m_offsets.risk_set_data((byte_t*)offset_mem.data() + align_up(m_content.size(), 16),
        m_numRecords + 1, ((const FileHeader*)m_mmapBase)->offsetsUintBits);
    m_isDetachMeta = true;
}

void PlainBlobStore::save_mmap(function<void(const void*, size_t)> write) const {
    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);

    assert(m_offsets.mem_size() % 16 == 0);
    XXHash64 xxhash64(g_dpbsnark_seed);
    FileHeader header(this);

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

PlainBlobStore::PlainBlobStore() {
    m_checksumLevel = 3;
    m_get_record_append = static_cast<get_record_append_func_t>
                    (&PlainBlobStore::get_record_append_imp);
    m_fspread_record_append = static_cast<fspread_record_append_func_t>
                    (&PlainBlobStore::fspread_record_append_imp);
    // binary compatible:
    m_get_record_append_CacheOffsets =
        reinterpret_cast<get_record_append_CacheOffsets_func_t>
        (m_get_record_append);
}

PlainBlobStore::~PlainBlobStore() {
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
    }
    else {
        m_content.clear();
        m_offsets.clear();
    }
}

void PlainBlobStore::take(fstrvec& vec) {
    m_offsets.build_from(vec.offsets);
#ifdef __GNUC__
#   pragma GCC diagnostic push
#   pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
    m_content.swap((valvec<byte_t>&)vec.strpool);
#ifdef __GNUC__
#   pragma GCC diagnostic pop
#endif
    fstrvec().swap(vec);
}

size_t PlainBlobStore::mem_size() const {
    return m_content.size() + m_offsets.mem_size();
}

void
PlainBlobStore::get_record_append_imp(size_t recID, valvec<byte_t>* recData)
const {
    assert(recID + 1 < m_offsets.size());
    size_t BegEnd[2];
    m_offsets.get2(recID, BegEnd);
    assert(BegEnd[0] <= BegEnd[1]);
    assert(BegEnd[1] <= m_content.size());
    size_t len = BegEnd[1] - BegEnd[0];
    recData->append(m_content.data() + BegEnd[0], len);
}

void
PlainBlobStore::fspread_record_append_imp(pread_func_t fspread, void* lambda,
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
    recData->append(pData, len);
}

void PlainBlobStore::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {
    FunctionAdaptBuffer adaptBuffer(writeAppend);
    OutputBuffer buffer(&adaptBuffer);

    size_t recNum = m_numRecords;
    size_t offset = 0;
    size_t maxOffsetEnt = m_offsets[recNum];
    XXHash64 xxhash64(g_dpbsnark_seed);

    FileHeader header(this);
    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof header);

    for (assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
        size_t oldId = *newToOld;
        assert(oldId < recNum);
        size_t offsetBeg = m_offsets[oldId + 0];
        size_t offsetEnd = m_offsets[oldId + 1];
        size_t len = offsetEnd - offsetBeg;
        assert(offsetBeg <= offsetEnd);
        offset += len;
        const  byte* beg = m_content.data() + offsetBeg;
        xxhash64.update(beg, len);
        buffer.ensureWrite(beg, len);
    }
    PadzeroForAlign<16>(buffer, xxhash64, offset);
    assert(offset == maxOffsetEnt);

    static const size_t offset_flush_size = 128;
    UintVecMin0 newOffsets(offset_flush_size, maxOffsetEnt);
    size_t flush_count = 0;
    offset = 0;
    auto flush_offset = [&] {
        size_t byte_count = newOffsets.uintbits() * offset_flush_size / 8;
        xxhash64.update(newOffsets.data(), byte_count);
        buffer.ensureWrite(newOffsets.data(), byte_count);
        flush_count += offset_flush_size;
    };
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        size_t newId = newToOld.index() - flush_count;
        size_t oldId = *newToOld;
        if (newId == offset_flush_size) {
            flush_offset();
            newId = 0;
        }
        size_t offsetBeg = m_offsets[oldId + 0];
        size_t offsetEnd = m_offsets[oldId + 1];
        newOffsets.set_wire(newId, offset);
        offset += offsetEnd - offsetBeg;
    }
    assert(offset == maxOffsetEnt);
    if (recNum - flush_count == offset_flush_size) {
        flush_offset();
    }
    newOffsets.set_wire(recNum - flush_count, offset);
    newOffsets.resize(recNum - flush_count + 1);
    newOffsets.shrink_to_fit();
    xxhash64.update(newOffsets.data(), newOffsets.mem_size());
    buffer.ensureWrite(newOffsets.data(), newOffsets.mem_size());

    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

///////////////////////////////////////////////////////////////////////////
class PlainBlobStore::MyBuilder::Impl : boost::noncopyable {
    std::string m_fpath;
    std::string m_fpath_offset;
    FileStream m_file;
    NativeDataOutput<OutputBuffer> m_writer;
    std::unique_ptr<UintVecMin0::Builder> m_offset_builder;
    size_t m_offset;
    size_t m_num_records;
    size_t m_content_size;
    size_t m_content_input_size;

    static const size_t offset_flush_size = 128;
public:
    Impl(size_t contentSize, fstring fpath, size_t offset)
        : m_fpath(fpath.begin(), fpath.end())
        , m_fpath_offset(fpath + ".offset")
        , m_file()
        , m_writer(&m_file)
        , m_offset_builder(UintVecMin0::create_builder_by_max_value(contentSize, m_fpath_offset.c_str()))
        , m_offset(offset)
        , m_num_records(0)
        , m_content_size(0)
        , m_content_input_size(contentSize) {
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
    ~Impl() {
        if (!m_fpath_offset.empty()) {
            ::remove(m_fpath_offset.c_str());
        }
    }
    void add_record(fstring rec) {
        m_writer.ensureWrite(rec.data(), rec.size());
        m_offset_builder->push_back(m_content_size);
        m_content_size += rec.size();
        ++m_num_records;
    }
    void finish() {
        assert(m_content_size <= m_content_input_size);
        (void)m_content_input_size;
        PadzeroForAlign<16>(m_writer, m_content_size);
        m_writer.flush_buffer();

        m_offset_builder->push_back(m_content_size);
        auto build_result = m_offset_builder->finish();
        m_offset_builder.reset();
        assert(build_result.mem_size % 8 == 0);
        m_file.cat(m_fpath_offset);
        ::remove(m_fpath_offset.c_str());
        m_fpath_offset.clear();

        m_file.close();

        size_t file_size = m_offset
            + sizeof(FileHeader)
            + align_up(m_content_size, 16)
            + build_result.mem_size
            + sizeof(BlobStoreFileFooter);
        assert(FileStream(m_fpath, "rb+").fsize() == file_size - sizeof(BlobStoreFileFooter));
        FileStream(m_fpath, "rb+").chsize(file_size);
        MmapWholeFile mmap(m_fpath, true);
        fstring mem((const char*)mmap.base + m_offset, (ptrdiff_t)(file_size - m_offset));
        *(FileHeader*)((byte_t*)mmap.base + m_offset) = FileHeader(mem, m_content_size, m_num_records);

        XXHash64 xxhash64(g_dpbsnark_seed);
        xxhash64.update(mem.data(), mem.size() - sizeof(BlobStoreFileFooter));

        BlobStoreFileFooter footer;
        footer.fileXXHash = xxhash64.digest();
        ((BlobStoreFileFooter*)(mem.data() + mem.size()))[-1] = footer;
    }
};

PlainBlobStore::MyBuilder::~MyBuilder() {
    delete impl;
}
PlainBlobStore::MyBuilder::MyBuilder(size_t blockUnits, fstring fpath, size_t offset) {
    impl = new Impl(blockUnits, fpath, offset);
}
void PlainBlobStore::MyBuilder::addRecord(fstring rec) {
    assert(NULL != impl);
    impl->add_record(rec);
}
void PlainBlobStore::MyBuilder::finish() {
    assert(NULL != impl);
    return impl->finish();
}

} // namespace terark
