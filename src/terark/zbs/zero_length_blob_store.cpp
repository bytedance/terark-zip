/*
 * plain_blob_store.cpp
 *
 *  Created on: 2017-02-10
 *      Author: leipeng
 */

#include "zero_length_blob_store.hpp"
#include "blob_store_file_header.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/util/checksum_exception.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace terark {

REGISTER_BlobStore(ZeroLengthBlobStore, "ZeroLengthBlobStore");

void ZeroLengthBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
    m_mmapBase = (FileHeaderBase*)dataMem.p;
    m_numRecords = m_mmapBase->records;
}

void ZeroLengthBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
}

void ZeroLengthBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
}

void ZeroLengthBlobStore::save_mmap(function<void(const void*, size_t)> write) const {
    FileHeaderBase header;
    memset(&header, 0, sizeof header);
    header.magic_len = MagicStrLen;
    strcpy(header.magic, MagicString);
    strcpy(header.className, "ZeroLengthBlobStore");
    header.fileSize = sizeof(FileHeaderBase);
    header.records = m_numRecords;

    write(&header, sizeof header);
}

ZeroLengthBlobStore::ZeroLengthBlobStore() {
    m_get_record_append = static_cast<get_record_append_func_t>
               (&ZeroLengthBlobStore::get_record_append_imp);
    // binary compatible:
    m_get_record_append_CacheOffsets =
        reinterpret_cast<get_record_append_CacheOffsets_func_t>
        (m_get_record_append);
    m_fspread_record_append = static_cast<fspread_record_append_func_t>
               (&ZeroLengthBlobStore::fspread_record_append_imp);
}

ZeroLengthBlobStore::~ZeroLengthBlobStore() {
    if (m_mmapBase) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
    }
}

void ZeroLengthBlobStore::finish(size_t records) {
    m_numRecords = records;
}

size_t ZeroLengthBlobStore::mem_size() const {
    return 0;
}

void
ZeroLengthBlobStore::get_record_append_imp(size_t recID, valvec<byte_t>* recData)
const {
    assert(recID < m_numRecords);
}

void
ZeroLengthBlobStore::fspread_record_append_imp(
    pread_func_t fspread, void* lambda,
    size_t baseOffset, size_t recID,
    valvec<byte_t>* recData,
    valvec<byte_t>* rdbuf)
const {
    assert(recID < m_numRecords);
}

void ZeroLengthBlobStore::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {
    save_mmap(writeAppend);
}

} // namespace terark
