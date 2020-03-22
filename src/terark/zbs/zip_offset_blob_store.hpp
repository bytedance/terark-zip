/*
 * zip_offset_blob_store.hpp
 *
 *  Created on: 2017-06-01
 *      Author: leipeng
 */

#ifndef ZBS_ZO_PLAIN_BLOB_STORE_HPP_
#define ZBS_ZO_PLAIN_BLOB_STORE_HPP_
#pragma once

#include "abstract_blob_store.hpp"
#include <terark/io/FileMemStream.hpp>
#include <terark/util/sorted_uint_vec.hpp>

namespace terark {

// Object layout is same to PlainBlobStore, but use SortedUintVec as offset
class TERARK_DLL_EXPORT ZipOffsetBlobStore : public AbstractBlobStore {
    struct FileHeader; friend struct FileHeader;
    int m_compressLevel;
    valvec<byte_t> m_content;
    SortedUintVec  m_offsets;

    void get_record_append_imp(size_t recID, valvec<byte_t>* recData) const;
    void get_record_append_CacheOffsets(size_t recID, CacheOffsets*) const;
    void fspread_record_append_imp(pread_func_t fspread, void* lambda,
                                   size_t baseOffset, size_t recID,
                                   valvec<byte_t>* recData,
                                   valvec<byte_t>* rdbuf) const;
public:
    ZipOffsetBlobStore();
    ~ZipOffsetBlobStore();

    struct Options {
      Options() : block_units(128), compress_level(0), checksum_level(3), checksum_type(0) {}
      int block_units;
      int compress_level;
      int checksum_level;
      int checksum_type;
    };

    void swap(ZipOffsetBlobStore& other);

    void init_from_memory(fstring dataMem, Dictionary dict) override;
    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
    void detach_meta_blocks(const valvec<fstring>& blocks) override;
    void save_mmap(function<void(const void*, size_t)> write) const override;
    using AbstractBlobStore::save_mmap;

    size_t mem_size() const override;
    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;

    struct TERARK_DLL_EXPORT MyBuilder : public AbstractBlobStore::Builder {
        class TERARK_DLL_EXPORT Impl; Impl* impl;
    public:
        MyBuilder(fstring fpath, size_t offset = 0, Options options = Options());
        MyBuilder(FileMemIO& mem, Options options = Options());
        virtual ~MyBuilder();
        void addRecord(fstring rec) override;
        void finish() override;
    };
};

} // namespace terark

#endif /* ZBS_ZO_PLAIN_BLOB_STORE_HPP_ */
