/*
 * entropy_zip_blob_store.hpp
 *
 *  Created on: 2019-05-28
 *      Author: zhaoming
 */

#ifndef ZBS_ENTROPY_ZIP_BLOB_STORE_HPP_
#define ZBS_ENTROPY_ZIP_BLOB_STORE_HPP_
#pragma once

#include "abstract_blob_store.hpp"
#include <terark/entropy/huffman_encoding.hpp>
#include <terark/io/FileMemStream.hpp>
#include <terark/util/sorted_uint_vec.hpp>

namespace terark {

class TERARK_DLL_EXPORT EntropyZipBlobStore : public AbstractBlobStore {
    struct FileHeader; friend struct FileHeader;
    valvec<byte_t> m_content;
    SortedUintVec  m_offsets;
    valvec<byte_t> m_table;
    const Huffman::decoder* m_decoder_o0;
    const Huffman::decoder_o1* m_decoder_o1;

    template<size_t Order>
    void get_record_append_imp(size_t recID, valvec<byte_t>* recData) const;
    template<size_t Order>
    void get_record_append_CacheOffsets(size_t recID, CacheOffsets*) const;
    template<size_t Order>
    void fspread_record_append_imp(pread_func_t fspread, void* lambda,
                                   size_t baseOffset, size_t recID,
                                   valvec<byte_t>* recData,
                                   valvec<byte_t>* rdbuf) const;
public:
    EntropyZipBlobStore();
    ~EntropyZipBlobStore();

    bool is_entropy_table_compress() const;
    bool is_order1() const;

    void swap(EntropyZipBlobStore& other);
    void init_get_calls();

    void init_from_memory(fstring dataMem, Dictionary dict) override;
    void init_from_components(SortedUintVec&& offset, valvec<byte_t>&& data,
                              valvec<byte_t>&& table, uint64_t raw_size);
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
        MyBuilder(freq_hist_o1& freq, size_t blockUnits, fstring fpath, size_t offset = 0,
                  int checksumLevel = 3, int checksumType = 0, bool entropyTableCompress = false);
        MyBuilder(freq_hist_o1& freq, size_t blockUnits, FileMemIO& mem,
                  int checksumLevel = 3, int checksumType = 0, bool entropyTableCompress = false);
        virtual ~MyBuilder();
        void addRecord(fstring rec) override;
        void finish() override;
    };
};

} // namespace terark

#endif /* ZBS_ENTROPY_ZIP_BLOB_STORE_HPP_ */
