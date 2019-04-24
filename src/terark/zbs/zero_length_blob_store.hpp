/*
 * plain_blob_store.hpp
 *
 *  Created on: 2017Äê2ÔÂ10ÈÕ
 *      Author: leipeng
 */

#ifndef ZBS_ZERO_LENGTH_BLOB_STORE_HPP_
#define ZBS_ZERO_LENGTH_BLOB_STORE_HPP_
#pragma once

#include "abstract_blob_store.hpp"
#include <terark/int_vector.hpp>
#include <terark/util/fstrvec.hpp>

namespace terark {

class TERARK_DLL_EXPORT ZeroLengthBlobStore : public AbstractBlobStore {
public:
    void init_from_memory(fstring dataMem, Dictionary dict) override;
    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
    void save_mmap(function<void(const void*, size_t)> write) const override;
    using AbstractBlobStore::save_mmap;
    using AbstractBlobStore::m_numRecords;

    ZeroLengthBlobStore();
    ~ZeroLengthBlobStore();

    void finish(size_t records);

    size_t mem_size() const override;
    void get_record_append_imp(size_t recID, valvec<byte_t>* recData) const;
    void fspread_record_append_imp(
        pread_func_t fspread, void* lambda,
        size_t baseOffset, size_t recID,
        valvec<byte_t>* recData,
        valvec<byte_t>* rdbuf) const;
    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;
};

} // namespace terark

#endif /* ZBS_ZERO_LENGTH_BLOB_STORE_HPP_ */
