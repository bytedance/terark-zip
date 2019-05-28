#pragma once
#include <string>
#include <terark/int_vector.hpp>
#include <terark/zbs/abstract_blob_store.hpp>

namespace terark {

class TERARK_DLL_EXPORT SimpleZipBlobStore : public AbstractBlobStore {
	struct FileHeader;
	valvec<byte>           m_strpool;
	ZipIntVector<uint64_t> m_off_len;
	UintVecMin0            m_records;
	size_t      m_lenBits;

protected:
	void init_from_memory(fstring dataMem, Dictionary dict) override;
public:
	SimpleZipBlobStore();
	~SimpleZipBlobStore();

    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
    void detach_meta_blocks(const valvec<fstring>& blocks) override;
	void build_from(class SortableStrVec& strVec, const class NestLoudsTrieConfig&);
	void load_mmap(fstring fpath, const void* mmapBase, size_t mmapSize);
    void save_mmap(function<void(const void*, size_t)> write) const override;
	using AbstractBlobStore::save_mmap;

	size_t mem_size() const override;

	void get_record_append_imp(size_t recId, valvec<byte_t>* recData) const;

	fstring get_mmap() const override;
    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;
};

} // namespace terark
