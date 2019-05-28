#include "blob_store_file_header.hpp"
#include "simple_zip_blob_store.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/mmap.hpp>
#include <terark/fsa/nest_louds_trie.hpp>
#include <assert.h>
#include <algorithm>

#if defined(_WIN32) || defined(_WIN64)
	#define WIN32_LEAN_AND_MEAN
	#define NOMINMAX
	#include <Windows.h>
#else
	#include <unistd.h> // for usleep
#endif

namespace terark {

SimpleZipBlobStore::SimpleZipBlobStore() {
	m_mmapBase = nullptr;
	m_unzipSize = 0;
	m_lenBits = 0;
    m_get_record_append = static_cast<get_record_append_func_t>
                (&SimpleZipBlobStore::get_record_append_imp);
    // binary compatible:
    m_get_record_append_CacheOffsets =
        reinterpret_cast<get_record_append_CacheOffsets_func_t>
        (m_get_record_append);

}

SimpleZipBlobStore::~SimpleZipBlobStore() {
	if (m_mmapBase) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
		m_strpool.risk_release_ownership();
		m_off_len.risk_release_ownership();
		m_records.risk_release_ownership();
	}
}

void
SimpleZipBlobStore::get_record_append_imp(size_t recId, valvec<byte_t>* recData)
const {
	assert(recId + 1 < m_records.size());
	size_t beg = m_records.get(recId + 0);
	size_t end = m_records.get(recId + 1);
	auto strpool = m_strpool.data();
	auto ol_data = m_off_len.data();
	auto ol_bits = m_off_len.uintbits();
	auto ol_mask = m_off_len.uintmask();
	auto ol_minVal = m_off_len.min_val();
	auto lenBits = m_lenBits;
	auto lenMask = (size_t(1) << m_lenBits) - 1;
	for (size_t i = beg; i < end; ++i) {
		uint64_t off_len = m_off_len.fast_get(ol_data, ol_bits, ol_mask, ol_minVal, i);
		size_t offset = size_t(off_len >> lenBits);
		size_t length = size_t(off_len &  lenMask);
		recData->append(strpool + offset, length);
	}
}

void
SimpleZipBlobStore::build_from(SortableStrVec& strVec, const NestLoudsTrieConfig& conf) {
	m_unzipSize = strVec.str_size();
	valvec<size_t> records(strVec.size(), valvec_reserve());
	valvec<SortableStrVec::SEntry> subStrNode;
	auto strbase = strVec.m_strpool.data();
	records.push_back(0);
	size_t maxLen = 0;
	size_t minFragLen = std::max(conf.minFragLen, 1);
	size_t maxFragLen = conf.maxFragLen;
	for (size_t i = 0; i < strVec.size(); ++i) {
		fstring row = strVec[i];
		for(auto p = row.udata(), end = p + row.size(); p < end; ) {
			auto t = std::min(p + maxFragLen, end);
			auto q = std::min(p + minFragLen, end);
			while (q < t && !conf.bestDelimBits[*q]) ++q;
			SortableStrVec::SEntry ent;
			ent.seq_id = subStrNode.size();
			ent.offset = p - strbase;
			ent.length = q - p;
			maxLen = std::max(size_t(q - p), maxLen);
			subStrNode.push_back(ent);
			p = q;
		}
		records.push_back(subStrNode.size());
	}
	strVec.build_subkeys(subStrNode);
	strVec.compress_strpool(1);
	strVec.sort_by_seq_id();
	int lenBits = terark_bsr_u32(uint32_t(maxLen)) + 1;
	valvec<uint64_t> off_len(strVec.size(), valvec_no_init());
	auto ents = strVec.m_index.data();
	for (size_t i = 0; i < strVec.size(); ++i) {
		off_len[i] = ents[i].offset << lenBits | ents[i].length;
	}
	m_off_len.build_from(off_len);
	m_records.build_from(records);
	m_strpool.swap(strVec.m_strpool);
	m_lenBits = lenBits;
	m_numRecords = records.size() - 1;
}

#pragma pack(push,8)
struct SimpleZipBlobStore::FileHeader : public FileHeaderBase {
	uint64_t pading21;
	uint64_t off_len_number;
	uint64_t off_len_minVal;
	uint32_t strpoolsize; // div 16
	uint32_t pading22;
	uint08_t lenBits;
	uint08_t offBits;
	uint08_t pad1[2];
	uint32_t pad2[3];
};
#pragma pack(pop)

void SimpleZipBlobStore::load_mmap(fstring fpath, const void* mmapBase, size_t mmapSize) {
	BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
	m_mmapBase = (FileHeader*)mmapBase;
	auto header = (FileHeader*)mmapBase;
	if (fstring(m_mmapBase->magic, MagicStrLen) != MagicString) {
		throw std::invalid_argument("magic = "
			+ fstring(m_mmapBase->magic, MagicStrLen)
			+ " is not a SimpleZipBlobStore");
	}
	m_lenBits = header->lenBits;
	m_unzipSize = header->unzipSize;
	m_numRecords = header->records;
	size_t realpoolsize = header->strpoolsize * 16;
	m_strpool.risk_set_data((byte*)(header + 1) , realpoolsize);
	m_records.risk_set_data((byte*)(header + 1) + realpoolsize
						   , header->records + 1
						   , My_bsr_size_t(header->off_len_number) + 1 );
	m_off_len.risk_set_data((byte*)m_records.data() + m_records.mem_size()
						   , header->off_len_number
						   , header->off_len_minVal
						   , header->offBits + header->lenBits);
}

void SimpleZipBlobStore::save_mmap(function<void(const void*, size_t)> write) const {

    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);

    FileHeader h;
    memset(&h, 0, sizeof(h));
    h.magic_len = MagicStrLen;
    strcpy(h.magic, MagicString);
    strcpy(h.className, "SimpleZipBlobStore");
    h.unzipSize = m_unzipSize;
    h.records = m_records.size() - 1;
    h.off_len_minVal = m_off_len.min_val();
    h.off_len_number = m_off_len.size();
    h.lenBits = (uint08_t)(m_lenBits);
    h.offBits = (uint08_t)(m_off_len.uintbits() - m_lenBits);
    h.strpoolsize = (m_strpool.size() + 15) / 16;
    buffer.ensureWrite(&h, sizeof(h));
    buffer.ensureWrite(m_strpool.data(), m_strpool.size());
    PadzeroForAlign<16>(buffer, m_strpool.size());
    buffer.ensureWrite(m_records.data(), m_records.mem_size());
    buffer.ensureWrite(m_off_len.data(), m_off_len.mem_size());
}

void SimpleZipBlobStore::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_records.data(), m_records.mem_size());
    blocks->emplace_back(m_off_len.data(), m_off_len.mem_size());
}

void SimpleZipBlobStore::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(m_strpool.data(), m_strpool.used_mem_size());
}

void SimpleZipBlobStore::detach_meta_blocks(const valvec<fstring>& blocks) {
    THROW_STD(invalid_argument
        , "SimpleZipBlobStore detach_meta_blocks unsupported !");
}

size_t SimpleZipBlobStore::mem_size() const {
	return m_strpool.used_mem_size() + m_off_len.mem_size() + m_records.mem_size();
}

fstring SimpleZipBlobStore::get_mmap() const {
	return fstring((const char*)m_mmapBase, m_mmapBase->fileSize);
}

void SimpleZipBlobStore::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {
	THROW_STD(invalid_argument, "Not implemented");
}

void SimpleZipBlobStore::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
	THROW_STD(invalid_argument, "Not implemented");
}

} // namespace terark

