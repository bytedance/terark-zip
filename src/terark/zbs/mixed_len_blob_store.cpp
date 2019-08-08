#include "mixed_len_blob_store.hpp"
#include "blob_store_file_header.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/checksum_exception.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/io/StreamBuffer.hpp>
#include "blob_store_file_header.hpp"
#include "zip_reorder_map.hpp"

namespace terark {

REGISTER_BlobStore(MixedLenBlobStore, "MixedLenBlobStore");
REGISTER_BlobStore(MixedLenBlobStore64, "MixedLenBlobStore64");

static const uint64_t g_dmbsnark_seed = 0x6e654c646578694dull; // echo MixedLen | od -t x8

template<class rank_select_t>
struct MixedLenBlobStoreTpl<rank_select_t>::FileHeader : public FileHeaderBase {
    uint64_t  unzipSize;

    uint08_t  offsetsUintBits;
    uint08_t  checksumLevel;
    uint08_t  padding21[6];

    // can be zero
    // UINT32_MAX indicate that all values are var length
    uint32_t  fixedLen;

    uint32_t  isFixedRankSelectBytesDiv8;
    uint64_t  varLenBytes;
    uint64_t  fixedNum;
    uint64_t  padding22;

    void init();
    FileHeader();
    FileHeader(const MixedLenBlobStoreTpl<rank_select_t>* store);
};

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::FileHeader::init() {
    memset(this, 0, sizeof(*this));
    magic_len = MagicStrLen;
    strcpy(magic, MagicString);
    if (std::is_same<rank_select_t, rank_select_il>::value) {
        strcpy(className, "MixedLenBlobStore");
    }
    else {
        assert((std::is_same<rank_select_t, rank_select_se_512_64>::value));
        strcpy(className, "MixedLenBlobStore64");
    }
}

template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::FileHeader::FileHeader() {
    init();
}

template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::FileHeader::FileHeader(const MixedLenBlobStoreTpl<rank_select_t>* store) {
    init();
    fileSize = 0
        + sizeof(FileHeader)
        + align_up(store->m_fixedLenValues.size(), 16)
        + align_up(store->m_varLenValues.size(), 16)
        + store->m_varLenOffsets.mem_size()
        + align_up(store->m_isFixedLen.mem_size(), 16)
        + sizeof(BlobStoreFileFooter);
    unzipSize = store->m_unzipSize;
    records = store->m_numRecords;
    fixedLen = uint32_t(store->m_fixedLen);
    fixedNum = uint64_t(store->m_fixedNum);
    if (store->m_isFixedLen.size()) {
        assert(store->m_isFixedLen.mem_size() % 8 == 0);
        isFixedRankSelectBytesDiv8 = store->m_isFixedLen.mem_size() / 8;
    }
    if (UINT32_MAX == fixedLen) {
        assert(0 == fixedNum);
    }
    else {
        assert(fixedLen * fixedNum == store->m_fixedLenValues.size());
    }
    if (0 == fixedNum) {
        assert(UINT32_MAX != fixedLen);
    }
    if (store->m_varLenOffsets.size()) {
        size_t varNum = store->m_varLenOffsets.size() - 1;
        offsetsUintBits = store->m_varLenOffsets.uintbits();
        varLenBytes = store->m_varLenOffsets[varNum];
    }
    assert(fixedNum + (store->m_varLenOffsets.size() ?
        store->m_varLenOffsets.size() - 1 : 0) == records);
    checksumLevel = static_cast<uint08_t>(store->m_checksumLevel);
}

template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::MixedLenBlobStoreTpl() {
  BOOST_STATIC_ASSERT(sizeof(FileHeader) == 128);
  m_unzipSize = 0;
  m_fixedNum = 0;
  m_fixedLen = size_t(-1);
  m_fixedLenWithoutCRC = size_t(-1);
  m_checksumLevel = 0;
}

template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::~MixedLenBlobStoreTpl() {
    if (m_isDetachMeta) {
        m_isFixedLen.risk_release_ownership();
        m_varLenOffsets.risk_release_ownership();
    }
    if (m_isUserMem) {
        if (m_isMmapData) {
            mmap_close((void*)m_mmapBase, m_mmapBase->fileSize);
        }
        m_mmapBase = nullptr;
        m_isMmapData = false;
        m_isUserMem = false;
        m_isFixedLen.risk_release_ownership();
        m_fixedLenValues.risk_release_ownership();
        m_varLenValues.risk_release_ownership();
        m_varLenOffsets.risk_release_ownership();
    }
    else {
        m_isFixedLen.clear();
        m_fixedLenValues.clear();
        m_varLenValues.clear();
        m_varLenOffsets.clear();
    }
}

template<class rank_select_t>
size_t MixedLenBlobStoreTpl<rank_select_t>::mem_size() const {
	return 0
    + m_isFixedLen.mem_size()
    + m_fixedLenValues.size()
    + m_varLenValues.size()
    + m_varLenOffsets.mem_size();
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::set_func_ptr() {
	if (m_isFixedLen.empty()) {
		if (size_t(-1) != m_fixedLen) {
            m_get_record_append = static_cast<get_record_append_func_t>
                      (&MixedLenBlobStoreTpl::getFixLenRecordAppend);
            m_fspread_record_append = static_cast<fspread_record_append_func_t>
                      (&MixedLenBlobStoreTpl::fspread_FixLenRecordAppend);
		}
		else {
            m_get_record_append = static_cast<get_record_append_func_t>
                      (&MixedLenBlobStoreTpl::getVarLenRecordAppend);
            m_fspread_record_append = static_cast<fspread_record_append_func_t>
                      (&MixedLenBlobStoreTpl::fspread_VarLenRecordAppend);
		}
	}
	else {
        m_get_record_append = static_cast<get_record_append_func_t>
                  (&MixedLenBlobStoreTpl::get_record_append_has_fixed_rs);
        m_fspread_record_append = static_cast<fspread_record_append_func_t>
                    (&MixedLenBlobStoreTpl::fspread_record_append_has_fixed_rs);
	}
    // binary compatible:
    m_get_record_append_CacheOffsets =
        reinterpret_cast<get_record_append_CacheOffsets_func_t>
        (m_get_record_append);
}

template<class rank_select_t>
void
MixedLenBlobStoreTpl<rank_select_t>::
get_record_append_has_fixed_rs(size_t recID, valvec<byte_t>* recData) const {
	assert(m_isFixedLen.size() == m_numRecords);
	if (m_isFixedLen[recID]) {
		size_t fixLenRecID = m_isFixedLen.rank1(recID);
		getFixLenRecordAppend(fixLenRecID, recData);
	}
	else {
		size_t varLenRecID = m_isFixedLen.rank0(recID);
		getVarLenRecordAppend(varLenRecID, recData);
	}
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::getFixLenRecordAppend(size_t fixLenRecID, valvec<byte_t>* recData)
const {
	assert(size_t(-1) != m_fixedLen);
	assert(m_fixedLen == 0 || m_fixedLenValues.size() % m_fixedLen == 0);
	assert((fixLenRecID + 1) * m_fixedLen <= m_fixedLenValues.size());
	const byte_t* pData = m_fixedLenValues.data() + m_fixedLen * fixLenRecID;
    if (2 == m_checksumLevel) {
        uint32_t crc1 = unaligned_load<uint32_t>(pData + m_fixedLenWithoutCRC);
		uint32_t crc2 = Crc32c_update(0, pData, m_fixedLenWithoutCRC);
		if (crc2 != crc1) {
			throw BadCrc32cException(
				"MixedLenBlobStoreTpl<rank_select_t>::getFixLenRecordAppend", crc1, crc2);
		}
    }
	recData->append(pData, m_fixedLenWithoutCRC);
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::getVarLenRecordAppend(size_t varLenRecID, valvec<byte_t>* recData)
const {
	assert(varLenRecID + 1 < m_varLenOffsets.size());
	auto   basePtr = m_varLenValues.data();
	size_t offset0 = m_varLenOffsets[varLenRecID + 0];
	size_t offset1 = m_varLenOffsets[varLenRecID + 1];
	assert(offset1 <= m_varLenValues.size());
	assert(offset0 <= offset1);
    const byte_t* pData = basePtr + offset0;
    size_t len = offset1 - offset0;
    if (2 == m_checksumLevel) {
        len -= sizeof(uint32_t);
        uint32_t crc1 = unaligned_load<uint32_t>(pData + len);
		uint32_t crc2 = Crc32c_update(0, pData, len);
		if (crc2 != crc1) {
			throw BadCrc32cException(
				"MixedLenBlobStoreTpl<rank_select_t>::getVarLenRecordAppend", crc1, crc2);
		}
    }
	recData->append(pData, len);
}

template<class rank_select_t>
void
MixedLenBlobStoreTpl<rank_select_t>::
fspread_record_append_has_fixed_rs(pread_func_t fspread, void* lambda,
                                   size_t baseOffset, size_t recID,
                                   valvec<byte_t>* recData,
                                   valvec<byte_t>* rdbuf)
const {
	assert(m_isFixedLen.size() == m_numRecords);
	if (m_isFixedLen[recID]) {
		size_t fixLenRecID = m_isFixedLen.rank1(recID);
        fspread_FixLenRecordAppend(fspread, lambda, baseOffset, fixLenRecID, recData, rdbuf);
	}
	else {
		size_t varLenRecID = m_isFixedLen.rank0(recID);
        fspread_VarLenRecordAppend(fspread, lambda, baseOffset, varLenRecID, recData, rdbuf);
	}
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::fspread_FixLenRecordAppend(
                                   pread_func_t fspread, void* lambda,
                                   size_t baseOffset, size_t fixLenRecID,
                                   valvec<byte_t>* recData,
                                   valvec<byte_t>* rdbuf)
const {
	assert(size_t(-1) != m_fixedLen);
	assert(m_fixedLen == 0 || m_fixedLenValues.size() % m_fixedLen == 0);
	assert((fixLenRecID + 1) * m_fixedLen <= m_fixedLenValues.size());
    size_t fixlen = m_fixedLen;
    size_t fixLenWithoutCRC = m_fixedLenWithoutCRC;
    size_t offset = m_fixedLenValues.data() - (byte_t*)m_mmapBase + fixlen * fixLenRecID;
	auto pData = fspread(lambda, baseOffset + offset, fixlen, rdbuf);
    if (2 == m_checksumLevel) {
        uint32_t crc1 = unaligned_load<uint32_t>(pData + fixLenWithoutCRC);
        uint32_t crc2 = Crc32c_update(0, pData, fixLenWithoutCRC);
        if (crc2 != crc1) {
            throw BadCrc32cException(
                    "MixedLenBlobStoreTpl<rank_select_t>::fspread_FixLenRecordAppend", crc1, crc2);
        }
    }
	recData->append(pData, fixLenWithoutCRC);
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::fspread_VarLenRecordAppend(
                                   pread_func_t fspread, void* lambda,
                                   size_t baseOffset, size_t varLenRecID,
                                   valvec<byte_t>* recData,
                                   valvec<byte_t>* rdbuf)
const {
	assert(varLenRecID + 1 < m_varLenOffsets.size());
	size_t offset0 = m_varLenOffsets[varLenRecID + 0];
	size_t offset1 = m_varLenOffsets[varLenRecID + 1];
	assert(offset1 <= m_varLenValues.size());
	assert(offset0 <= offset1);
    size_t offset = m_varLenValues.data() - (byte_t*)m_mmapBase + offset0;
    size_t varlen = offset1 - offset0;
	auto pData = fspread(lambda, baseOffset + offset, varlen, rdbuf);
    if (2 == m_checksumLevel) {
        varlen -= sizeof(uint32_t);
        uint32_t crc1 = unaligned_load<uint32_t>(pData + varlen);
        uint32_t crc2 = Crc32c_update(0, pData, varlen);
        if (crc2 != crc1) {
            throw BadCrc32cException(
                    "MixedLenBlobStoreTpl<rank_select_t>::fspread_VarLenRecordAppend", crc1, crc2);
        }
    }
	recData->append(pData, varlen);
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::init_from_memory(fstring dataMem, Dictionary/*dict*/) {
	auto mmapBase = (const FileHeader*)dataMem.p;
	m_mmapBase = mmapBase;
	m_numRecords = mmapBase->records;
	m_unzipSize = mmapBase->unzipSize;
	m_checksumLevel = mmapBase->checksumLevel;
	m_fixedLen = int32_t(mmapBase->fixedLen); // signed extention
    m_fixedLenWithoutCRC = (2 == m_checksumLevel ? m_fixedLen - sizeof(uint32_t) : m_fixedLen);
	m_fixedNum = mmapBase->fixedNum;
	if (isChecksumVerifyEnabled()) {
		XXHash64 hash(g_dmbsnark_seed);
		hash.update(mmapBase, mmapBase->fileSize - sizeof(BlobStoreFileFooter));
		const uint64_t hashVal = hash.digest();
		auto &footer = ((const BlobStoreFileFooter*)((const byte_t*)(mmapBase)+mmapBase->fileSize))[-1];
		if (hashVal != footer.fileXXHash) {
			std::string msg = "MixedLenBlobStore::load_mmap(\"" + m_fpath + "\")";
			throw BadChecksumException(msg, footer.fileXXHash, hashVal);
		}
	}
	byte_t* curr = (byte_t*)(mmapBase + 1);
	if (m_fixedNum) {
		size_t size = size_t(m_fixedLen * m_fixedNum);
		m_fixedLenValues.risk_set_data(curr, size);
		curr += align_up(size, 16);
	}
	if (m_fixedNum < m_numRecords) {
		m_varLenValues.risk_set_data(curr, mmapBase->varLenBytes);
		curr += align_up(mmapBase->varLenBytes, 16);

		size_t varNum = m_numRecords - m_fixedNum;
		m_varLenOffsets.risk_set_data(curr, varNum + 1, mmapBase->offsetsUintBits);
		assert(m_varLenOffsets.mem_size() % 16 == 0);
		assert(m_varLenOffsets[varNum] == mmapBase->varLenBytes);
		curr += m_varLenOffsets.mem_size();
	}
	if (mmapBase->isFixedRankSelectBytesDiv8) {
		size_t size = mmapBase->isFixedRankSelectBytesDiv8 * 8;
		m_isFixedLen.risk_mmap_from(curr, size);
		curr += size;
	}
    set_func_ptr();
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    if (m_isFixedLen.size()) {
        blocks->emplace_back((byte_t*)m_isFixedLen.data(), m_isFixedLen.mem_size());
    }
    if (m_varLenOffsets.size()) {
        blocks->emplace_back(m_varLenOffsets.data(), m_varLenOffsets.mem_size());
    }
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    if (!m_fixedLenValues.empty()) {
        blocks->emplace_back(m_fixedLenValues);
    }
    if (!m_varLenValues.empty()) {
        blocks->emplace_back(m_varLenValues);
    }
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::detach_meta_blocks(const valvec<fstring>& blocks) {
    assert(!m_isDetachMeta);
    if (m_isFixedLen.size()) {
        auto fixed_len_mem = blocks.front();
        assert(fixed_len_mem.size() == m_isFixedLen.mem_size());
        if (m_isUserMem) {
            m_isFixedLen.risk_release_ownership();
        } else {
            m_isFixedLen.clear();
        }
        m_isFixedLen.risk_mmap_from((byte_t*)fixed_len_mem.data(), fixed_len_mem.size());
    }
    if (m_varLenOffsets.size()) {
        auto var_len_offsets_mem = blocks.back();
        assert(var_len_offsets_mem.size() == m_varLenOffsets.mem_size());
        if (m_isUserMem) {
            m_varLenOffsets.risk_release_ownership();
        } else {
            m_isFixedLen.clear();
        }
        size_t varNum = m_numRecords - m_fixedNum;
        m_varLenOffsets.risk_set_data((byte_t*)var_len_offsets_mem.data(), varNum + 1,
            ((const FileHeader*)m_mmapBase)->offsetsUintBits);
    }
    m_isDetachMeta = true;
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::save_mmap(function<void(const void* data, size_t size)> write) const {
    FunctionAdaptBuffer adaptBuffer(write);
    OutputBuffer buffer(&adaptBuffer);
    XXHash64 xxhash64(g_dmbsnark_seed);

    FileHeader header(this);
    xxhash64.update(&header, sizeof header);
    buffer.ensureWrite(&header, sizeof(header));
    if (m_fixedNum) {
        xxhash64.update(m_fixedLenValues.data(), m_fixedLenValues.size());
        buffer.ensureWrite(m_fixedLenValues.data(), m_fixedLenValues.size());
        PadzeroForAlign<16>(buffer, xxhash64, m_fixedLenValues.size());
    }
    if (m_varLenOffsets.size()) {
        xxhash64.update(m_varLenValues.data(), m_varLenValues.size());
        buffer.ensureWrite(m_varLenValues.data(), m_varLenValues.size());
        PadzeroForAlign<16>(buffer, xxhash64, m_varLenValues.size());

        assert(m_varLenOffsets.mem_size() % 16 == 0);
        xxhash64.update(m_varLenOffsets.data(), m_varLenOffsets.mem_size());
        buffer.ensureWrite(m_varLenOffsets.data(), m_varLenOffsets.mem_size());
    }
    if (m_isFixedLen.size()) {
        assert(m_isFixedLen.mem_size() % 8 == 0);
        xxhash64.update(m_isFixedLen.data(), m_isFixedLen.mem_size());
        buffer.ensureWrite(m_isFixedLen.data(), m_isFixedLen.mem_size());
        PadzeroForAlign<16>(buffer, xxhash64, m_isFixedLen.mem_size());
    }
    BlobStoreFileFooter footer;
    footer.fileXXHash = xxhash64.digest();
    buffer.ensureWrite(&footer, sizeof footer);
}

template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {
	FunctionAdaptBuffer adaptBuffer(writeAppend);
	OutputBuffer buffer(&adaptBuffer);

	size_t recNum = m_numRecords;
    TERARK_UNUSED_VAR(recNum);
	size_t fOffset = 0;
	size_t vOffset = 0;
	size_t vCount = 0;
	XXHash64 xxhash64(g_dmbsnark_seed);

	FileHeader header(this);
	xxhash64.update(&header, sizeof header);
	buffer.ensureWrite(&header, sizeof header);

    rank_select_t newIsFixedLen;

	bool isFixedLenEmpty = m_isFixedLen.empty();
	size_t fixedLen = m_fixedLen;
	assert(isFixedLenEmpty || m_isFixedLen.size() == m_numRecords);
	assert(fixedLen == 0 || m_fixedLenValues.size() % fixedLen == 0);

    std::unique_ptr<UintVecMin0::Builder> var_len_offset_builder(
        UintVecMin0::create_builder_by_max_value(
            m_varLenOffsets.size() > 0 ? m_varLenOffsets.back() : 0, tmpFile.c_str()
        ));

	auto pushFixLen = [&](size_t oldId) {
		assert((oldId + 1) * fixedLen <= m_fixedLenValues.size());
		const byte_t* pData = m_fixedLenValues.data() + fixedLen * oldId;
		xxhash64.update(pData, fixedLen);
		buffer.ensureWrite(pData, fixedLen);
		newIsFixedLen.push_back(true);
		fOffset += fixedLen;
	};
	auto pushVarLen = [&](size_t oldId) {
		assert(oldId + 1 < m_varLenOffsets.size());
		size_t offset0 = m_varLenOffsets[oldId + 0];
		size_t offset1 = m_varLenOffsets[oldId + 1];
        var_len_offset_builder->push_back(vOffset);
		assert(offset1 <= m_varLenValues.size());
		assert(offset0 <= offset1);
		auto ptr = m_varLenValues.data() + offset0;
		size_t len = offset1 - offset0;
		xxhash64.update(ptr, len);
		buffer.ensureWrite(ptr, len);
		vOffset += len;
		++vCount;
	};
	for(assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
		size_t oldId = *newToOld;
		assert(oldId < recNum);
		if (isFixedLenEmpty) {
			if (size_t(-1) != fixedLen) {
				pushFixLen(oldId);
			}
			else {
				newIsFixedLen.push_back(false);
			}
		}
		else {
			if (m_isFixedLen[oldId]) {
				pushFixLen(m_isFixedLen.rank1(oldId));
			}
			else {
				newIsFixedLen.push_back(false);
			}
		}
	}
	assert(fOffset == m_fixedLenValues.size());
	PadzeroForAlign<16>(buffer, xxhash64, m_fixedLenValues.size());
    for (newToOld.rewind(); !newToOld.eof(); ++newToOld) {
        //size_t newId = newToOld.index();
		size_t oldId = *newToOld;
		if (isFixedLenEmpty) {
			if (size_t(-1) == fixedLen) {
				pushVarLen(oldId);
			}
		}
		else {
			if (!m_isFixedLen[oldId]) {
				pushVarLen(m_isFixedLen.rank0(oldId));
			}
		}
	}
	assert(vOffset == m_varLenValues.size());
	PadzeroForAlign<16>(buffer, xxhash64, vOffset);

	if (vCount > 0) {
        var_len_offset_builder->push_back(vOffset);
        auto build_result = var_len_offset_builder->finish();
        var_len_offset_builder.reset();

        MmapWholeFile mmapOffset(tmpFile);
        UintVecMin0 newVarLenOffsets;
        newVarLenOffsets.risk_set_data((byte_t*)mmapOffset.base, vCount + 1,
            build_result.uintbits);
        TERARK_SCOPE_EXIT(newVarLenOffsets.risk_release_ownership());

		assert(vCount + 1 == newVarLenOffsets.size());
		xxhash64.update(newVarLenOffsets.data(), newVarLenOffsets.mem_size());
		buffer.ensureWrite(newVarLenOffsets.data(), newVarLenOffsets.mem_size());

		newIsFixedLen.build_cache(false, false);

		xxhash64.update(newIsFixedLen.data(), newIsFixedLen.mem_size());
		buffer.ensureWrite(newIsFixedLen.data(), newIsFixedLen.mem_size());
		PadzeroForAlign<16>(buffer, xxhash64, newIsFixedLen.mem_size());
	}
	else {
		newIsFixedLen.clear();
	}
    ::remove(tmpFile.c_str());

	BlobStoreFileFooter footer;
	footer.fileXXHash = xxhash64.digest();
	buffer.ensureWrite(&footer, sizeof footer);
}

/////////////////////////////////////////////////////////////////////////
template<class rank_select_t>
class MixedLenBlobStoreTpl<rank_select_t>::MyBuilder::Impl : boost::noncopyable {
    rank_select_t m_is_fixed_len;
    size_t m_fixed_len;
    size_t m_fixed_len_without_crc;
    std::string m_fpath;
    std::string m_fpath_var_len;
    std::string m_fpath_var_len_offset;
    FileStream m_file;
    FileStream m_file_var_len;
    NativeDataOutput<OutputBuffer> m_writer;
    NativeDataOutput<OutputBuffer> m_writer_var_len;
    std::unique_ptr<UintVecMin0::Builder> m_var_len_offset_builder;
    size_t m_offset;
    size_t m_content_size_fixed_len;
    size_t m_content_size_var_len;
    size_t m_content_input_size_var_len;
    size_t m_num_records;
    size_t m_num_records_var_len;
    int m_checksumLevel;

    static const size_t offset_flush_size = 128;
public:
    Impl(size_t fixedLen, size_t fixedLenWithoutCRC, size_t varLenContentSize, fstring fpath, size_t offset,
         int checksumLevel)
        : m_is_fixed_len()
        , m_fixed_len(fixedLen)
        , m_fixed_len_without_crc(fixedLenWithoutCRC)
        , m_fpath(fpath.begin(), fpath.end())
        , m_fpath_var_len(m_fpath + ".varlen")
        , m_fpath_var_len_offset(m_fpath + ".varlen-offset")
        , m_file()
        , m_file_var_len()
        , m_writer(&m_file)
        , m_writer_var_len(&m_file_var_len)
        , m_var_len_offset_builder(UintVecMin0::create_builder_by_max_value(
            varLenContentSize, m_fpath_var_len_offset.c_str()))
        , m_offset(offset)
        , m_content_size_fixed_len(0)
        , m_content_size_var_len(0)
        , m_content_input_size_var_len(varLenContentSize)
        , m_num_records(0)
        , m_num_records_var_len(0)
        , m_checksumLevel(checksumLevel) {
        assert(offset % 8 == 0);
        if (offset == 0) {
            m_file.open(fpath, "wb");
        }
        else {
            m_file.open(fpath, "rb+");
            m_file.seek(offset);
        }
        m_file_var_len.open(m_fpath_var_len, "wb+");
        m_file_var_len.disbuf();
        typename std::aligned_storage<sizeof(FileHeader)>::type header;
        memset(&header, 0, sizeof header);
        m_writer.ensureWrite(&header, sizeof header);
    }
    ~Impl() {
        if (!m_fpath_var_len.empty()) {
            ::remove(m_fpath_var_len.c_str());
        }
        if (!m_fpath_var_len_offset.empty()) {
            ::remove(m_fpath_var_len_offset.c_str());
        }
    }
    void add_record(fstring rec) {
        if (rec.size() == m_fixed_len_without_crc) {
            m_writer.ensureWrite(rec.data(), rec.size());
            m_is_fixed_len.push_back(true);
            m_content_size_fixed_len += rec.size();
            if (2 == m_checksumLevel) {
                uint32_t crc = Crc32c_update(0, rec.data(), rec.size());
                m_writer.ensureWrite(&crc, sizeof(crc));
                m_content_size_fixed_len += sizeof(crc);
            }
        }
        else {
            m_var_len_offset_builder->push_back(m_content_size_var_len);
            m_writer_var_len.ensureWrite(rec.data(), rec.size());
            m_is_fixed_len.push_back(false);
            m_content_size_var_len += rec.size();
            if (2 == m_checksumLevel) {
                uint32_t crc = Crc32c_update(0, rec.data(), rec.size());
                m_writer_var_len.ensureWrite(&crc, sizeof(crc));
                m_content_size_var_len += sizeof(crc);
            }
            ++m_num_records_var_len;
        }
        ++m_num_records;
    }
    void finish() {
        assert(m_content_size_var_len <= m_content_input_size_var_len);
        PadzeroForAlign<16>(m_writer, m_content_size_fixed_len);
        m_writer.flush_buffer();
        m_writer_var_len.flush();
        m_file_var_len.rewind();
        m_file.cat(m_file_var_len.fp());
        m_file_var_len.close();
        PadzeroForAlign<16>(m_writer, m_content_size_var_len);

        ::remove(m_fpath_var_len.c_str());
        m_fpath_var_len.clear();

        size_t fixed_len_count;
        size_t var_len_uintbits;
        size_t offsets_var_len_size;
        if (m_num_records_var_len == 0) {
            m_is_fixed_len.clear();
            fixed_len_count = m_num_records;
            var_len_uintbits = 0;
            offsets_var_len_size = 0;
            m_var_len_offset_builder.reset();
        }
        else {
            m_var_len_offset_builder->push_back(m_content_size_var_len);
            auto build_result = m_var_len_offset_builder->finish();
            m_var_len_offset_builder.reset();

            if (m_num_records_var_len == m_num_records) {
                m_fixed_len = size_t(-1);
                m_is_fixed_len.clear();
                fixed_len_count = 0;
            }
            else {
                m_is_fixed_len.build_cache(false, false);
                fixed_len_count = m_is_fixed_len.max_rank1();
            }
            var_len_uintbits = build_result.uintbits;
            offsets_var_len_size = build_result.mem_size;
        }

        m_writer.flush_buffer();
        m_file.cat(m_fpath_var_len_offset);
        ::remove(m_fpath_var_len_offset.c_str());
        m_fpath_var_len_offset.clear();

        size_t is_fixed_len_size = 0;
        if (!m_is_fixed_len.empty()) {
            is_fixed_len_size = m_is_fixed_len.mem_size();
            m_writer.ensureWrite(m_is_fixed_len.data(), is_fixed_len_size);
            PadzeroForAlign<16>(m_writer, is_fixed_len_size);
        }

        m_writer.flush_buffer();
        m_file.close();

        size_t file_size = m_offset
            + sizeof(FileHeader)
            + align_up(m_content_size_fixed_len, 16)
            + align_up(m_content_size_var_len, 16)
            + offsets_var_len_size
            + align_up(is_fixed_len_size, 16)
            + sizeof(BlobStoreFileFooter);
        assert(FileStream(m_fpath, "rb+").fsize() == file_size - sizeof(BlobStoreFileFooter));
        FileStream(m_fpath, "rb+").chsize(file_size);
        MmapWholeFile mmap(m_fpath, true);
        fstring mem((const char*)mmap.base + m_offset, (ptrdiff_t)(file_size - m_offset));
        FileHeader& header = *(FileHeader*)mem.data();
        header = FileHeader();


        header.fileSize = file_size - m_offset;
        header.unzipSize = m_content_size_fixed_len + m_content_size_var_len;
        header.records = m_num_records;
        header.fixedLen = uint32_t(m_fixed_len);
        header.fixedNum = uint64_t(fixed_len_count);
        if (is_fixed_len_size) {
            assert(is_fixed_len_size % 8 == 0);
            header.isFixedRankSelectBytesDiv8 = is_fixed_len_size / 8;
        }
        if (m_content_size_var_len) {
            header.offsetsUintBits = var_len_uintbits;
            header.varLenBytes = m_content_size_var_len;
        }
        header.checksumLevel = static_cast<uint8_t>(m_checksumLevel);

        XXHash64 xxhash64(g_dmbsnark_seed);
        xxhash64.update(mem.data(), mem.size() - sizeof(BlobStoreFileFooter));

        BlobStoreFileFooter footer;
        footer.fileXXHash = xxhash64.digest();
        ((BlobStoreFileFooter*)(mem.data() + mem.size()))[-1] = footer;
    }
};

template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::MyBuilder::~MyBuilder() {
    delete impl;
}
template<class rank_select_t>
MixedLenBlobStoreTpl<rank_select_t>::MyBuilder::MyBuilder(size_t fixedLen,
                                                          size_t varLenContentSize,
                                                          size_t varLenContentCnt,
                                                          fstring fpath,
                                                          size_t offset,
                                                          int checksumLevel) {
    size_t fixedLenWithoutCRC = fixedLen;
    if (2 == checksumLevel) { // record level crc, 32 bits per record
        fixedLen += sizeof(uint32_t);
        varLenContentSize += sizeof(uint32_t) * varLenContentCnt;
    }
    impl = new Impl(fixedLen, fixedLenWithoutCRC, varLenContentSize, fpath, offset, checksumLevel);
}
template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::MyBuilder::addRecord(fstring rec) {
    assert(NULL != impl);
    impl->add_record(rec);
}
template<class rank_select_t>
void MixedLenBlobStoreTpl<rank_select_t>::MyBuilder::finish() {
    assert(NULL != impl);
    return impl->finish();
}

template class TERARK_DLL_EXPORT MixedLenBlobStoreTpl<rank_select_il>;
template class TERARK_DLL_EXPORT MixedLenBlobStoreTpl<rank_select_se_512_64>;

} // namespace terark
