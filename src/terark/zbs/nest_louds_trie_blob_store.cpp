#include "nest_louds_trie_blob_store.hpp"
#include "zip_reorder_map.hpp"


#if __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wdynamic-class-memaccess"
#endif

namespace terark {

///////////////////////////////////////////////////////////////////////////
/// NestLoudsTrieBlobStore:
template<class NestLoudsTrie>
NestLoudsTrieBlobStore<NestLoudsTrie>::NestLoudsTrieBlobStore() {
    this->m_get_record_append = static_cast<get_record_append_func_t>
                  (&NestLoudsTrieBlobStore::get_record_append_imp);
    // binary compatible:
    m_get_record_append_CacheOffsets =
        reinterpret_cast<get_record_append_CacheOffsets_func_t>
        (m_get_record_append);
}

template<class NestLoudsTrie>
NestLoudsTrieBlobStore<NestLoudsTrie>::~NestLoudsTrieBlobStore() {
	if (BaseDFA::mmap_base) {
		AbstractBlobStore::m_mmapBase = nullptr; // unmap by BaseDFA
		m_recIdVec.risk_release_ownership();
		NestLoudsTrie::risk_release_ownership();
	}
	else {
		assert(nullptr == AbstractBlobStore::m_mmapBase);
		if (AbstractBlobStore::m_mmapBase) {
			fprintf(stderr
				, "ERROR: %s: when BaseDFA::mmap_base is NULL, AbstractBlobStore::m_mmapBase must also be NULL\n"
				, BOOST_CURRENT_FUNCTION
			);
		}
	}
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::
build_from(SortableStrVec& strVec, const NestLoudsTrieConfig& conf) {
	valvec<uint32_t> idvec;
	AbstractBlobStore::m_unzipSize = strVec.str_size();
	if (conf.flags[conf.optUseDawgStrPool]) {
		NestLoudsTrie::build_strpool2(strVec, idvec, conf);
		m_isDawgStrPool = true;
	} else {
		NestLoudsTrie::build_strpool(strVec, idvec, conf);
		m_isDawgStrPool = false;
	}
	m_recIdVec.build_from(idvec);
	AbstractBlobStore::m_numRecords = idvec.size();
	BaseDFA::m_zpath_states = NestLoudsTrie::num_zpath_states();
	BaseDFA::m_total_zpath_len = NestLoudsTrie::total_zpath_len();
	m_zpNestLevel = conf.nestLevel;
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::
get_record_append_imp(size_t recID, valvec<byte_t>* recData) const {
	assert(recID < m_recIdVec.size());
	size_t node_id = m_recIdVec[recID];
	assert(node_id < NestLoudsTrie::total_states());
	if (m_isDawgStrPool)
		NestLoudsTrie::restore_dawg_string_append(node_id, recData);
	else
		NestLoudsTrie::restore_string_append(node_id, recData);
}

///! Will temporarily change *this for save_mmap and restore after saving
template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::
reorder(const uint32_t* newToOld, fstring newFile) {
	size_t recNum = this->m_numRecords;
	size_t minVal = m_recIdVec.min_val();
	size_t maxVal = this->total_states()-1;
	UintVector newIdVec(recNum, minVal, maxVal);
	for(size_t newId = 0; newId < recNum; ++newId) {
		size_t oldId = newToOld[newId];
		size_t value = m_recIdVec.get(oldId);
		newIdVec.set(newId, value);
	}
    typedef NestLoudsTrieBlobStore<NestLoudsTrie> self_t;
    //DON'T WRITE CODE LIKE FOLLOWING
    typename std::aligned_storage<sizeof(self_t), alignof(self_t)>::type shadow_storage;
    memcpy(&shadow_storage, this, sizeof(self_t));
    auto shadow = reinterpret_cast<self_t*>(&shadow_storage);
    memcpy(&shadow->m_recIdVec, &newIdVec, sizeof(newIdVec));
    //!!!!!!!!!!!!!
    shadow->save_mmap(newFile);
}

template<class NestLoudsTrie>
fstring NestLoudsTrieBlobStore<NestLoudsTrie>::get_mmap() const {
	return fstring((const char*)mmap_base, mmap_base->file_size);
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::swap(NestLoudsTrieBlobStore& y) {
	BaseDFA::risk_swap(y);
	AbstractBlobStore::risk_swap(y);
	NestLoudsTrie::swap(y);
	m_recIdVec.swap(y.m_recIdVec);
	std::swap(m_zpNestLevel, y.m_zpNestLevel);
	std::swap(m_isDawgStrPool, y.m_isDawgStrPool);
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::
init_from_memory(fstring dataMem, Dictionary/*dict*/) {
	std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap_user_mem(dataMem));
	if (auto nlt = dynamic_cast<NestLoudsTrieBlobStore*>(dfa.get())) {
		this->swap(*nlt);
	} else {
		THROW_STD(invalid_argument
			, "dataMem is not a NestLoudsTrieBlobStore, dfa stat: %s"
			, dfa->str_stat().c_str());
	}
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::
get_meta_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
    blocks->emplace_back(this->get_mmap()); // all data are index
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::
get_data_blocks(valvec<fstring>* blocks) const {
    blocks->erase_all();
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::
detach_meta_blocks(const valvec<fstring>& blocks) {
    THROW_STD(invalid_argument
        , "NestLoudsTrieBlobStore detach_meta_blocks unsupported !");
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::reorder_zip_data(
        ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile)
const {

	size_t recNum = this->m_numRecords;
	size_t minVal = m_recIdVec.min_val();
	size_t maxVal = this->total_states()-1;
	UintVector newIdVec(recNum, minVal, maxVal);
    for (assert(newToOld.size() == recNum); !newToOld.eof(); ++newToOld) {
        size_t newId = newToOld.index();
        size_t oldId = *newToOld;
		size_t value = m_recIdVec.get(oldId);
		newIdVec.set(newId, value);
	}
    typedef NestLoudsTrieBlobStore<NestLoudsTrie> self_t;
    //DON'T WRITE CODE LIKE FOLLOWING
    typename std::aligned_storage<sizeof(self_t), alignof(self_t)>::type shadow_storage;
    memcpy(&shadow_storage, this, sizeof(self_t));
    auto shadow = reinterpret_cast<self_t*>(&shadow_storage);
    memcpy(&shadow->m_recIdVec, &newIdVec, sizeof(newIdVec));
    //!!!!!!!!!!!!!
    shadow->save_mmap(writeAppend);
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::
finish_load_mmap(const DFA_MmapHeader* base) {
	byte_t* bbase = (byte_t*)base;
	assert(2 == base->num_blocks);
	AbstractBlobStore::m_numRecords = size_t(base->dawg_num_words);
	size_t minID = base->louds_dfa_min_zpath_id;
	size_t maxID = base->total_states-1;
	assert(minID <= maxID);
	size_t uBits = (maxID > minID) ? terark_bsr_u32(maxID - minID) + 1 : 0;
	m_recIdVec.risk_set_data(bbase + base->blocks[0].offset, m_numRecords, minID, uBits);
	assert(m_recIdVec.mem_size() ==  base->blocks[0].length);
	NestLoudsTrie::load_mmap(bbase + base->blocks[1].offset, base->blocks[1].length);
	BaseDFA::m_total_zpath_len = base->zpath_length;
	BaseDFA::m_zpath_states = base->zpath_states;
	BaseDFA::m_is_dag = true;
	this->m_zpNestLevel = NestLoudsTrie::nest_level();
	this->m_isDawgStrPool = base->is_nlt_dawg_strpool ? true : false;
	AbstractBlobStore::m_unzipSize = base->numFreeStates; // use numFreeStates as unzipped size
	AbstractBlobStore::m_mmapBase = (const FileHeaderBase*)(base);
	AbstractBlobStore::m_checksumLevel = base->crc32cLevel;
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::save_mmap(fstring fpath)
const {
    BaseDFA::save_mmap(fpath);
}

template<class NestLoudsTrie>
void NestLoudsTrieBlobStore<NestLoudsTrie>::save_mmap(
    function<void(const void* data, size_t size)> writeAppend)
const {
    BaseDFA::save_mmap(writeAppend);
}

template<class NestLoudsTrie>
long
NestLoudsTrieBlobStore<NestLoudsTrie>::
prepare_save_mmap(DFA_MmapHeader* base, const void** dataPtrs) const {
	base->is_dag = true;
	base->dawg_num_words = this->m_numRecords;
	base->total_states   = this->total_states();
	base->transition_num = this->total_states() - 1;
	base->num_blocks = 2;
	base->is_nlt_dawg_strpool = m_isDawgStrPool;
	base->zpath_length = BaseDFA::m_total_zpath_len;
	base->zpath_states = BaseDFA::m_zpath_states;
	base->louds_dfa_min_zpath_id = m_recIdVec.min_val();
	base->numFreeStates = AbstractBlobStore::m_unzipSize; // use numFreeStates as unzipped size

	dataPtrs[0] = m_recIdVec.data();
	base->blocks[0].offset = sizeof(DFA_MmapHeader);
	base->blocks[0].length = m_recIdVec.mem_size();

	size_t trie_mem_size = 0;
	byte_t const* tmpbuf = NestLoudsTrie::save_mmap(&trie_mem_size);
	dataPtrs[1] = tmpbuf;
	base->blocks[1].offset = align_to_64(base->blocks[0].endpos());
	base->blocks[1].length = trie_mem_size;
	base->louds_dfa_num_zpath_trie = NestLoudsTrie::nest_level();

	long need_free_mask = long(1) << 1;
	return need_free_mask;
}

template<class NestLoudsTrie>
void
NestLoudsTrieBlobStore<NestLoudsTrie>::str_stat(std::string* s) const {
	NestLoudsTrie::str_stat(s);
	char buf[80];
	snprintf(buf, sizeof(buf)
		, "UnzippedSize=%10lld (use header.numFreeStates)\n"
		, llong(AbstractBlobStore::m_unzipSize)
		);
	*s = buf + std::move(*s);
}

template class NestLoudsTrieBlobStore<NestLoudsTrie_SE>;
template class NestLoudsTrieBlobStore<NestLoudsTrie_IL>;
template class NestLoudsTrieBlobStore<NestLoudsTrie_SE_512>;

TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_SE)
TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_IL)
TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_SE_512)
TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_Mixed_SE_512)
TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_Mixed_IL_256)
TMPL_INST_DFA_CLASS(NestLoudsTrieBlobStore_Mixed_XL_256)

TERARK_DLL_EXPORT
AbstractBlobStore*
NestLoudsTrieBlobStore_build(fstring clazz, int nestLevel, SortableStrVec& strVec) {
	NestLoudsTrieConfig conf;
	conf.initFromEnv();
	conf.nestLevel = nestLevel;
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define ForClass(Class) \
	if (clazz == #Class) { \
		std::unique_ptr<Class> p(new Class()); \
		p->build_from(strVec, conf); \
		return p.release(); \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	ForClass(NestLoudsTrieBlobStore_SE);
	ForClass(NestLoudsTrieBlobStore_IL);
	ForClass(NestLoudsTrieBlobStore_SE_512);
	ForClass(NestLoudsTrieBlobStore_Mixed_SE_512);
	ForClass(NestLoudsTrieBlobStore_Mixed_IL_256);
	ForClass(NestLoudsTrieBlobStore_Mixed_XL_256);
	THROW_STD(invalid_argument, "unknown class name: %s", clazz.c_str());
}

} // namespace terark

#if __clang__
# pragma clang diagnostic pop
#endif
