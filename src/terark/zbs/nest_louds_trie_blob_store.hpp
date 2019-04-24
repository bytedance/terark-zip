#ifndef __terark_nest_louds_trie_blob_store_hpp__
#define __terark_nest_louds_trie_blob_store_hpp__

#include <terark/zbs/abstract_blob_store.hpp>
#include <terark/fsa/nest_louds_trie.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/tmplinst.hpp>

namespace terark {

template<class NestLoudsTrie>
class TERARK_DLL_EXPORT NestLoudsTrieBlobStore
	: public AbstractBlobStore, public BaseDFA, public NestLoudsTrie {
	UintVector m_recIdVec;
	unsigned m_zpNestLevel;
	bool m_isDawgStrPool;
public:
	typedef size_t state_id_t;
	enum { sigma = 256 };
	using NestLoudsTrie::nil_state;
	using NestLoudsTrie::max_state;

	typedef typename NestLoudsTrie::StateMoveContext StateMoveContext;

	using AbstractBlobStore::get_record;
	using NestLoudsTrie::total_states;
	using NestLoudsTrie::is_pzip;
	using NestLoudsTrie::has_children;
	using NestLoudsTrie::num_children;
	using NestLoudsTrie::gnode_states;
	using BaseDFA::num_zpath_states;
	using BaseDFA::total_zpath_len;
	using NestLoudsTrie::state_move;
	using NestLoudsTrie::state_move_fast;
	using NestLoudsTrie::state_move_slow;
	using NestLoudsTrie::for_each_move;
	using NestLoudsTrie::for_each_dest;
	using NestLoudsTrie::for_each_dest_rev;

	void swap(NestLoudsTrieBlobStore& y);

	bool has_freelist() const override { return false; }
	bool v_has_children(size_t s) const override { return NestLoudsTrie::has_children(s); }
	size_t v_gnode_states() const override { return NestLoudsTrie::gnode_states(); }

	bool is_term(size_t) const { return false; }

	fstring get_zpath_data(size_t, MatchContext*) const { THROW_STD(invalid_argument, "not implemented"); }
	size_t zp_nest_level() const override { return NestLoudsTrie::nest_level(); }
	size_t mem_size() const override { return m_recIdVec.mem_size() + NestLoudsTrie::mem_size(); }

    NestLoudsTrieBlobStore();
	~NestLoudsTrieBlobStore();
	void build_from(SortableStrVec&, const NestLoudsTrieConfig&);
	void init_from_memory(fstring dataMem, Dictionary dict) override;
    void get_meta_blocks(valvec<fstring>* blocks) const override;
    void get_data_blocks(valvec<fstring>* blocks) const override;
	void get_record_append_imp(size_t recID, valvec<byte_t>* recData) const;
	void reorder(const uint32_t* newToOld, fstring newFilePath);
	fstring get_mmap() const override;
    void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const override;

	template<class DataIO>
	void dio_load(DataIO& dio) {
		THROW_STD(logic_error, "Not implemented");
	}
	template<class DataIO>
	void dio_save(DataIO& dio) const {
		THROW_STD(logic_error, "Not implemented");
	}
	template<class DataIO>
	friend void
	DataIO_loadObject(DataIO& dio, NestLoudsTrieBlobStore& dfa) { dfa.dio_load(dio); }
	template<class DataIO>
	friend void
	DataIO_saveObject(DataIO& dio, const NestLoudsTrieBlobStore& dfa) { dfa.dio_save(dio); }

	void write_dot_file(FILE* fp) const override { NestLoudsTrie::write_dot_file(fp); }
	void write_dot_file(fstring fname) const override { NestLoudsTrie::write_dot_file(fname); }

	using BaseDFA::load_mmap;

    void save_mmap(fstring fpath) const override;
    void save_mmap(function<void(const void*, size_t)> write) const override;
	void finish_load_mmap(const DFA_MmapHeader*) override;
	long prepare_save_mmap(DFA_MmapHeader*, const void**) const override;

	void str_stat(std::string* ) const override;

	typedef NestLoudsTrieBlobStore MyType;
#include <terark/fsa/ppi/dfa_const_virtuals.hpp>
};

typedef NestLoudsTrieBlobStore<NestLoudsTrie_SE> NestLoudsTrieBlobStore_SE;
typedef NestLoudsTrieBlobStore<NestLoudsTrie_IL> NestLoudsTrieBlobStore_IL;
typedef NestLoudsTrieBlobStore<NestLoudsTrie_SE_512> NestLoudsTrieBlobStore_SE_512;

typedef NestLoudsTrieBlobStore<NestLoudsTrie_Mixed_SE_512> NestLoudsTrieBlobStore_Mixed_SE_512;
typedef NestLoudsTrieBlobStore<NestLoudsTrie_Mixed_IL_256> NestLoudsTrieBlobStore_Mixed_IL_256;
typedef NestLoudsTrieBlobStore<NestLoudsTrie_Mixed_XL_256> NestLoudsTrieBlobStore_Mixed_XL_256;

} // namespace terark

#endif // __terark_nest_louds_trie_blob_store_hpp__

