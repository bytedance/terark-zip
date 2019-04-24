#ifndef __terark_automata_nest_louds_trie_hpp__
#define __terark_automata_nest_louds_trie_hpp__

#include <terark/fsa/x_fsa_util.hpp>
#include <terark/fsa/fsa.hpp>
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>
#include <terark/int_vector.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/zo_sorted_strvec.hpp>
#include "dfa_algo_basic.hpp"
#include "fsa_cache.hpp"

namespace terark {

class TERARK_DLL_EXPORT NestLoudsTrieConfig {
public:
	enum OptFlags {
		optReserved = 0,
		optSearchDelimForward = 1,
		optCutFragOnPunct = 2,
		optUseDawgStrPool = 3,
	};
	int nestLevel;
	int maxFragLen; // maxFragLen < 0 indicate don't split by linefeeds
	int minFragLen;
	byte_t minLinkStrLen;
	byte_t corestrCompressLevel;
	byte_t saFragMinFreq;
	byte_t bzMinLen;
	static_bitmap<TERARK_WORD_BITS> flags;
	static_bitmap<256> bestDelimBits;
	mutable std::unique_ptr<class SuffixTrieCacheDFA> suffixTrie;
	mutable uint16_t* bestZipLenArr;

	std::string tmpDir;

	/// taking effect only when tmpDir is not empty
	/// 0: default, real tmpLevel is used in a smart way
	/// 1: use tmpfile for BFS queue
	/// 2: swap out linkVec when load large object from tmp file
	/// 3: linkVec is write to tmp file with 2x size
	///    and restore from tmp file when required
	/// 4: save nestStrPool to tmp file
	int tmpLevel;

	/// if true, top level sort will be omitted, this improves performance
	/// default: false
	bool isInputSorted;
	byte debugLevel;

    /// if nestStrVec * nestScale < inputStrVec, stop nesting
    ///   default is 8
    ///   set to 1   will disable  nesting
    //    set to 255 will maximize nesting
    byte nestScale;

	/// to reduce queue tmpfile size
	bool enableQueueCompression;

	bool useMixedCoreLink;

	NestLoudsTrieConfig();
	~NestLoudsTrieConfig();
	void initFromEnv();
	void setBestDelims(const char* delims);
};

template<class RankSelect, class RankSelect2 = RankSelect, bool FastLabel = false>
class TERARK_DLL_EXPORT NestLoudsTrieTpl {
public: // protected:
    typedef typename RankSelect::index_t index_t;
    RankSelect    m_louds;
	RankSelect2   m_is_link;  // m_is_link[child_node_id]
	UintVector    m_next_link;
	byte_t*       m_label_data; // m_label_data[0] is unused
	byte_t*       m_core_data;
	size_t        m_core_size;
	size_t        m_core_len_mask;
	byte_t        m_core_len_bits;
	byte_t        m_core_min_len;
	size_t        m_core_max_link_val;
	uint64_t      m_total_zpath_len;
	NestLoudsTrieTpl<RankSelect, RankSelect>* m_next_trie;
	struct layer_ref_t {
		index_t beg, end, mid;
	};
    valvec<uint32_t>    m_sel0_cache;
	valvec<index_t>     m_layer_id;
	valvec<index_t>     m_layer_rank;
	valvec<layer_ref_t> m_layer_ref;
	uint32_t            m_max_layer_id;
	uint32_t            m_max_layer_size;
    uint32_t            m_max_strlen;

public:
	typedef RankSelect rank_select_t;
	typedef RankSelect2 rank_select2_t;
	typedef typename RankSelect2::is_mixed is_link_rs_mixed;
	static const bool is_fast_label = FastLabel;

    template<class Dawg>
    class TERARK_DLL_EXPORT Iterator : public ADFA_LexIterator {
    protected:
        struct Entry;
        struct Entry* m_top;
        struct Entry* m_base;
        const NestLoudsTrieTpl* m_trie;
        void reset1();
        void append_lex_min_suffix(size_t root, Entry* ip, byte_t* wp);
        void append_lex_max_suffix(size_t root, Entry* ip, byte_t* wp);
    public:
        Iterator();
        explicit Iterator(const Dawg*);
        void reset(const BaseDFA*, size_t root = 0) override;
        bool seek_end() override final;
        bool seek_lower_bound(fstring key) override final;
        bool incr() override final;
        bool decr() override final;
        size_t seek_max_prefix(fstring) override final;
    };
    template<class Dawg>
    class TERARK_DLL_EXPORT UserMemIterator : public Iterator<Dawg> {
        typedef typename Iterator<Dawg>::Entry Entry;
        using   Iterator<Dawg>::m_dfa;
        using   Iterator<Dawg>::m_top;
        using   Iterator<Dawg>::m_base;
        using   Iterator<Dawg>::m_trie;
        using   Iterator<Dawg>::m_word;
    public:
        UserMemIterator(const Dawg*, void* user_mem);
        ~UserMemIterator();
        static size_t s_max_mem_size(const NestLoudsTrieTpl* trie);
        void reset(const BaseDFA*, size_t root = 0) override;
    };
    template<class Entry>
    size_t initIterEntry(size_t parent, Entry*, byte_t* buf, size_t cap) const;
    byte_t getFirstChar(size_t child0, size_t lcount) const;
    byte_t getNthChar(size_t child0, size_t lcount, size_t nth) const;
    byte_t getNthCharNext(size_t child0, size_t lcount, size_t nth, byte_t cch) const;
    byte_t getNthCharPrev(size_t child0, size_t lcount, size_t nth, byte_t cch) const;
	size_t getZpathFixed(size_t, byte_t* buf, size_t cap) const;
    intptr_t matchZpath(size_t, const byte_t* str, size_t slen) const;
	intptr_t matchZpath_loop(size_t, intptr_t, const byte_t* str, intptr_t slen) const;

	NestLoudsTrieTpl();
	NestLoudsTrieTpl(const NestLoudsTrieTpl&);
	NestLoudsTrieTpl& operator=(const NestLoudsTrieTpl&);
	~NestLoudsTrieTpl();

	void risk_release_ownership();
	void swap(NestLoudsTrieTpl&);

	size_t total_states() const { return m_is_link.size(); }
	size_t mem_size() const;
	size_t nest_level() const;
	size_t core_mem_size() const;

	size_t get_parent(size_t child) const;
	uint64_t get_link_val(size_t node_id) const;
	fstring get_core_str(size_t node_id) const;

	void restore_string_append(size_t node_id, valvec<byte_t>* str) const;
	void restore_string_append(size_t node_id, std::string   * str) const;

	void restore_dawg_string_append(size_t node_id, valvec<byte_t>* str) const;
	void restore_dawg_string_append(size_t node_id, std::string   * str) const;

	void restore_string(size_t node_id, valvec<byte_t>* str) const;
	void restore_dawg_string(size_t node_id, valvec<byte_t>* str) const;
	void restore_next_string(size_t node_id, valvec<byte_t>* str) const;
	void restore_string_loop(size_t node_id, valvec<byte_t>* str) const;

	void restore_string(size_t node_id, std::string* str) const;
	void restore_dawg_string(size_t node_id, std::string* str) const;
	void restore_next_string(size_t node_id, std::string* str) const;
	void restore_string_loop(size_t node_id, std::string* str) const;

	void build_strpool(SortableStrVec&, valvec<index_t>& idvec, const NestLoudsTrieConfig&);
	void build_strpool2(SortableStrVec& strVec, valvec<index_t>& linkVec, const NestLoudsTrieConfig& conf);
	void build_patricia(SortableStrVec&, function<void(const valvec<index_t>& linkVec)> buildTerm, const NestLoudsTrieConfig&);
	void build_patricia(FixedLenStrVec&, function<void(const valvec<index_t>& linkVec)> buildTerm, const NestLoudsTrieConfig&);
	void build_patricia(  SortedStrVec&, function<void(const valvec<index_t>& linkVec)> buildTerm, const NestLoudsTrieConfig&);
	void build_patricia(ZoSortedStrVec&, function<void(const valvec<index_t>& linkVec)> buildTerm, const NestLoudsTrieConfig&);

    template<class StrVecType>
    void build_patricia_tpl(StrVecType&, function<void(const valvec<index_t>& linkVec)> buildTerm, const NestLoudsTrieConfig&);

	void load_mmap(const void* data, size_t size);
	byte_t* save_mmap(size_t* pSize) const;

    template<class RankSelectTerm>
    void init_for_term(const RankSelectTerm& is_term);

	void write_dot_file(FILE* fp) const;
	void write_dot_file(fstring fname) const;

	void str_stat(std::string* ) const;

public: // protected:
	void build_strpool_loop(SortableStrVec&, valvec<index_t>& linkVec, size_t curNestLevel, const NestLoudsTrieConfig&);
	void build_link(valvec<index_t>& nextLinkVec, valvec<byte_t>& label);
	void build_self_trie(SortableStrVec&, valvec<index_t>& linkVec, valvec<byte_t>& label, size_t curNestLevel, const NestLoudsTrieConfig&);
	template<class StrVecType>
	void build_self_trie_tpl(StrVecType&, SortableStrVec&, valvec<index_t>& linkVec, valvec<byte_t>& label, size_t curNestLevel, const NestLoudsTrieConfig&);
	void build_core(SortableStrVec& strVec, valvec<byte_t>& label, const NestLoudsTrieConfig&);
	void build_core_no_reverse_keys(SortableStrVec& strVec, valvec<byte_t>& label, const NestLoudsTrieConfig&);
	void build_mixed(SortableStrVec& strVec, valvec<byte_t>& label, size_t curNestLevel, const NestLoudsTrieConfig&);
	void build_nested(SortableStrVec& strVec, valvec<byte_t>& label, size_t curNestLevel, const NestLoudsTrieConfig&);

	void debug_equal_check(const NestLoudsTrieTpl&) const;

	template<class StrBuf>
	void tpl_restore_string_append(size_t node_id, StrBuf* str) const;
	template<class StrBuf>
	void tpl_restore_dawg_string_append(size_t node_id, StrBuf* str) const;
	template<class StrBuf>
	void tpl_restore_next_string(size_t node_id, StrBuf* str) const;
	template<class StrBuf>
	void tpl_restore_string_loop(size_t node_id, StrBuf* str) const;
	template<class StrBuf>
	void tpl_restore_string_loop_ex(size_t node_id, StrBuf* str, bool reverse) const;

	byte_t label_first_byte(size_t node_id) const;

public:
	static const size_t nil_state = size_t(-1);
	static const size_t max_state = size_t(-2);

	fstring get_zpath_data(size_t, MatchContext*) const;

    bool is_pzip(size_t s) const {
        assert(s < m_is_link.size());
        return m_is_link[s];
    }
	bool   has_children(size_t) const;
	size_t num_children(size_t) const;
	size_t gnode_states() const;
	size_t num_zpath_states() const { return m_is_link.max_rank1(); }
	uint64_t total_zpath_len() const { return m_total_zpath_len; }

	struct StateMoveContext {
		size_t n_children;
		size_t child0;
	};
	size_t state_move(size_t, auchar_t ch) const;
    std::pair<size_t, bool> state_move_lower_bound(size_t, auchar_t ch) const;
	size_t state_move_slow(size_t, auchar_t ch, StateMoveContext& ctx) const;
	size_t state_move_fast(size_t, auchar_t ch, size_t n_children, size_t child0) const;
	size_t state_move_fast(size_t parent, auchar_t ch, const StateMoveContext& ctx) const {
		return state_move_fast(parent, ch, ctx.n_children, ctx.child0);
	}
    template<class LoudsBits, class LoudsSel, class LoudsRank>
    size_t state_move_fast2(size_t parent, byte_t ch, const byte_t* label,
                            const LoudsBits*, const LoudsSel* sel0, const LoudsRank*) const;

    template<bool HasLink>
    size_t state_move_smart(size_t s, auchar_t ch) const {
        if (HasLink || FastLabel)
            return state_move(s, ch);
        else
            return state_move_no_link(s, ch);
    }
    template<bool HasLink>
    std::pair<size_t, bool>
    state_move_lower_bound_smart(size_t s, auchar_t ch) const {
        if (HasLink || FastLabel)
            return state_move_lower_bound(s, ch);
        else
            return state_move_lower_bound_no_link(s, ch);
    }
private:
    size_t state_move_no_link(size_t, auchar_t ch) const;
    std::pair<size_t, bool>
    state_move_lower_bound_no_link(size_t, auchar_t ch) const;

public:
    template<class RankSelectTerm>
    void lower_bound(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank, const NTD_CacheTrie* cache, const RankSelectTerm& is_term) const;
    template<class RankSelectTerm, bool HasLink, bool HasDictRank>
    void lower_bound_impl(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank, const NTD_CacheTrie* cache, const RankSelectTerm& is_term) const;

    template<class RankSelectTerm>
    size_t state_begin(const RankSelectTerm& is_term) const;
    template<class RankSelectTerm>
    size_t state_end(const RankSelectTerm& is_term) const;
    template<class RankSelectTerm>
    size_t state_next(size_t state, const RankSelectTerm& is_term) const;
    template<class RankSelectTerm>
    size_t state_prev(size_t state, const RankSelectTerm& is_term) const;

    template<class RankSelectTerm>
    size_t state_to_dict_rank(size_t state, const RankSelectTerm& is_term) const;
    template<class RankSelectTerm>
    size_t dict_rank_to_state(size_t rank, const RankSelectTerm& is_term) const;

	template<class OP>
	void for_each_move(size_t parent, OP op) const {
		assert(parent < m_is_link.size());
		size_t bitpos = m_louds.select0(parent);
		size_t child0 = bitpos - parent;
		assert(m_louds.rank0(bitpos) == parent);
		assert(m_louds.is0(bitpos));
		size_t lcount = m_louds.one_seq_len(bitpos+1);
		assert(lcount <= 256);
		assert(child0 + lcount <= m_is_link.size());
		if (FastLabel) {
			if (lcount < 36) {
				const byte_t* mylabel = m_label_data + child0;
				for(size_t i = 0; i < lcount; ++i) {
					size_t child = child0 + i;
					op(child, mylabel[i]);
				}
			} else {
				const byte_t* mylabel_bm = m_label_data + child0 + 4;
				size_t child = child0;
				for(size_t i = 0; i < 4; ++i) {
					size_t w = unaligned_load<size_t>(mylabel_bm + i*8);
					size_t ch = i * 64;
					for (; w; child++) {
						int ctz = fast_ctz(w);
						ch += ctz;
						assert(ch < 256);
						op(child, byte_t(ch));
						ch++;
						w >>= ctz; // must not be w >>= ctz + 1
						w >>= 1;   // because ctz + 1 may be bits of w(32 or 64)
					}
				}
				assert(child - child0 == lcount);
			}
		}
		else {
			for(size_t i = 0; i < lcount; ++i) {
				size_t child = child0 + i;
				op(child, label_first_byte(child));
			}
		}
	}

	template<class OP>
	void for_each_dest(size_t parent, OP op) const {
		assert(parent < m_is_link.size());
		size_t bitpos = m_louds.select0(parent);
		size_t child0 = bitpos - parent;
		assert(m_louds.rank0(bitpos) == parent);
		assert(m_louds.is0(bitpos));
		size_t lcount = m_louds.one_seq_len(bitpos+1);
		assert(lcount <= 256);
		assert(child0 + lcount <= m_is_link.size());
		for(size_t i = 0; i < lcount; ++i) {
			op(child0 + i);
		}
	}

	template<class OP>
	void for_each_dest_rev(size_t parent, OP op) const {
		assert(parent < m_is_link.size());
		size_t bitpos = m_louds.select0(parent);
		size_t child0 = bitpos - parent;
		assert(m_louds.rank0(bitpos) == parent);
		assert(m_louds.is0(bitpos));
		size_t lcount = m_louds.one_seq_len(bitpos+1);
		assert(lcount <= 256);
		assert(child0 + lcount <= m_is_link.size());
		for(size_t i = lcount; i > 0; ) {
			--i;
			op(child0 + i);
		}
	}

	// DAWG functions:
	template<class IsTermRS>
	void dawg_nth_word(MatchContext&, size_t, std::string*, const IsTermRS&) const {
		THROW_STD(invalid_argument,	"This function is stub for 0 != ctx.root");
	}
	template<class IsTermRS>
	void dawg_nth_word(MatchContext&, size_t, valvec<byte_t>*, const IsTermRS&) const {
		THROW_STD(invalid_argument,	"This function is stub for 0 != ctx.root");
	}
	template<class OnMatch, class TR, class IsTermRS>
	size_t tpl_match_dawg
(MatchContext&, size_t base_nth, fstring, const OnMatch&, TR, const IsTermRS& isTerm)
	const {
		THROW_STD(invalid_argument, "This function is stub for 0 != ctx.root");
	}
	// End DAWG functions
};

typedef NestLoudsTrieTpl<rank_select_se_256> NestLoudsTrie_SE_256, NestLoudsTrie_SE;
typedef NestLoudsTrieTpl<rank_select_il_256> NestLoudsTrie_IL_256, NestLoudsTrie_IL;
typedef NestLoudsTrieTpl<rank_select_se_512> NestLoudsTrie_SE_512;
typedef NestLoudsTrieTpl<rank_select_se_512_64> NestLoudsTrie_SE_512_64;

typedef NestLoudsTrieTpl<rank_select_se_512, rank_select_mixed_se_512_0> NestLoudsTrie_Mixed_SE_512;
typedef NestLoudsTrieTpl<rank_select_il_256, rank_select_mixed_il_256_0> NestLoudsTrie_Mixed_IL_256;
typedef NestLoudsTrieTpl<rank_select_il_256, rank_select_mixed_xl_256_0> NestLoudsTrie_Mixed_XL_256;

// FastLabel = true
typedef NestLoudsTrieTpl<rank_select_se_256_32, rank_select_se_256_32, true> NestLoudsTrie_SE_256_32_FL;
typedef NestLoudsTrieTpl<rank_select_il_256_32, rank_select_il_256_32, true> NestLoudsTrie_IL_256_32_FL;
typedef NestLoudsTrieTpl<rank_select_se_512_32, rank_select_se_512_32, true> NestLoudsTrie_SE_512_32_FL;
typedef NestLoudsTrieTpl<rank_select_se_512_64, rank_select_se_512_64, true> NestLoudsTrie_SE_512_64_FL;

typedef NestLoudsTrieTpl<rank_select_se_512_32, rank_select_mixed_se_512_0, true> NestLoudsTrie_Mixed_SE_512_32_FL;
typedef NestLoudsTrieTpl<rank_select_il_256_32, rank_select_mixed_il_256_0, true> NestLoudsTrie_Mixed_IL_256_32_FL;
typedef NestLoudsTrieTpl<rank_select_il_256_32, rank_select_mixed_xl_256_0, true> NestLoudsTrie_Mixed_XL_256_32_FL;

typedef NestLoudsTrieTpl<rank_select_il_256_32_41, rank_select_mixed_il_256_0, true> NestLoudsTrie_Mixed_IL_256_32_41_FL;
typedef NestLoudsTrieTpl<rank_select_il_256_32_41, rank_select_mixed_xl_256_0, true> NestLoudsTrie_Mixed_XL_256_32_41_FL;

} // namespace terark

#endif // __terark_automata_nest_louds_trie_hpp__

