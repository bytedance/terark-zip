#include <cstring>
#include "nest_trie_dawg.hpp"
#include "fsa_cache_detail.hpp"
#include "nest_louds_trie_inline.hpp"
#include "dfa_mmap_header.hpp"
#include "tmplinst.hpp"

namespace terark {

template<class NestTrie, class DawgType>
NestTrieDAWG<NestTrie, DawgType>::~NestTrieDAWG() {
	if (this->mmap_base) {
		IsTermRep::risk_release_ownership();
	}
	delete m_trie;
	delete m_cache;
}

template<class NestTrie, class DawgType>
NestTrieDAWG<NestTrie, DawgType>::NestTrieDAWG() {
	this->m_is_dag = true;
	this->m_dyn_sigma = 256;
	this->m_cache = NULL;
	m_trie = NULL;
	m_zpNestLevel = 0;
}

template<class NestTrie, class DawgType>
NestTrieDAWG<NestTrie, DawgType>::
NestTrieDAWG(const NestTrieDAWG& y)
: MatchingDFA(y)
, DawgType(y)
, IsTermRep(y)
, m_zpNestLevel(y.m_zpNestLevel)
, m_cache(NULL) // don't copy
{
    assert(256 == y.m_dyn_sigma);
	if (y.m_trie)
		m_trie = new NestTrie(*y.m_trie);
    else
        m_trie = NULL;
}

template<class NestTrie, class DawgType>
NestTrieDAWG<NestTrie, DawgType>&
NestTrieDAWG<NestTrie, DawgType>::
operator=(const NestTrieDAWG& y) {
	if (&y != this) {
		this->~NestTrieDAWG();
		new(this)NestTrieDAWG(); // default cons
		NestTrieDAWG(y).swap(*this);
	}
	return *this;
}

template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::swap(NestTrieDAWG& y) {
	BaseDFA::risk_swap(y);
	std::swap(n_words, y.n_words);
	IsTermRep::swap(y);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
state_move_slow(size_t parent, auchar_t ch, StateMoveContext& ctx) const {
	return m_trie->state_move_slow(parent, ch, ctx);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
state_move_fast(size_t parent, auchar_t ch, size_t n_children, size_t child0) const {
	return m_trie->state_move_fast(parent, ch, n_children, child0);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::v_num_children(size_t parent) const {
	return m_trie->num_children(parent);
}

template<class NestTrie, class DawgType>
bool NestTrieDAWG<NestTrie, DawgType>::v_has_children(size_t parent) const {
	return m_trie->has_children(parent);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::v_gnode_states() const {
	return m_trie->gnode_states();
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::zp_nest_level() const {
	return this->m_zpNestLevel;
}

template<class NestTrie, class DawgType>
bool NestTrieDAWG<NestTrie, DawgType>::has_freelist() const {
	return false;
}

template<class NestTrie, class DawgType>
size_t
NestTrieDAWG<NestTrie, DawgType>::
state_move(size_t curr, auchar_t ch) const {
	return m_trie->state_move(curr, ch);
}

template<class NestTrie, class DawgType>
fstring
NestTrieDAWG<NestTrie, DawgType>::
get_zpath_data(size_t s, MatchContext* ctx) const {
    return m_trie->get_zpath_data(s, ctx);
}

template<class NestTrie, class DawgType>
size_t
NestTrieDAWG<NestTrie, DawgType>::mem_size() const {
	return IsTermRep::mem_size();
}


template<class NestTrie, class DawgType>
//terark_flatten
size_t NestTrieDAWG<NestTrie, DawgType>::
index(MatchContext& ctx, fstring str) const {
    assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
    if (m_trie->m_is_link.max_rank1() > 0)
        return index_impl<true>(ctx, str);
    else
        return index_impl<false>(ctx, str);
}

template<class NestTrie, class DawgType>
//terark_flatten
size_t
NestTrieDAWG<NestTrie, DawgType>::index(fstring str) const {
    assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
    if (m_trie->m_is_link.max_rank1() > 0)
        return index_impl<true>(str);
    else
        return index_impl<false>(str);
}

template<class NestTrie, class DawgType>
template<bool HasLink>
terark_flatten
size_t NestTrieDAWG<NestTrie, DawgType>::
index_impl(fstring str) const {
	assert(HasLink == (m_trie->m_is_link.max_rank1() > 0));
	auto trie = m_trie;
    size_t curr = initial_state;
	size_t i = 0;
	auto loudsBits = trie->m_louds.bldata();
	auto loudsSel0 = trie->m_louds.get_sel0_cache();
	auto loudsRank = trie->m_louds.get_rank_cache();
	auto labelData = trie->m_label_data;
	if (terark_unlikely(NULL != m_cache)) {
		auto da = m_cache->get_double_array();
		auto zpBase = m_cache->get_zpath_data_base();
		while (true) {
			if (HasLink) {
				size_t offset0 = da[curr + 0].m_zp_offset;
				size_t offset1 = da[curr + 1].m_zp_offset;
				if (offset0 < offset1) {
					const byte_t* zs = zpBase + offset0;
					const size_t  zn = offset1 - offset0;
                    _mm_prefetch((const char*)zs, _MM_HINT_T0);
					if (i + zn > str.size())
						return null_word;
					const byte_t* zk = (const byte_t*)(str.p + i);
					size_t j = 0;
					do { // prefer do .. while for performance
						if (zk[j] != zs[j])
							return null_word;
					} while (++j < zn);
					i += zn;
				}
			}
			if (terark_unlikely(str.size() == i)) {
				if (da[curr].m_is_term) {
					size_t map_state = da[curr].m_map_state;
					assert(this->is_term2(trie, map_state));
					return this->term_rank1(trie, map_state);
				}
				return null_word;
			}
			size_t child = da[curr].m_child0 + byte_t(str[i]);
			assert(child < m_cache->total_states());
			if (terark_likely(da[child].m_parent == curr)) {
				curr = child;
				i++;
			}
			else {
				size_t map_state = da[curr].m_map_state;
				assert(map_state < trie->total_states());
				byte_t ch = (byte_t)str.p[i];
                if (HasLink)
                    curr = trie->state_move_fast2(map_state, ch, labelData, loudsBits, loudsSel0, loudsRank);
                else
		            curr = trie->template state_move_smart<HasLink>(map_state, ch);
				if (terark_likely(nil_state != curr)) {
					i++;
					break;
				}
				return null_word;
			}
		}
	}
	for (; nil_state != curr; ++i) {
		if (HasLink && trie->is_pzip(curr)) {
			const byte_t* zk = (const byte_t*)(str.p + i);
			intptr_t matchLen = trie->matchZpath(curr, zk, str.n - i);
			if (matchLen <= 0)
				return null_word;
			i += matchLen;
		}
		assert(i <= str.size());
		if (terark_unlikely(str.size() == i)) {
			if (this->is_term2(trie, curr))
				return this->term_rank1(trie, curr);
			else
				return null_word;
		}
		byte_t ch = (byte_t)str.p[i];
        if (HasLink)
            curr = trie->state_move_fast2(curr, ch, labelData, loudsBits, loudsSel0, loudsRank);
        else
		    curr = trie->template state_move_smart<HasLink>(curr, ch);
	}
	return null_word;
}

template<class NestTrie, class DawgType>
template<bool HasLink>
terark_flatten
size_t NestTrieDAWG<NestTrie, DawgType>::
index_impl(MatchContext& ctx, fstring str) const {
	assert(HasLink == (m_trie->m_is_link.max_rank1() > 0));
//	assert(0 == ctx.pos);
//	assert(0 == ctx.zidx);
	size_t curr = ctx.root;
	if (0 == (curr | ctx.pos | ctx.zidx)) {
        return index_impl<HasLink>(str);
	}
	else {
    	auto trie = m_trie;
		size_t i = ctx.pos;
		size_t j = ctx.zidx;
		for (; nil_state != curr; ++i) {
			if (HasLink && trie->is_pzip(curr)) {
				fstring zstr = trie->get_zpath_data(curr, &ctx);
				const byte_t* zs = zstr.udata();
				const size_t  zn = zstr.size();
#if !defined(NDEBUG)
				assert(zn > 0);
				if (!trie->is_fast_label) {
					assert(0==i || zs[j-1] == byte_t(str.p[i-1]));
				}
#endif
				if (i + zn - j > str.size())
					return null_word;
				// now j may not be 0, so must not use do ... while
				for (; j < zn; ++i, ++j) {
					if (byte_t(str.p[i]) != zs[j])
						return null_word;
				}
				j = 0;
			}
			assert(i <= str.size());
			if (terark_unlikely(str.size() == i)) {
				if (this->is_term2(trie, curr))
					return this->term_rank1(trie, curr);
				else
					return null_word;
			}
			byte_t ch = (byte_t)str.p[i];
			curr = trie->template state_move_smart<HasLink>(curr, ch);
		}
    	return null_word;
	}
}

template<class NestTrie, class DawgType>
//terark_flatten
void NestTrieDAWG<NestTrie, DawgType>::
lower_bound(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank) const {
    assert(index || dict_rank);
    m_trie->lower_bound(ctx, word, index, dict_rank, m_cache, getIsTerm());
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
index_begin() const {
    size_t node_id = m_trie->state_begin(getIsTerm());
    return node_id != size_t(-1) ? getIsTerm().rank1(node_id) : size_t(-1);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
index_end() const {
    size_t node_id = m_trie->state_end(getIsTerm());
    return node_id != size_t(-1) ? getIsTerm().rank1(node_id) : size_t(-1);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
index_next(size_t nth) const {
    assert(nth < this->n_words);
    size_t node_id = getIsTerm().select1(nth);
    assert(node_id < getIsTerm().size());
    assert(getIsTerm()[node_id]);
    node_id = m_trie->state_next(node_id, getIsTerm());
    return node_id != size_t(-1) ? getIsTerm().rank1(node_id) : size_t(-1);
}

template<class NestTrie, class DawgType>
size_t NestTrieDAWG<NestTrie, DawgType>::
index_prev(size_t nth) const {
    assert(nth < this->n_words);
    size_t node_id = getIsTerm().select1(nth);
    assert(node_id < getIsTerm().size());
    assert(getIsTerm()[node_id]);
    node_id = m_trie->state_prev(node_id, getIsTerm());
    return node_id != size_t(-1) ? getIsTerm().rank1(node_id) : size_t(-1);
}

template<class NestTrie, class DawgType>
DawgIndexIter NestTrieDAWG<NestTrie, DawgType>::
dawg_lower_bound(MatchContext& ctx, fstring qry) const {
	assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
	THROW_STD(invalid_argument, "Not supported");
/** TODO:
	if (0 == ctx.root) {
		assert(0 == ctx.pos);
		assert(0 == ctx.zidx);
		size_t curr = 0;
		for (size_t i = 0; nil_state != curr; ++i) {
			if (m_trie->is_pzip(curr)) {
				fstring zs = this->get_zpath_data(curr, &ctx);
				size_t j = ctx.zidx;
				assert(j <= zs.size());
				assert(zs.size() > 0);
				size_t n = std::min(zs.size(), qry.size() - i);
				for (; j < n; ++i, ++j) {
					if (qry[i] != zs[j])
						return {qry[i] < zs[j] ? idx : idx + 1, i, false};
				}
			}
			assert(i <= qry.size());
			if (qry.size() == i) {
				return {idx, i, s.is_term()};
				return m_is_term[curr] ? m_is_term.rank1(curr) : null_word;
			}
			curr = this->state_move(curr, (byte_t)qry.p[i]);
		}
		return null_word;
	}
	return m_trie->dawg_index(ctx, qry);
 */
}

template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::
nth_word(MatchContext& ctx, size_t nth, std::string* word) const {
	assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
	if (terark_likely(0 == ctx.root)) {
		assert(0 == ctx.pos);
		assert(0 == ctx.zidx);
		assert(getIsTerm().max_rank1() == this->n_words);
		assert(nth < this->n_words);
		size_t node_id = getIsTerm().select1(nth);
		assert(node_id < getIsTerm().size());
		assert(getIsTerm()[node_id]);
		m_trie->restore_dawg_string(node_id, word);
	}
	else {
		m_trie->dawg_nth_word(ctx, nth, word, getIsTerm());
	}
}

template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::
nth_word(MatchContext& ctx, size_t nth, valvec<byte_t>* word) const {
	assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
	if (terark_likely(0 == ctx.root)) {
		assert(0 == ctx.pos);
		assert(0 == ctx.zidx);
		assert(getIsTerm().max_rank1() == this->n_words);
		assert(nth < this->n_words);
		size_t node_id = getIsTerm().select1(nth);
		assert(node_id < getIsTerm().size());
		assert(getIsTerm()[node_id]);
		m_trie->restore_dawg_string(node_id, word);
	}
	else {
		m_trie->dawg_nth_word(ctx, nth, word, getIsTerm());
	}
}

template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::
nth_word(size_t nth, std::string* word) const {
	assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
	assert(getIsTerm().max_rank1() == this->n_words);
	assert(nth < this->n_words);
	size_t node_id = getIsTerm().select1(nth);
	assert(node_id < getIsTerm().size());
	assert(getIsTerm()[node_id]);
	m_trie->restore_dawg_string(node_id, word);
}

template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::
nth_word(size_t nth, valvec<byte_t>* word) const {
	assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
	assert(getIsTerm().max_rank1() == this->n_words);
	assert(nth < this->n_words);
	size_t node_id = getIsTerm().select1(nth);
	assert(node_id < getIsTerm().size());
	assert(getIsTerm()[node_id]);
	m_trie->restore_dawg_string(node_id, word);
}

// may combine many NestTrieDAWG's random keys then sort
template<class NestTrie, class DawgType>
void NestTrieDAWG<NestTrie, DawgType>::
get_random_keys_append(SortableStrVec* keys, size_t max_keys) const {
    assert(nullptr != keys);
    assert(m_trie->m_is_link.max_rank1() == this->m_zpath_states);
    assert(getIsTerm().max_rank1() == this->n_words);
    size_t nWords = this->n_words;
    size_t seed = keys->size() + keys->str_size() + max_keys;
    std::mt19937_64 rnd(seed);
    for(size_t i = 0; i < max_keys; ++i) {
        size_t k = rnd() % nWords;
        size_t node_id = getIsTerm().select1(k);
        assert(node_id < getIsTerm().size());
        assert(getIsTerm()[node_id]);
        size_t offset0 = keys->m_strpool.size();
        m_trie->restore_dawg_string_append(node_id, &keys->m_strpool);
        size_t offset1 = keys->m_strpool.size();
        SortableStrVec::SEntry en;
        en.offset = offset0;
        en.length = offset1 - offset0;
        en.seq_id = k; // seq_id is word id
        keys->m_index.push_back(en);
    }
}

template<class NestTrie, class DawgType>
size_t
NestTrieDAWG<NestTrie, DawgType>::v_state_to_word_id(size_t state) const {
	assert(state < m_trie->total_states());
	return getIsTerm().rank1(state);
}

template<class NestTrie, class DawgType>
size_t
NestTrieDAWG<NestTrie, DawgType>::state_to_dict_rank(size_t state) const {
    return m_trie->state_to_dict_rank(state, getIsTerm());
}

template<class NestTrie, class DawgType>
size_t
NestTrieDAWG<NestTrie, DawgType>::dict_rank_to_state(size_t rank) const {
    return m_trie->dict_rank_to_state(rank, getIsTerm());
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::finish_load_mmap(const DFA_MmapHeader* base) {
	if (m_trie) {
		THROW_STD(invalid_argument, "m_trie is not NULL");
	}
	byte_t* bbase = (byte_t*)base;
	m_trie = new NestTrie();
	size_t i = 0;
	if (!NestTrie::is_link_rs_mixed::value) {
		i = 1;
		getIsTerm().risk_mmap_from(bbase + base->blocks[0].offset, base->blocks[0].length);
		assert(2 == base->num_blocks);
	}
	else {
		assert(1 == base->num_blocks);
	}
	m_trie->load_mmap(bbase + base->blocks[i].offset, base->blocks[i].length);
    m_trie->init_for_term(getIsTerm());
    m_trie->m_max_strlen = base->atom_dfa_num;
    if (m_trie->m_max_strlen == 0) { // always 0 for old NLT File
        // will over allocate memory for Iterator
        m_trie->m_max_strlen = (m_trie->m_layer_id.size() + 1) * 256;
    }
	assert(getIsTerm().size() == m_trie->total_states());
	this->n_words = size_t(base->dawg_num_words);
	this->m_zpNestLevel = m_trie->nest_level();
	assert(m_trie->m_is_link.max_rank1() == base->zpath_states);
}

template<class NestTrie, class DawgType>
bool
NestTrieDAWG<NestTrie, DawgType>::has_fsa_cache() const {
	return NULL != m_cache;
}

template<class NestTrie, class DawgType>
bool
NestTrieDAWG<NestTrie, DawgType>::
build_fsa_cache(double cacheRatio, const char* walkMethod) {
	if (cacheRatio > 1e-8) {
		size_t cacheStates = size_t(m_trie->total_states() * cacheRatio);
		if (cacheStates > 100) {
			m_cache = new NTD_CacheTrie(this, cacheStates, walkMethod);
			return true;
		}
	}
	return false;
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::print_fsa_cache_stat(FILE* fp) const {
	if (m_cache) {
		m_cache->print_cache_stat(fp);
	}
	else {
		fprintf(fp, "No FSA_Cache\n");
	}
}

template<class NestTrie, class DawgType>
long
NestTrieDAWG<NestTrie, DawgType>::
prepare_save_mmap(DFA_MmapHeader* base, const void** dataPtrs) const {
	if (NULL == m_trie) {
		THROW_STD(invalid_argument, "m_trie is NULL");
	}
	base->is_dag = true;
	base->dawg_num_words = this->n_words;
	base->transition_num = total_states() - 1;
	base->adfa_total_words_len = this->m_adfa_total_words_len;

	long need_free_mask = 0;
	size_t blockIndex = 0;
	size_t blockOffset = sizeof(DFA_MmapHeader);
	if (!NestTrie::is_link_rs_mixed::value) {
		dataPtrs[0] = getIsTerm().bldata();
		base->blocks[0].offset = sizeof(DFA_MmapHeader);
		base->blocks[0].length = getIsTerm().mem_size();
		base->num_blocks = 2;
		blockIndex = 1;
		blockOffset = align_to_64(base->blocks[0].endpos());
		need_free_mask = long(1) << 1;
	}
	else {
		base->num_blocks = 1;
		need_free_mask = long(1) << 0;
	}
	size_t trie_mem_size = 0;
	byte_t const* tmpbuf = m_trie->save_mmap(&trie_mem_size);
	dataPtrs[blockIndex] = tmpbuf;
	base->blocks[blockIndex].offset = blockOffset;
	base->blocks[blockIndex].length = trie_mem_size;
	base->louds_dfa_num_zpath_trie = m_trie->nest_level();
    base->atom_dfa_num = m_trie->m_max_strlen;

	return need_free_mask;
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::str_stat(std::string* s) const {
	m_trie->str_stat(s);
	s->append("--------------\n");
	char buf[128];
	sprintf(buf, "adfa_total_words_len = %lld\n", (llong)m_adfa_total_words_len);
	s->append(buf);
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_from(SortableStrVec& strVec, const NestLoudsTrieConfig& conf) {
    build_from_tpl(strVec, conf);
}
template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_from(FixedLenStrVec& strVec, const NestLoudsTrieConfig& conf) {
    build_from_tpl(strVec, conf);
}
template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_from(SortedStrVec& strVec, const NestLoudsTrieConfig& conf) {
    build_from_tpl(strVec, conf);
}
template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_from(ZoSortedStrVec& strVec, const NestLoudsTrieConfig& conf) {
    build_from_tpl(strVec, conf);
}

template<class NestTrie, class DawgType>
template<class StrVecType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_from_tpl(StrVecType& strVec, const NestLoudsTrieConfig& conf) {
	if (conf.nestLevel < 1) {
		THROW_STD(invalid_argument, "conf.nestLevel=%d", conf.nestLevel);
	}
	if (m_trie) {
		THROW_STD(invalid_argument, "m_trie is not NULL");
	}
	this->m_adfa_total_words_len = strVec.str_size();
#if !defined(NDEBUG)
	size_t strVecSize = strVec.size();
#endif
	m_trie = new NestTrie();
    using namespace std::placeholders;
	auto buildTerm = std::bind(&NestTrieDAWG::build_term_bits, this, _1);
	m_trie->build_patricia(strVec, buildTerm, conf);
    m_trie->init_for_term(getIsTerm());
	this->m_zpath_states = m_trie->num_zpath_states();
	this->m_total_zpath_len = m_trie->total_zpath_len();
	this->n_words = getIsTerm().max_rank1();
	this->m_zpNestLevel = conf.nestLevel;
	// strVec may have duplicates, so assert <=
	assert(getIsTerm().max_rank1() <= strVecSize);
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::build_term_bits(const valvec<index_t>& linkVec) {
	auto& termFlag = this->getIsTerm();
	termFlag.resize_fill(this->m_trie->m_is_link.size(), 0);
	for(size_t node_id : linkVec) {
		assert(node_id < termFlag.size());
		termFlag.set1(node_id);
	}
	termFlag.build_cache(0, 1);
}

template<class NestTrie, class DawgType>
void
NestTrieDAWG<NestTrie, DawgType>::
build_with_id(SortableStrVec& strVec, valvec<index_t>& idvec, const NestLoudsTrieConfig& conf) {
	if (conf.nestLevel < 1) {
		THROW_STD(invalid_argument, "conf.nestLevel=%d", conf.nestLevel);
	}
	if (m_trie) {
		THROW_STD(invalid_argument, "m_trie is not NULL");
	}
#if !defined(NDEBUG)
	size_t strVecSize = strVec.size();
#endif
	m_trie = new NestTrie();
	m_trie->build_strpool2(strVec, idvec, conf);
	assert(idvec.size() == strVecSize);
	getIsTerm().resize(m_trie->total_states());
	for (size_t i = 0; i < idvec.size(); ++i) {
		getIsTerm().set1(idvec[i]);
	}
	getIsTerm().build_cache(false, true);
	this->m_zpath_states = m_trie->num_zpath_states();
	this->m_total_zpath_len = m_trie->total_zpath_len();
	this->n_words = getIsTerm().max_rank1();
	this->m_zpNestLevel = conf.nestLevel;
	assert(getIsTerm().max_rank1() <= strVecSize); // can be dup

	for (size_t i = 0; i < idvec.size(); ++i) {
		idvec[i] = getIsTerm().rank1(idvec[i]);
	}
}

#if 1
template<class NestTrie, class DawgType>
ADFA_LexIterator*
NestTrieDAWG<NestTrie, DawgType>::adfa_make_iter(size_t root) const {
    assert(NULL != m_trie);
    return new Iterator(this);
}
template<class NestTrie, class DawgType>
ADFA_LexIterator16*
NestTrieDAWG<NestTrie, DawgType>::adfa_make_iter16(size_t root) const {
//    return new Iterator(this);
    THROW_STD(logic_error, "Not supported");
}
#endif

template class NestTrieDAWG<NestLoudsTrie_SE, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_IL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_SE_512, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_SE_512_64, BaseDAWG>;

template class NestTrieDAWG<NestLoudsTrie_Mixed_SE_512, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_Mixed_IL_256, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_Mixed_XL_256, BaseDAWG>;

///@{ FastLabel = true
template class NestTrieDAWG<NestLoudsTrie_SE_256_32_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_SE_512_32_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_IL_256_32_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_SE_512_64_FL, BaseDAWG>;

template class NestTrieDAWG<NestLoudsTrie_Mixed_SE_512_32_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_Mixed_IL_256_32_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_Mixed_XL_256_32_FL, BaseDAWG>;

template class NestTrieDAWG<NestLoudsTrie_Mixed_IL_256_32_41_FL, BaseDAWG>;
template class NestTrieDAWG<NestLoudsTrie_Mixed_XL_256_32_41_FL, BaseDAWG>;
///@}

// for compatible with old mmap file format
typedef NestLoudsTrieDAWG_SE_256 NestLoudsTrieDAWG_SE;
typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL;

TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_IL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_512)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_512_64)

//TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_256)
//TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_IL_256)

TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_SE_512)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_IL_256)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_XL_256)

///@{ FastLabel = true
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_256_32_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_IL_256_32_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_512_32_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_SE_512_64_FL)

TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_SE_512_32_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_IL_256_32_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_XL_256_32_FL)

TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_IL_256_32_41_FL)
TMPL_INST_DFA_CLASS(NestLoudsTrieDAWG_Mixed_XL_256_32_41_FL)
///@}

} // namespace terark
