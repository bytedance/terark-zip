#pragma once
#include "nest_louds_trie.hpp"
#include "fast_search_byte.hpp"
#include "fsa_cache_detail.hpp"

namespace terark {

template<class NLTrie>
inline byte_t
NestLoudsTrie_label_first_byte_nested(const NLTrie* trie, size_t node_id)
{
	assert(NULL != trie);
//  trie->m_is_link.prefetch_rank1(node_id);
    _mm_prefetch((const char*)(&trie->m_label_data[node_id]), _MM_HINT_T0);
	while (trie->m_is_link[node_id]) {
		size_t linkRank1 = trie->m_is_link.rank1(node_id);
		size_t link_hig = trie->m_next_link[linkRank1];
		size_t link_low = trie->m_label_data[node_id];
		uint64_t linkVal = uint64_t(link_hig) << 8 | link_low;
		if (linkVal >= trie->m_core_max_link_val) {
			node_id = size_t(linkVal - trie->m_core_max_link_val);
			trie = trie->m_next_trie;
		}
		else {
			size_t offset = size_t(linkVal >> trie->m_core_len_bits);
#if !defined(NDEBUG)
			size_t length = size_t(linkVal &  trie->m_core_len_mask);
			assert(offset + length <= trie->m_core_size);
#endif
			return trie->m_core_data[offset];
		}
	}
	assert(NULL != trie);
	return trie->m_label_data[node_id];
}

template<class RankSelect, class RankSelect2, bool FastLabel>
byte_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::label_first_byte(size_t node_id)
const {
	assert(!FastLabel);
	assert(node_id > 0);
	assert(node_id < m_is_link.size());
//  m_is_link.prefetch_rank1(node_id);
//  _mm_prefetch((const char*)(&m_label_data[node_id]), _MM_HINT_T0);
	if (m_is_link[node_id]) {
		size_t linkRank1 = m_is_link.rank1(node_id);
		size_t link_hig = m_next_link[linkRank1];
		size_t link_low = m_label_data[node_id];
		uint64_t linkVal = uint64_t(link_hig) << 8 | link_low;
		if (linkVal >= m_core_max_link_val) {
			size_t next_id = size_t(linkVal - m_core_max_link_val);
			return NestLoudsTrie_label_first_byte_nested(m_next_trie, next_id);
		}
		else {
			size_t offset = size_t(linkVal >> m_core_len_bits);
#if !defined(NDEBUG)
			size_t length = size_t(linkVal &  m_core_len_mask);
			assert(offset + length <= m_core_size);
#endif
			return m_core_data[offset];
		}
	}
	return m_label_data[node_id];
}

template<class RankSelect, class RankSelect2, bool FastLabel>
terark_flatten
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move(size_t parent, auchar_t ch)
const {
    assert(ch < 256);
    assert(parent < total_states());
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
	size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
    m_is_link.prefetch_bit(child0); // prefetch for next search
	assert(m_louds.rank0(bitpos) == parent);
	assert(m_louds.is0(bitpos));
	size_t lcount = m_louds.one_seq_len(bitpos+1);
	return state_move_fast(parent, ch, lcount, child0);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
std::pair<size_t, bool>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_lower_bound(size_t parent, auchar_t ch)
const {
    assert(ch < 256);
    assert(parent < total_states());
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
    size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
    assert(m_louds.rank0(bitpos) == parent);
    assert(m_louds.is0(bitpos));
    size_t lcount = m_louds.one_seq_len(bitpos + 1);
    if (!lcount)
        return { +nil_state, false };
    assert(child0 + lcount <= total_states());
    if (FastLabel) {
        if (lcount < 36) {
            size_t i = lower_bound_0(m_label_data + child0, lcount, byte_t(ch));
            bool ok = i < lcount && m_label_data[child0 + i] == ch;
            return { child0 + i, ok };
        }
        else {
            const byte_t* data = m_label_data + child0;
            size_t word_index = byte_t(ch) / TERARK_WORD_BITS;
            size_t bit_pos = byte_t(ch) % TERARK_WORD_BITS;
            size_t word = unaligned_load<size_t>(data + 4 + word_index * sizeof(size_t));
            size_t offset = fast_popcount_trail(word, bit_pos);
            size_t i = data[word_index] + offset;
            bool ok = ((word >> bit_pos) & 1) != 0;
            return { child0 + i, ok };
        }
    }
    else {
        size_t i = child0, j = child0 + lcount;
        size_t mid;
        auchar_t mid_ch;
        while (i < j) {
            mid = (i + j) / 2;
            mid_ch = label_first_byte(mid);
            if (mid_ch < ch)
                i = mid + 1;
            else
                j = mid;
        }
        if (i < child0 + lcount) {
            if (i != mid) {
                mid_ch = label_first_byte(i);
            }
            return { i, (ch == mid_ch) };
        }
        return { i, false };
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_fast(size_t parent, auchar_t ch, size_t lcount, size_t child0)
const {
	assert(ch < 256);
	assert(parent < total_states());
//	assert(lcount > 0); // lcount == 0 is valid
	assert(child0 + lcount <= total_states());
	if (FastLabel) {
        const byte_t* label = m_label_data + child0;
        if (lcount < 36) {
            if (true/* && lcount <= 16*/) {
                if (lcount && ch <= label[lcount-1]) {
                    size_t i = size_t(-1);
                    do i++; while (label[i] < ch);
                    if (label[i] == ch)
                        return child0 + i;
                }
            }
            else {
                //size_t i = fast_search_byte_max_35(label, lcount, ch);
                size_t i = lower_bound_0(label, lcount, ch);
                if (i < lcount && label[i] == ch)
                    return child0 + i;
            }
        }
        else {
#if 0
            if (terark_bit_test((size_t*)(label + 4), ch)) {
                size_t i = fast_search_byte_rs_idx(label, byte_t(ch));
                return child0 + i;
            }
#else
            size_t i = ch / TERARK_WORD_BITS;
            size_t j = ch % TERARK_WORD_BITS;
            size_t w = unaligned_load<size_t>(label + 4 + i*sizeof(size_t));
            if ((w >> j) & 1)
                return child0 + label[i] + fast_popcount_trail(w, j);
#endif
        }
        return nil_state;
	}
	else {
		size_t lo = child0;
		size_t hi = child0 + lcount;
		while (lo < hi) {
			size_t mid_id = (lo + hi) / 2;
			auchar_t mid_ch = label_first_byte(mid_id);
			if (mid_ch < ch)
				lo = mid_id + 1;
			else if (ch < mid_ch)
				hi = mid_id;
			else
				// early return, because label_first_byte is slow
				return mid_id;
		}
	}
	return nil_state;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class LoudsBits, class LoudsSel, class LoudsRank>
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_fast2(size_t parent, byte_t ch, const byte_t* label,
                 const LoudsBits* bits, const LoudsSel* sel0, const LoudsRank* rank)
const {
    assert(ch < 256);
    assert(parent < total_states());
#if 0
    size_t bitpos = RankSelect::fast_select0(bits, sel0, rank, parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = RankSelect::fast_select0(bits, sel0, rank, parent);
#endif
    size_t child0 = bitpos - parent;
    label += child0;
    _mm_prefetch((const char*)label, _MM_HINT_T0);
    m_is_link.prefetch_bit(child0); // prefetch for next search
    size_t lcount = RankSelect::fast_one_seq_len(bits, bitpos+1);
    if (terark_unlikely(0 == lcount)) {
        return nil_state;
    }
    assert(child0 + lcount <= total_states());
    if (FastLabel) {
        if (lcount < 36) {
            if (true/* && lcount <= 16*/) {
                if (lcount && ch <= label[lcount-1]) {
                    size_t i = size_t(-1);
                    do i++; while (label[i] < ch);
                    if (label[i] == ch)
                        return child0 + i;
                }
            }
            else {
                //size_t i = fast_search_byte_max_35(label, lcount, ch);
                size_t i = lower_bound_0(label, lcount, ch);
                if (i < lcount && label[i] == ch)
                    return child0 + i;
            }
        }
        else {
#if 0
            if (terark_bit_test((size_t*)(label + 4), ch)) {
                size_t i = fast_search_byte_rs_idx(label, ch);
                return child0 + i;
            }
#else
            size_t i = ch / TERARK_WORD_BITS;
            size_t j = ch % TERARK_WORD_BITS;
            size_t w = unaligned_load<size_t>(label + 4 + i*sizeof(size_t));
            if ((w >> j) & 1)
                return child0 + label[i] + fast_popcount_trail(w, j);
#endif
        }
        return nil_state;
    }
    else {
        if (false/*true*//*lcount > 6*/) {
            // this is slower :(
        #if 0
            size_t rlo = m_is_link.rank1(child0);
            size_t rhi = m_is_link.rank1(child0 + lcount);
            if (rlo == rhi)
        #else
            size_t zseq = m_is_link.zero_seq_len(child0);
            if (zseq == lcount)
        #endif
            {
                if (lcount < 30) {
                    if (lcount && ch <= label[lcount-1]) {
                        size_t i = size_t(-1);
                        do i++; while (label[i] < ch);
                        if (label[i] == ch)
                            return child0 + i;
                    }
                }
                else {
                    size_t i = lower_bound_0(label, lcount, ch);
                    if (i < lcount && label[i] == ch)
                        return child0 + i;
                }
                return nil_state;
            }
        }
        size_t lo = child0;
        size_t hi = child0 + lcount;
        while (lo < hi) {
            size_t mid_id = (lo + hi) / 2;
            byte_t mid_ch = label_first_byte(mid_id);
            if (mid_ch < ch)
                lo = mid_id + 1;
            else if (ch < mid_ch)
                hi = mid_id;
            else
                // early return, because label_first_byte is slow
                return mid_id;
        }
    }
    return nil_state;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
terark_flatten
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_slow(size_t parent, auchar_t ch, StateMoveContext& ctx) const {
    assert(ch < 256);
    assert(parent < total_states());
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
	size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
	assert(m_louds.rank0(bitpos) == parent);
	assert(m_louds.is0(bitpos));
	size_t lcount = m_louds.one_seq_len(bitpos+1);
	if (terark_unlikely(0 == lcount)) {
		ctx.n_children = 0;
		ctx.child0 = size_t(-1);
		return nil_state;
	}
	else {
		ctx.n_children = lcount;
		ctx.child0 = child0;
		return state_move_fast(parent, ch, lcount, child0);
	}
}

template<class RankSelect, class RankSelect2, bool FastLabel>
terark_flatten
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_no_link(size_t parent, auchar_t ch) const {
    assert(!FastLabel);
    assert(ch < 256);
    assert(parent < total_states());
    assert(m_is_link.max_rank1() == 0);
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
    size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
    assert(m_louds.rank0(bitpos) == parent);
    assert(m_louds.is0(bitpos));
    size_t lcount = m_louds.one_seq_len(bitpos + 1);
    if (lcount) {
        const byte_t* mylabel = m_label_data + child0;
		size_t idx;
		idx = fast_search_byte(mylabel, lcount, byte_t(ch));
        if (idx < lcount)
            return child0 + idx;
    }
    return nil_state;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
std::pair<size_t, bool>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_move_lower_bound_no_link(size_t parent, auchar_t ch) const {
    assert(!FastLabel);
    assert(ch < 256);
    assert(parent < total_states());
    assert(m_is_link.max_rank1() == 0);
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
    size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
    assert(m_louds.rank0(bitpos) == parent);
    assert(m_louds.is0(bitpos));
    size_t lcount = m_louds.one_seq_len(bitpos + 1);
    if (lcount) {
        const byte_t* mylabel = m_label_data + child0;
        size_t idx = lower_bound_0(mylabel, lcount, byte_t(ch));
        bool ok = idx < lcount && mylabel[idx] == ch;
        return { child0 + idx, ok };
    }
    return { +nil_state, false };
}



template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
init_for_term(const RankSelectTerm& is_term) {
    index_t pos = 2, id = 0, rank = 0;
    while (true) {
        m_layer_id.emplace_back(id);
        m_layer_rank.emplace_back(rank);
        if ((id = pos - id - 1) >= m_louds.max_rank1())
            break;
        pos = m_louds.select0(id) + 1;
        rank = is_term.rank1(id);
    }
    assert(id == m_louds.max_rank1());
    assert(m_layer_id.size() == m_layer_rank.size());

    index_t layer_max = m_layer_id.size();
    m_layer_ref.resize_no_init(layer_max);
    index_t layer_id = 0, layer_size = 0;
    for (index_t i = 0; i < layer_max; ++i) {
        index_t end_id = i == layer_max - 1
                       ? (index_t)is_term.size() : m_layer_id[i + 1];
        m_layer_ref[i] = layer_ref_t{m_layer_id[i], end_id, 0};
        index_t size = end_id - m_layer_id[i];
        if (size > layer_size) {
            layer_id = i;
            layer_size = size;
        }
    }
    m_max_layer_id = layer_id;
    m_max_layer_size = layer_size;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
lower_bound(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank, const NTD_CacheTrie* cache, const RankSelectTerm& is_term) const {
    if (m_is_link.max_rank1() > 0) {
        if (dict_rank)
            lower_bound_impl<RankSelectTerm, true, true>(ctx, word, index, dict_rank, cache, is_term);
        else
            lower_bound_impl<RankSelectTerm, true, false>(ctx, word, index, dict_rank, cache, is_term);
    }
    else {
        if (dict_rank)
            lower_bound_impl<RankSelectTerm, false, true>(ctx, word, index, dict_rank, cache, is_term);
        else
            lower_bound_impl<RankSelectTerm, false, false>(ctx, word, index, dict_rank, cache, is_term);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm, bool HasLink, bool HasDictRank>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
lower_bound_impl(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank, const NTD_CacheTrie* cache, const RankSelectTerm& is_term) const {
    assert(HasLink == (m_is_link.max_rank1() > 0));
    auto trie = this;
    //	assert(0 == ctx.bit_pos);
    //	assert(0 == ctx.zidx);
    size_t curr = ctx.root;
    size_t i = ctx.pos;
    size_t j = ctx.zidx;
    size_t layer_max = m_layer_id.size();
    size_t layer = 0;
    size_t rank = 0;
    if (curr != initial_state) {
        if (HasDictRank) {
            layer = upper_bound_0(m_layer_id.data(), layer_max, curr) - 1;
            assert(layer < layer_max);
        }
        size_t parent = curr;
        size_t parent_layer = layer;
        do {
            --parent_layer;
            rank += is_term.rank1(parent + 1) - m_layer_rank[parent_layer];
            parent = m_louds.select1(parent) - parent - 1;
        } while (parent != initial_state);
    }
    auto trans_state = [&](size_t state, bool dec) {
        if (index) {
            size_t fixed_state;
            if (state == m_layer_ref[layer].end)
                *index = size_t(-1);
            else if (dec && is_term[state])
                *index = is_term.rank1(state);
            else if ((fixed_state = state_next(state, is_term)) != size_t(-1))
                *index = is_term.rank1(fixed_state);
            else
                *index = size_t(-1);
        }
        if (HasDictRank) {
            assert(dict_rank != nullptr);
            for (++layer; layer < layer_max; ++layer) {
                state = m_louds.select0(state) - state;
                assert(state <= is_term.size());
                if (state == m_layer_id[layer])
                    break;
                assert(state > m_layer_id[layer]);
                rank += is_term.rank1(state) - m_layer_rank[layer];
            }
            assert(rank >= 1 || !dec);
            *dict_rank = rank - dec;
        }
    };
    if (cache && 0 == curr) {
        auto da = cache->get_double_array();
        auto zpBase = cache->get_zpath_data_base();
        while (true) {
            size_t map_state = da[curr].m_map_state;
            if (HasLink) {
                size_t offset0 = da[curr + 0].m_zp_offset;
                size_t offset1 = da[curr + 1].m_zp_offset;
                if (offset0 < offset1) {
                    const byte_t* zs = zpBase + offset0;
                    const size_t  zn = offset1 - offset0;
                    while (i < word.size() && j < zn) {
                        byte_t c1 = byte_t(word.p[i++]);
                        byte_t c2 = zs[j++];
                        if (c1 != c2)
                            return trans_state(map_state, c1 < c2 && is_term.is1(map_state));
                    }
                    if (j < zn)
                        return trans_state(map_state, is_term.is1(map_state));
                    j = 0;
                }
            }
            if (terark_unlikely(word.size() == i)) {
                return trans_state(map_state, is_term.is1(map_state));
            }
            size_t child = da[curr].m_child0 + byte_t(word[i]);
            assert(child < cache->total_states());
            if (terark_likely(da[child].m_parent == curr)) {
                curr = child;
                ++layer;
                if (HasDictRank)
                    rank += is_term.rank1(da[child].m_map_state + 1) - m_layer_rank[layer];
                i++;
            }
            else {
                assert(map_state < trie->total_states());
                byte_t ch = (byte_t)word.p[i];
                auto mr = trie->template state_move_lower_bound_smart<HasLink>(map_state, ch);
                if (terark_likely(mr.second)) {
                    curr = mr.first;
                    ++layer;
                    if (HasDictRank)
                        rank += is_term.rank1(curr + 1) - m_layer_rank[layer];
                    i++;
                    break;
                }
                if (mr.first == nil_state)
                    return trans_state(map_state, false);
                ++layer;
                if (HasDictRank)
                    rank += is_term.rank1(mr.first) - m_layer_rank[layer];
                return trans_state(mr.first, false);
            }
        }
    }
    else {
        if (HasDictRank)
            rank += is_term.rank1(1) - m_layer_rank[0];
    }
    assert(nil_state != curr);
    while(true) {
        if (HasLink && trie->is_pzip(curr)) {
            fstring zstr = trie->get_zpath_data(curr, &ctx);
            const byte_t* zs = zstr.udata();
            const size_t  zn = zstr.size();
#if !defined(NDEBUG)
            assert(zn > 0);
            if (!trie->is_fast_label)
                assert(0 == i || zs[j - 1] == byte_t(word.p[i - 1]));
#endif
            while (i < word.size() && j < zn) {
                byte_t c1 = byte_t(word.p[i++]);
                byte_t c2 = zs[j++];
                if (c1 != c2)
                  return trans_state(curr, c1 < c2 && is_term.is1(curr));
            }
            if (j < zn)
                return trans_state(curr, is_term.is1(curr));
            j = 0;
        }
        assert(i <= word.size());
        if (terark_unlikely(word.size() == i))
            return trans_state(curr, is_term.is1(curr));
        byte_t ch = (byte_t)word.p[i];
        auto mr = trie->template state_move_lower_bound_smart<HasLink>(curr, ch);
        if (terark_unlikely(!mr.second)) {
            if (mr.first == nil_state)
                return trans_state(curr, false);
            ++layer;
            if (HasDictRank)
                rank += is_term.rank1(mr.first) - m_layer_rank[layer];
            return trans_state(mr.first, false);
        }
        curr = mr.first;
        ++layer;
        if (HasDictRank)
            rank += is_term.rank1(mr.first + 1) - m_layer_rank[layer];
        ++i;
    }
    if (index) *index = size_t(-1);
    if (dict_rank) *dict_rank = is_term.max_rank1();
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_begin(const RankSelectTerm& is_term) const {
    if (is_term[0])
        return 0;
    return state_next(0, is_term);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_end(const RankSelectTerm& is_term) const {
    size_t state = 0;
    while (true) {
#if 0
        size_t bitpos = m_louds.select0(state);
#else
        size_t bitpos;
        if (state < m_sel0_cache.size())
            bitpos = m_sel0_cache[state];
        else
            bitpos = m_louds.select0(state);
#endif
        size_t lcount = m_louds.one_seq_len(bitpos + 1);
        if (!lcount) {
            break;
        }
        size_t child0 = bitpos - state;
        assert(m_louds.rank0(bitpos) == state);
        assert(m_louds.is0(bitpos));
        state = child0 + lcount - 1;
    }
    if (is_term[state])
        return state;
    return state_prev(state, is_term);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_next(size_t state, const RankSelectTerm& is_term) const {
    while (true) {
#if 0
        size_t bitpos = m_louds.select0(state);
#else
        size_t bitpos;
        if (state < m_sel0_cache.size())
            bitpos = m_sel0_cache[state];
        else
            bitpos = m_louds.select0(state);
#endif
        assert(bitpos + 1 < m_louds.size());
        if (m_louds[bitpos + 1]) {
            // move to first child
            size_t child0 = bitpos - state;
            assert(m_louds.rank0(bitpos) == state);
            assert(m_louds.is0(bitpos));
            state = child0;
            if (is_term[state])
                return state;
            continue;
        }
        bitpos = m_louds.select1(state);
        assert(bitpos + 1 < m_louds.size());
        if (m_louds[bitpos + 1]) {
            // move to next brother
            ++state;
            if (is_term[state])
                return state;
            continue;
        }
        while (true) {
            // move to parent's next brother(which has next brother)
            size_t parent = bitpos - state;
            assert(m_louds.rank1(bitpos) == state);
            assert(m_louds.is1(bitpos));
            state = parent - 1;
            if (state == 0) {
                return size_t(-1);
            }
            bitpos = m_louds.select1(state);
            assert(bitpos + 1 < m_louds.size());
            if (m_louds[bitpos + 1]) {
                // move to next brother
                ++state;
                if (is_term[state])
                    return state;
                break;
            }
        }
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_prev(size_t state, const RankSelectTerm& is_term) const {
    while (true) {
        if (state == 0)
            return size_t(-1);
        size_t bitpos = m_louds.select1(state);
        assert(bitpos > 0);
        if (m_louds[bitpos - 1]) {
            // move to prev brother
            --state;
#if 0
            bitpos = m_louds.select0(state);
#else
            if (state < m_sel0_cache.size())
                bitpos = m_sel0_cache[state];
            else
                bitpos = m_louds.select0(state);
#endif
            size_t lcount = m_louds.one_seq_len(bitpos + 1);
            while (lcount > 0) {
                size_t child0 = bitpos - state;
                assert(m_louds.rank0(bitpos) == state);
                assert(m_louds.is0(bitpos));
                state = child0 + lcount - 1;
                if (state < m_sel0_cache.size())
                    bitpos = m_sel0_cache[state];
                else
                    bitpos = m_louds.select0(state);
                lcount = m_louds.one_seq_len(bitpos + 1);
            }
            if (is_term[state])
                return state;
            continue;
        }
        size_t parent = bitpos - state;
        assert(m_louds.rank1(bitpos) == state);
        assert(m_louds.is1(bitpos));
        state = parent - 1;
        if (is_term[state])
            return state;
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
state_to_dict_rank(size_t state, const RankSelectTerm& is_term) const {
    assert(state < m_louds.max_rank1());
    size_t layer_max = m_layer_id.size();
    size_t layer = upper_bound_0(m_layer_id.data(), layer_max, state) - 1;
    assert(layer < layer_max);
    size_t parent_rank = 0;
    size_t parent = state;
    size_t parent_layer = layer;
    while (parent != initial_state) {
        parent = m_louds.select1(parent) - parent - 1;
        --parent_layer;
        size_t rank = is_term.rank1(parent + 1);
        parent_rank += rank - m_layer_rank[parent_layer];
    };
    assert(parent_layer == 0);
    size_t child_rank = is_term.rank1(state) - m_layer_rank[layer];
    size_t child = state;
    size_t child_layer = layer + 1;
    while (child_layer < layer_max) {
        child = m_louds.select0(child) - child;
        assert(child <= is_term.size());
        if (child == m_layer_id[child_layer])
            break;
        assert(child > m_layer_id[child_layer]);
        size_t rank = is_term.rank1(child);
        child_rank += rank - m_layer_rank[child_layer];
        ++child_layer;
    }
    return parent_rank + child_rank;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class RankSelectTerm>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
dict_rank_to_state(size_t index, const RankSelectTerm& is_term) const {
    assert(index < m_louds.max_rank1());
    size_t layer_max = m_layer_id.size();
#if 0
    valvec<layer_ref_t> layer = m_layer_ref;
#else
    layer_ref_t* layer = (layer_ref_t*)alloca(m_layer_ref.used_mem_size());
    memcpy(layer, m_layer_ref.data(), m_layer_ref.used_mem_size());
#endif
    size_t layer_id = m_max_layer_id, layer_size = m_max_layer_size;
    while (true) {
        assert(layer[layer_id].beg < layer[layer_id].end);
        size_t state = layer[layer_id].mid = (layer[layer_id].beg + layer[layer_id].end) / 2;
        assert(layer_id == upper_bound_0(m_layer_id.data(), layer_max, state) - 1);
        size_t parent_rank = 0;
        size_t parent = state;
        size_t parent_layer = layer_id;
        while (parent != initial_state) {
            parent = m_louds.select1(parent) - parent - 1;
            --parent_layer;
            layer[parent_layer].mid = parent;
            size_t rank = is_term.rank1(parent + 1);
            parent_rank += rank - m_layer_rank[parent_layer];
        };
        assert(parent_layer == 0);
        size_t child_rank = is_term.rank1(state) - m_layer_rank[layer_id];
        size_t child = state;
        size_t child_layer = layer_id + 1;
        while (child_layer < layer_max) {
            child = m_louds.select0(child) - child;
            layer[child_layer].mid = child;
            assert(child <= is_term.size());
            if (child == m_layer_id[child_layer]) {
                while (++child_layer < layer_max)
                    layer[child_layer].mid = m_layer_id[child_layer];
                break;
            }
            assert(child > m_layer_id[child_layer]);
            size_t rank = is_term.rank1(child);
            child_rank += rank - m_layer_rank[child_layer];
            ++child_layer;
        }
        size_t rank = parent_rank + child_rank;
        if (rank == index && is_term[state]) {
            return state;
        }
        else if (layer_size == 1) {
            if (rank == index) {
                for (size_t i = layer_id + 1; i < layer_max; ++i) {
                    if (layer[i].beg != layer[i].end) {
                        assert(layer[i].beg + 1 == layer[i].end);
                        state = layer[i].beg;
                        if (is_term[state])
                            return state;
                    }
                }
            }
            else {
                for (size_t i = 0; i < layer_max; ++i) {
                    if (layer[i].beg != layer[i].end) {
                        assert(layer[i].beg + 1 == layer[i].end);
                        state = layer[i].beg;
                        if (is_term[state]) {
                            rank = state_to_dict_rank(state, is_term);
                            if (rank == index)
                                return state;
                        }
                    }
                }
            }
            assert(0);
            break;
        }
        size_t next_layer_id = 0;
        layer_size = 0;
        if (index < rank) {
            for (size_t i = 0; i < layer_max; ++i) {
                layer[i].end = layer[i].mid + (i < layer_id);
                size_t size = layer[i].end - layer[i].beg;
                if (size > layer_size)
                {
                    next_layer_id = i;
                    layer_size = size;
                }
            }
        }
        else {
            for (size_t i = 0; i < layer_max; ++i) {
                layer[i].beg = layer[i].mid - (i > layer_id);
                size_t size = layer[i].end - layer[i].beg;
                if (size > layer_size)
                {
                    next_layer_id = i;
                    layer_size = size;
                }
            }
        }
        assert(layer_size > 0);
        layer_id = next_layer_id;
    }
    return size_t(-1);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
inline uint64_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
get_link_val(size_t node_id) const {
	assert(node_id > 0);
	assert(node_id < m_is_link.size());
	size_t linkRank1 = m_is_link.rank1(node_id);
	if (FastLabel) {
		return m_next_link[linkRank1];
	}
	else {
		size_t low_bits = m_label_data[node_id];
		size_t hig_bits = m_next_link[linkRank1];
		return uint64_t(hig_bits) << 8 | low_bits;
	}
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
num_children(size_t parent) const {
	assert(parent < total_states());
	size_t bitpos = m_louds.select0(parent);
	assert(m_louds.rank0(bitpos) == parent);
	assert(m_louds.is0(bitpos));
	return m_louds.one_seq_len(bitpos+1);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
bool NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
has_children(size_t parent) const {
	assert(parent < total_states());
	size_t bitpos = m_louds.select0(parent);
	assert(m_louds.rank0(bitpos) == parent);
	assert(m_louds.is0(bitpos));
	return m_louds.is1(bitpos+1);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::gnode_states() const {
	return m_is_link.size();
}

///////////////////////////////////////////////////////////////////////////////

template<size_t StateID_Size>
struct NLT_IterEntry;

template<>
struct NLT_IterEntry<4> {
    uint32_t state;
    uint32_t child0;
    uint16_t n_children;
    uint08_t nth_child;
    uint08_t zpath_len;
};
template<>
struct NLT_IterEntry<8> {
    uint64_t state     : 48;
    uint64_t n_children: 16;
    uint64_t child0    : 48;
    uint64_t nth_child :  8;
    uint64_t zpath_len :  8;
};
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
struct NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::Entry : NLT_IterEntry<sizeof(index_t)> {
    bool has_next() const { return this->nth_child + 1 < this->n_children; }
};

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Entry>
inline
size_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
initIterEntry(size_t parent, Entry* e, byte_t* buf, size_t cap) const {
    assert(parent < total_states());
    m_is_link.prefetch_bit(parent);
#if 0
	size_t bitpos = m_louds.select0(parent);
#else
    size_t bitpos;
    if (parent < m_sel0_cache.size())
        bitpos = m_sel0_cache[parent];
    else
        bitpos = m_louds.select0(parent);
#endif
	size_t child0 = bitpos - parent;
    _mm_prefetch((const char*)m_label_data + child0, _MM_HINT_T0);
	assert(m_louds.rank0(bitpos) == parent);
	assert(m_louds.is0(bitpos));
	size_t lcount = m_louds.one_seq_len(bitpos+1);
	assert(child0 + lcount <= total_states());
    e->state = parent;
    e->child0 = child0;
    e->nth_child = 0;
    e->n_children = lcount;
    if (m_is_link[parent]) {
        size_t zlen = getZpathFixed(parent, buf, cap);
        assert(zlen + 1 <= cap);
        assert(zlen <= 254);
        e->zpath_len = zlen;
        return zlen;
    }
    else {
        assert(cap >= 1);
        e->zpath_len = 0;
        return 0;
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
inline
byte_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
getFirstChar(size_t child0, size_t lcount) const {
    assert(lcount > 0);
    if (FastLabel) {
        if (lcount < 36) {
            return m_label_data[child0];
        }
        else {
            const size_t* bits = (const size_t*)(m_label_data + child0 + 4);
            for(size_t i = 0; i < 32/sizeof(size_t); ++i) {
                size_t b = unaligned_load<size_t>(bits, i);
                if (b)
                    return byte_t(i * (8*sizeof(size_t)) + fast_ctz(b));
            }
            assert(false);
            abort(); // broken
        }
        return 0;
    }
    else {
        return label_first_byte(child0);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
inline
byte_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
getNthChar(size_t child0, size_t lcount, size_t nth) const {
    assert(nth < lcount);
    assert(child0 + lcount <= total_states());
    if (FastLabel) {
        auto label = m_label_data + child0;
        if (lcount < 36)
            return label[nth];
        else
            return rs_select1(label, nth);
    }
    else {
        return label_first_byte(child0 + nth);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
inline
byte_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
getNthCharNext(size_t child0, size_t lcount, size_t nth, byte_t cch) const {
    assert(nth < lcount);
    assert(child0 + lcount <= total_states());
    if (FastLabel) {
        auto label = m_label_data + child0;
        if (lcount < 36)
            return label[nth];
        else {
            const byte_t ch1 = rs_next_one_pos(label + 4, cch);
        #if !defined(NDEBUG)
            const byte_t ch2 = rs_select1(label, nth);
            const size_t nth0 = fast_search_byte_rs_idx(label, cch);
            const size_t nth2 = fast_search_byte_rs_idx(label, ch1);
            assert(ch1 == ch2);
            assert(nth == nth2);
            assert(nth == nth0 + 1);
            assert(cch <  ch1);
        #endif
            return ch1;
        }
    }
    else {
        return label_first_byte(child0 + nth);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
inline
byte_t NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
getNthCharPrev(size_t child0, size_t lcount, size_t nth, byte_t cch) const {
    assert(nth < lcount);
    assert(child0 + lcount <= total_states());
    if (FastLabel) {
        auto label = m_label_data + child0;
        if (lcount < 36)
            return label[nth];
        else {
            const byte_t ch1 = rs_prev_one_pos(label + 4, cch);
        #if !defined(NDEBUG)
            const byte_t ch2 = rs_select1(label, nth);
            const size_t nth2 = fast_search_byte_rs_idx(label, ch1);
            assert(ch1 == ch2);
            assert(nth == nth2);
            assert(ch1 <  cch);
        #endif
            return ch1;
        }
    }
    else {
        return label_first_byte(child0 + nth);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::Iterator() : ADFA_LexIterator(valvec_no_init()) {
    m_base = m_top = NULL;
    m_trie = NULL;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::Iterator(const Dawg* d) : ADFA_LexIterator(valvec_no_init()) {
    const NestLoudsTrieTpl* trie = d->m_trie;
    size_t word_mem = pow2_align_up(trie->m_max_strlen + 17, 16);
    size_t iter_mem = sizeof(Entry)*(trie->m_layer_id.size() + 2);
    m_word.ensure_capacity_slow(word_mem + iter_mem);
    m_dfa = d;
    m_top = m_base = (Entry*)(m_word.data() + word_mem);
    m_trie = trie;
}

///@param user_mem must be at least iterator_max_mem_size
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
UserMemIterator<Dawg>::UserMemIterator(const Dawg* d, void* user_mem) {
    const NestLoudsTrieTpl* trie = d->m_trie;
    size_t word_mem = pow2_align_up(trie->m_max_strlen + 17, 16);
    size_t iter_mem = sizeof(Entry)*(trie->m_layer_id.size() + 2);
    m_word.risk_set_data((byte_t*)user_mem, word_mem + iter_mem);
    m_dfa = d;
    m_top = m_base = (Entry*)(m_word.data() + word_mem);
    m_trie = trie;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
UserMemIterator<Dawg>::~UserMemIterator() {
    m_word.risk_release_ownership();
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
inline size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
UserMemIterator<Dawg>::s_max_mem_size(const NestLoudsTrieTpl* trie) {
    size_t word_mem = pow2_align_up(trie->m_max_strlen + 17, 16);
    size_t iter_mem = sizeof(Entry)*(trie->m_layer_id.size() + 2);
    return word_mem + iter_mem;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
UserMemIterator<Dawg>::reset(const BaseDFA* dfa, size_t root) {
    if (terark_unlikely(dfa && m_dfa != dfa)) {
        THROW_STD(logic_error, "dfa must be same as m_dfa");
    }
    Iterator<Dawg>::reset(dfa, root);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
inline void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::Iterator<Dawg>::reset1() {
    m_word.risk_set_size(0);
    m_top = m_base;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::reset(const BaseDFA* dfa, size_t root) {
    if (NULL == dfa || m_dfa == dfa) {
        reset1();
        return;
    }
    const Dawg* d = static_cast<const Dawg*>(dfa);
    const NestLoudsTrieTpl* trie = d->m_trie;
    size_t word_mem = pow2_align_up(trie->m_max_strlen + 17, 16);
    size_t iter_mem = sizeof(Entry)*(trie->m_layer_id.size() + 2);
    m_word.ensure_capacity(word_mem + iter_mem);
    m_dfa = d;
    m_top = m_base = (Entry*)(m_word.data() + word_mem);
    m_trie = trie;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
bool
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::Iterator<Dawg>::seek_end() {
    reset1();
    if (m_trie->total_states() > 0) {
        append_lex_max_suffix(initial_state, m_base, m_word.data());
        return true;
    }
    return false;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::append_lex_min_suffix(size_t root, Entry* ip, byte_t* wp) {
    size_t curr = root;
    auto d = static_cast<const Dawg*>(m_dfa);
    auto trie = static_cast<const typename Dawg::trie_type*>(m_trie);
    const byte_t* wlimit = (byte_t*)m_base;
    assert(root < trie->total_states());
    for (;;) {
        d->prefetch_term_bit(trie, curr);
        size_t zlen = trie->initIterEntry(curr, ip, wp, wlimit - wp);
        ip++;
        wp += zlen;
        if (d->is_term2(trie, curr))
            break;
        if (size_t n_children = ip[-1].n_children) {
            curr = ip[-1].child0;
            *wp++ = trie->getFirstChar(curr, n_children);
        } else {
            assert(false);
            abort();
        }
    }
    m_curr = curr;
    m_word.risk_set_end(wp);
    m_top = ip;
    *wp = '\0';
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::append_lex_max_suffix(size_t root, Entry* ip, byte_t* wp) {
    size_t curr = root;
    auto trie = static_cast<const typename Dawg::trie_type*>(m_trie);
    const byte_t* wlimit = (byte_t*)m_base;
    assert(root < trie->total_states());
    for (;;) {
        size_t zlen = trie->initIterEntry(curr, ip, wp, wlimit - wp);
        ip++;
        wp += zlen;
        if (size_t n_children = ip[-1].n_children) {
            size_t child0 = ip[-1].child0;
            ip[-1].nth_child = n_children-1;
            *wp++ = trie->getNthChar(child0, n_children, n_children-1);
            curr = child0 + n_children-1;
        } else
            break;
    }
    m_curr = curr;
    m_word.risk_set_end(wp);
    m_top = ip;
    *wp = '\0';
    assert(m_dfa->v_is_term(m_curr));
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
terark_flatten
bool
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::seek_lower_bound(fstring key) {
    auto wp = m_word.data();
    auto ip = m_base;
    auto trie = static_cast<const typename Dawg::trie_type*>(m_trie);
    const Dawg* d = static_cast<const Dawg*>(m_dfa);
    const Entry * base = m_base;
    const byte_t* wlimit = (byte_t*)base;
    size_t curr = initial_state;
    for (size_t pos = 0;; pos++) {
        assert(curr < trie->total_states());
        d->prefetch_term_bit(trie, curr);
        size_t zlen = trie->initIterEntry(curr, ip, wp, wlimit - wp);
        if (zlen) {
            auto kkn = key.size() - pos;
            auto zkn = std::min(zlen, kkn);
            auto pkey = key.udata() + pos;
            for (size_t zidx = 0; zidx < zkn; ++zidx) {
                if (terark_unlikely(pkey[zidx] != wp[zidx])) {
                    if (pkey[zidx] < wp[zidx]) { // is lower bound
                        wp += zlen;
                        goto CurrMinSuffix;
                    }
                    else // next word is lower_bound
                        goto RewindStackForNext;
                }
            }
            wp += zlen;
            if (terark_unlikely(kkn <= zlen)) // OK, current word is lower_bound
                goto CurrMinSuffix;
            pos += zlen;
        }
        else {
            assert(pos <= key.size());
            if (terark_unlikely(key.size() == pos)) { // done
                assert(size_t(wp - m_word.data()) == pos);
            CurrMinSuffix:
                if (d->is_term2(trie, curr)) {
                    *wp = '\0';
                    m_word.risk_set_end(wp);
                    m_top = ip+1;
                    m_curr = curr;
                    assert(key <= fstring(m_word));
                }
                else {
                    size_t n_children = ip->n_children;
                    if (terark_likely(n_children)) {
                        curr = ip->child0;
                        *wp = trie->getFirstChar(curr, n_children);
                        ip++;
                        wp++;
                        goto call_append_lex_min_suffix;
                    }
                    else {
                        assert(curr == initial_state);
                        reset1();
                        return false;
                    }
                }
                return true;
            }
        }
        assert(pos < key.size());
        size_t n_children = ip->n_children;
        size_t child0 = ip->child0;
        byte_t ch = (byte_t)key.p[pos];
	    if (FastLabel) {
            const byte_t* label = trie->m_label_data + child0;
            if (n_children < 36) {
                if (true/*n_children < 16*/) {
                    if (n_children && ch <= label[n_children-1]) {
		                size_t lo = size_t(-1);
                        do lo++; while (label[lo] < ch);
                        curr = child0 + lo;
                        ip->nth_child = lo;
                        const byte_t ch2 = label[lo];
                        if (ch2 == ch)
                            goto ThisLoopDone;
                        ch = ch2;
                        goto IterNextL;
                    }
                }
                else {
		            size_t lo = lower_bound_0(label, n_children, ch);
                    if (lo < n_children) {
                        curr = child0 + lo;
                        ip->nth_child = lo;
                        const byte_t ch2 = label[lo];
                        if (ch2 == ch)
                            goto ThisLoopDone;
                        ch = ch2;
                        goto IterNextL;
                    }
                }
            }
            else {
		        size_t lo = fast_search_byte_rs_idx(label, ch);
                if (lo < n_children) {
                    curr = child0 + lo;
                    ip->nth_child = lo;
                    if (terark_bit_test((const size_t*)(label + 4), ch))
                        goto ThisLoopDone;
                    ch = rs_next_one_pos(label + 4, ch);
                #if !defined(NDEBUG)
                    size_t lo2 = fast_search_byte_rs_idx(label, ch);
                    assert(lo == lo2);
                #endif
                    goto IterNextL;
                }
            }
	    }
	    else {
		    size_t lo = child0;
		    size_t hi = child0 + n_children;
		    while (lo < hi) {
			    size_t mid_id = (lo + hi) / 2;
			    byte_t mid_ch = trie->label_first_byte(mid_id);
			    if (mid_ch < ch)
				    lo = mid_id + 1;
			    else if (ch < mid_ch)
				    hi = mid_id;
                else {
                    curr = mid_id;
                    ip->nth_child = mid_id - child0;
                    goto ThisLoopDone;
                }
		    }
            if (lo < child0 + n_children) {
                curr = lo;
                ip->nth_child = lo - child0;
                ch = trie->label_first_byte(lo);
                goto IterNextL;
            }
	    }
        wp -= zlen;
        goto RewindStackForNext;
    IterNextL:
        *wp = ch;
        ip++;
        wp++;
        goto call_append_lex_min_suffix;
    ThisLoopDone:
        *wp++ = ch;
        ip++;
    }
RewindStackForNext:
    while (ip > base) {
        if (ip[-1].has_next()) {
            size_t nth = ++ip[-1].nth_child;
            curr = ip[-1].child0;
            byte_t ch2 = trie->getNthChar(curr, ip[-1].n_children, nth);
            assert(wp[-1] < ch2);
            wp[-1] = ch2;
            curr += nth;
            goto call_append_lex_min_suffix;
        }
        --ip;
        assert(size_t(wp - m_word.data()) >= ip->zpath_len + 1u);
        wp -= ip->zpath_len + 1;
    }
    reset1();
    return false;
call_append_lex_min_suffix:
    append_lex_min_suffix(curr, ip, wp);
    assert(key <= fstring(m_word));
    return true;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
terark_flatten
bool
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::Iterator<Dawg>::incr() {
    assert(m_top >= m_base);
    Entry* base = m_base;
    Entry* ip = m_top;
    if (terark_unlikely(ip == base)) {
        return false;
    }
    assert(0 == ip[-1].nth_child);
    assert(m_curr == ip[-1].state);
    assert(m_dfa->v_is_term(m_curr));
    auto wp = m_word.end();
    auto trie = static_cast<const typename Dawg::trie_type*>(m_trie);
    size_t child;
    if (size_t n_children = ip[-1].n_children) {
        child = ip[-1].child0;
        *wp = trie->getFirstChar(child, n_children);
    }
    else {
        while (!ip[-1].has_next()) {
            assert(ip > base);
            if (terark_unlikely(--ip == base)) {
                reset1();
                return false;
            }
            assert(size_t(wp - m_word.data()) >= ip->zpath_len + 1u);
            wp -= ip->zpath_len + 1;
        }
        child = ip[-1].child0;
        size_t nth = ++ip[-1].nth_child;
    //  *wp = trie->getNthChar(child, ip[-1].n_children, nth);
        *wp = trie->getNthCharNext(child, ip[-1].n_children, nth, *wp);
        child += nth;
    }
    append_lex_min_suffix(child, ip, wp+1);
    return true;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
terark_flatten
bool
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::Iterator<Dawg>::decr() {
    assert(m_top >= m_base);
    Entry* base = m_base;
    Entry* ip = m_top;
    if (terark_unlikely(ip == base)) {
        return false;
    }
    assert(0 == ip[-1].nth_child);
    assert(m_curr == ip[-1].state);
    if (terark_unlikely(--ip == base)) {
        reset1();
        return false;
    }
    byte_t* wp = m_word.end();
    assert(size_t(wp - m_word.data()) >= ip->zpath_len + 1u);
    wp -= ip->zpath_len + 1;
    const Dawg* d = static_cast<const Dawg*>(m_dfa);
    while (ip[-1].nth_child == 0) {
        assert(ip > base);
        size_t curr = ip[-1].state;
        if (d->is_term(curr)) {
            m_word.risk_set_end(wp);
            m_top = ip;
            m_curr = curr;
            *wp = '\0';
            return true;
        }
        if (terark_unlikely(--ip == base)) {
            reset1();
            return false;
        }
        assert(size_t(wp - m_word.data()) >= ip->zpath_len + 1u);
        wp -= ip->zpath_len + 1;
    }
    assert(ip > base);
    assert(ip[-1].n_children >= 2);
    assert(ip[-1].nth_child > 0);
    assert(ip[-1].nth_child < ip[-1].n_children);
    size_t n_children = ip[-1].n_children;
    size_t nth_child = --ip[-1].nth_child;
    size_t child0 = ip[-1].child0;
//  *wp = m_trie->getNthChar(child0, n_children, nth_child);
    *wp = m_trie->getNthCharPrev(child0, n_children, nth_child, *wp);
    size_t childn = child0 + nth_child;
    append_lex_max_suffix(childn, ip, wp+1);
    return true;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class Dawg>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
Iterator<Dawg>::seek_max_prefix(fstring key) {
    byte_t* wp = m_word.data();
    Entry * ip = m_base;
    auto trie = static_cast<const typename Dawg::trie_type*>(m_trie);
    const Dawg* d = static_cast<const Dawg*>(m_dfa);
    const byte_t* wlimit = (byte_t*)m_base;
    size_t curr = initial_state;
    size_t pos = 0;
    m_word.risk_set_size(0);
    m_curr = size_t(-1);
    m_top = ip;
    for (;;) {
        assert(curr < trie->total_states());
        d->prefetch_term_bit(trie, curr);
        size_t zlen = trie->initIterEntry(curr, ip, wp, wlimit - wp);
        if (zlen) {
            auto kkn = key.size() - pos;
            auto zkn = std::min(zlen, kkn);
            auto pkey = key.udata() + pos;
            for (size_t zidx = 0; zidx < zkn; ++zidx) {
                if (terark_unlikely(pkey[zidx] != wp[zidx])) {
                    pos += zidx;
                    goto RestoreLastMatch;
                }
            }
            wp += zkn;
            pos += zkn;
            if (terark_unlikely(zkn < zlen))
                goto RestoreLastMatch;
        }
        assert(pos <= key.size());
        assert(size_t(wp - m_word.data()) == pos);
        if (d->is_term2(trie, curr)) {
            m_word.risk_set_size(pos);
            m_top = ip+1;
            m_curr = curr;
        }
        if (key.size() == pos) {
            goto RestoreLastMatch;
        }
        const size_t n_children = ip->n_children;
        const size_t child0 = ip->child0;
        const byte_t ch = (byte_t)key.p[pos];
        if (FastLabel) {
            const byte_t* label = trie->m_label_data + child0;
            if (n_children < 36) {
                if (true/*n_children < 16*/) {
                    if (n_children && ch <= label[n_children-1]) {
                        size_t lo = size_t(-1);
                        do lo++; while (label[lo] < ch);
                        curr = child0 + lo;
                        ip->nth_child = lo;
                        if (label[lo] == ch)
                            goto ThisLoopDone;
                    }
                    goto RestoreLastMatch;
                }
                else {
                    size_t lo = lower_bound_0(label, n_children, ch);
                    if (lo < n_children) {
                        curr = child0 + lo;
                        ip->nth_child = lo;
                        if (label[lo] == ch)
                            goto ThisLoopDone;
                    }
                    goto RestoreLastMatch;
                }
            }
            else {
                size_t lo = fast_search_byte_rs_idx(label, ch);
                if (lo < n_children) {
                    curr = child0 + lo;
                    ip->nth_child = lo;
                    if (terark_bit_test((const size_t*)(label + 4), ch))
                        goto ThisLoopDone;
                }
                goto RestoreLastMatch;
            }
        }
        else {
            size_t lo = child0;
            size_t hi = child0 + n_children;
            while (lo < hi) {
                size_t mid_id = (lo + hi) / 2;
                byte_t mid_ch = trie->label_first_byte(mid_id);
                if (mid_ch < ch)
                    lo = mid_id + 1;
                else if (ch < mid_ch)
                    hi = mid_id;
                else {
                    curr = mid_id;
                    ip->nth_child = mid_id - child0;
                    goto ThisLoopDone;
                }
            }
            goto RestoreLastMatch;
        }
    ThisLoopDone:
        *wp++ = ch;
        ip++;
        pos++;
    }
RestoreLastMatch:
    if (m_top > m_base) {
        m_top[-1].nth_child = 0;
        m_word.end()[0] = '\0';
    }
    return pos;
}

} // namespace terark


