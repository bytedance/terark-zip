#pragma once

#include "fsa_cache.hpp"
#include "double_array_trie.hpp"
#include <terark/util/hugepage.hpp>
#include <terark/util/profiling.hpp>

namespace terark {

struct NTD_CacheState {
	uint32_t m_child0; // aka base, the child state on transition: '\0'
	uint32_t m_parent; // aka check
	uint32_t m_map_state;
	uint32_t m_zp_offset : 31;
	uint32_t m_is_term   :  1;

	typedef uint32_t state_id_t;
	typedef uint32_t position_t;
	static const uint32_t max_zp_offset = uint32_t((uint32_t(1) << 31) - 1);
	static const uint32_t max_state = 0xFFFFFFFE;
	static const uint32_t nil_state = 0xFFFFFFFF;

	NTD_CacheState() {
		m_child0 = nil_state;
		m_parent = nil_state;
		m_is_term = false;
		m_zp_offset = max_zp_offset;
		m_map_state = nil_state;  // indicate is_free
	}

	void set_term_bit() { m_is_term = true; }
	void set_free_bit() { m_map_state = nil_state; }
	void set_child0(state_id_t x) { assert(x < max_state); m_child0 = x; m_map_state = max_state; }
	void set_parent(state_id_t x) { assert(x < max_state); m_parent = x; m_map_state = max_state; }

	bool is_term() const { return 0 != m_is_term; }
	bool is_free() const { return nil_state == m_map_state; }
	uint32_t child0() const { /*assert(!is_free());*/ return m_child0; }
	uint32_t parent() const { /*assert(!is_free());*/ return m_parent; }

	typedef state_id_t transition_t;
	static transition_t first_trans(state_id_t t) { return t; }
};

// just for NestLoudsTrieDAWG, because cut by state id
// Louds is BFS style, smaller state id is more near to root
template<class DFA>
class SmallStateIdSubDFA {
public:
	typedef uint32_t state_id_t;
	const static state_id_t nil_state = UINT32_MAX;//DFA_as_Trie::nil_state;
	const static state_id_t max_state = UINT32_MAX-1;//DFA_as_Trie::max_state;
	const DFA* m_dfa;
	const size_t  m_size;
	SmallStateIdSubDFA(const DFA* trie, size_t size) : m_dfa(trie), m_size(size) {}
	enum { sigma = 512 };
	size_t get_all_move(size_t parent, CharTarget<size_t>* moves) const {
		size_t n = m_dfa->get_all_move(parent, moves);
		size_t i = 0;
		for (size_t j = 0; j < n; ++j) {
			if (moves[j].target < m_size)
				moves[i] = moves[j], i++;
		}
		return i;
	}
	bool is_term(size_t s) const { return m_dfa->is_term(s); }
	size_t total_states() const { return m_size; }
};

class NTD_CacheTrie : public DoubleArrayTrie<NTD_CacheState> {
	typedef DoubleArrayTrie<NTD_CacheState> super;
public:
	byte_t const* m_zp_data; // zip path cache
	size_t m_dfa_mem_size;
	size_t m_dfa_states;
	double m_build_time;
	template<class DFA_as_Trie>
	NTD_CacheTrie(const DFA_as_Trie* trie, size_t size, const char* walkMethod) {
		profiling pf;
		const SmallStateIdSubDFA<DFA_as_Trie> subdfa(trie, size);
		long long t1 = pf.now();
		valvec<uint32_t> t2d;
		valvec<uint32_t> d2t;
		if (NULL == walkMethod) {
			walkMethod = "CFS";
			if (auto env = getenv("FSA_Cache_walkMethod")) {
				if (strcasecmp(env, "BFS") == 0
				 || strcasecmp(env, "CFS") == 0
				 || strcasecmp(env, "DFS") == 0
				) {
					walkMethod = env;
				}
				else {
					fprintf(stderr
						, "WARN: env FSA_Cache_walkMethod=%s is invalid, use default: \"%s\"\n"
						, env, walkMethod);
				}
			}
		}
		this->build_from(subdfa, d2t, t2d, walkMethod, 1);
		MatchContext mctx;
		valvec<byte_t> zp_data;
		for(size_t i = 0; i < d2t.size(); ++i) {
			size_t j = d2t[i];
			this->states[i].m_zp_offset = uint32_t(zp_data.size());
			this->states[i].m_map_state = uint32_t(j);
			if (j < size && trie->is_pzip(j)) {
				zp_data.append(trie->get_zpath_data(j, &mctx));
			}
		}
	//	states.back().m_zp_offset = zp_data.size(); // not needed
		assert(zp_data.size() == states.back().m_zp_offset);
		assert(d2t.size() == states.size());
		long long t2 = pf.now();
		this->m_gnode_states = size;
		this->transition_num = size - 1;
		this->m_total_zpath_len = zp_data.size();
		this->m_dfa_mem_size = trie->mem_size();
		this->m_dfa_states = trie->total_states();
		this->m_build_time = pf.sf(t1, t2);
		if (getEnvBool("FSA_Cache_showStat")) {
			print_cache_stat(stderr);
		}
		size_t zp_cell = ceiled_div(zp_data.size(), sizeof(NTD_CacheState));
		auto   zp_pmem = this->states.grow_no_init(zp_cell);
		memcpy(zp_pmem, zp_data.data(), zp_data.size());
		if (this->states.used_mem_size() >= hugepage_size) {
			use_hugepage_advise(&this->states);
		}
		this->states.risk_set_size(d2t.size());
		m_zp_data = (const byte_t*)(this->states.end());
	}
	void print_cache_stat(FILE* fp) const {
		fprintf(fp, "FSA_Cache build  time: %9.6f sec | %8.3f MB/sec\n", m_build_time, mem_size()/1e6/m_build_time);
		fprintf(fp, "FSA_Cache total bytes: %9zd     | all = %11zd | ratio = %7.5f\n", mem_size(), m_dfa_mem_size, 1.0*mem_size()/m_dfa_mem_size);
		fprintf(fp, "FSA_Cache total nodes: %9zd     | all = %11zd | ratio = %7.5f\n", total_states(), m_dfa_states, 1.0*total_states() / m_dfa_states);
		fprintf(fp, "FSA_Cache  used nodes: %9zd\n", m_gnode_states);
		fprintf(fp, "FSA_Cache  fill ratio: %9.6f\n", m_gnode_states*1.0/states.size());
		fprintf(fp, "FSA_Cache zpath bytes: %9zd\n", size_t(m_total_zpath_len));
		fprintf(fp, "FSA_Cache nodes bytes: %9zd\n", states.used_mem_size());
		fprintf(fp, "FSA_Cache node1 bytes: %9zd\n", sizeof(NTD_CacheState));
	}
	bool is_pzip(size_t s) const {
		assert(s < states.size()-1);
		return states[s].m_zp_offset < states[s+1].m_zp_offset;
	}
	fstring get_zpath_data(size_t s, MatchContext*) const {
		size_t offset0 = states[s+0].m_zp_offset;
		size_t offset1 = states[s+1].m_zp_offset;
		assert(offset0 <= offset1);
		return fstring(m_zp_data + offset0, offset1 - offset0);
	}
	size_t mem_size() const { return this->states.full_mem_size(); }
	const NTD_CacheState* get_double_array() const { return states.data(); }
	const byte_t* get_zpath_data_base() const { return m_zp_data; }
};

} // namespace terark

