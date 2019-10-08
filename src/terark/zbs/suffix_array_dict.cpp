#include "suffix_array_dict.hpp"
#include <zstd/dictBuilder/divsufsort.h>
#include "sufarr_inducedsort.h"
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#ifdef SuffixDictCacheDebug
#include <terark/fsa/automata_basic.hpp>
#endif // SuffixDictCacheDebug
#include <terark/fsa/double_array_trie.hpp>
#include <terark/util/hugepage.hpp>
#include <terark/util/profiling.hpp>

namespace terark {

static bool g_useHugePage = getEnvBool("TerarkUseHugePage");
static bool g_suffixDictShowState = getEnvBool("SuffixDictCacheDFA_showStat");

// 0 for sais
// 1 for divdufsort
// 2 for inner_sais
// 3 for gluten_sain
int g_useDivSufSort = (int)getEnvLong("TerarkUseDivSufSort", 0);

void SetUseDivSufSort(int v) {
    terark::g_useDivSufSort = v;
}

static const size_t MaxDepth = (1 << 8) - 1;

// manully added prefetch makes it slower on visual c++ 2015
#define SuffixDict_EnablePrefetch
#if defined(SuffixDict_EnablePrefetch)
	#define SuffixDict_prefetch(ptr) _mm_prefetch((const char*)ptr, _MM_HINT_T0)
#else
	#define SuffixDict_prefetch(ptr)
#endif

class MyAppendOnlyTrie {
public:
	struct state_t {
		size_t getZstrLen() const { return m_zstrLen; }
		void setZstrLen(size_t zlen) { m_zstrLen = zlen; }
		bool is_pzip() const { return 0 != m_zstrLen; }
		uint32_t m_firstChild;
		uint32_t m_incomingLabel :  8;
		uint32_t m_zstrLen       :  8;
		uint32_t m_numChildren   : 16;
		uint32_t m_suffixLow;
		uint32_t m_suffixHig;
		state_t() {
			m_firstChild = UINT32_MAX;
			m_incomingLabel = 0;
			m_zstrLen = 0;
			m_numChildren = 0;
			m_suffixLow = -1;
			m_suffixHig = -1;
		}
	};
	valvec<state_t> states;
	typedef uint32_t state_id_t;
	static const size_t   sigma = 256;
	static const uint32_t max_state = UINT32_MAX - 2;
	static const uint32_t nil_state = UINT32_MAX - 1;

	size_t total_states() const { return states.size(); }
	bool is_term(size_t) const { return false; }

	size_t get_all_move(size_t state, CharTarget<size_t>* moves) const {
		auto lstates = states.data();
		size_t beg = lstates[state].m_firstChild;
		size_t end = lstates[state].m_numChildren + beg;
		for (size_t i = beg; i < end; ++i) {
			moves[i-beg] = CharTarget<size_t>(lstates[i].m_incomingLabel, i);
		}
		return end - beg;
	}
	void add_all_move(size_t state, const CharTarget<size_t>* moves, size_t nMoves) {
		size_t firstChild = states.size() - nMoves;
		auto children = states.data() + firstChild;
		states[state].m_firstChild = firstChild;
		states[state].m_numChildren = uint16_t(nMoves);
		for (size_t i = 0; i < nMoves; ++i) {
			children[i].m_incomingLabel = moves[i].ch;
			assert(moves[i].target == firstChild + i);
		}
	}
	size_t new_state() {
		size_t oldsize = states.size();
		states.push_back();
		return oldsize;
	}
	void finish_append() {
		states.push_back();
		states.end()[-1].m_firstChild = states.end()[-2].m_firstChild;
	}
	void erase_all() {}
	void shrink_to_fit() { states.shrink_to_fit(); }
};

#ifdef SuffixDictCacheDebug
struct SuffixDictCacheState : public State32 {
	size_t getZstrLen() const { return this->padding_bits; }
	void setZstrLen(size_t pos) {
		assert(pos <= MaxDepth);
		// has 13 bits, now just use 8 bits
		this->padding_bits = pos;
	}
	uint32_t m_suffixLow = -1;
	uint32_t m_suffixHig = -1;
};
class SuffixDictCacheDFA::MyBitmapSmartTrie
	: public AutomataAsBaseDFA<SuffixDictCacheState>
{
	friend class SuffixDictCacheDFA;
	friend class HashSuffixDictCacheDFA;
protected:
	void dot_write_one_state(FILE* fp, size_t ls, const char* ext_attr) const override;
public:
	void finish_append() {}
};
#else
	class SuffixDictCacheDFA::MyBitmapSmartTrie {};
#endif // SuffixDictCacheDebug

#pragma pack(push,1)
class SuffixDictCacheDFA::MyDoubleArrayNode {
public:
	typedef uint32_t state_id_t;
	static const state_id_t max_state = 0x0FFFFFFE;
	static const state_id_t nil_state = 0x0FFFFFFF;

	size_t m_child0    : 28; // aka base, the child state on transition: '\0'
	size_t m_parent    : 28; // aka check
	size_t m_zlenLo    :  8;
	uint32_t m_suffixLow; // for very little performance, use 32
	uint32_t m_suffixHig;

	MyDoubleArrayNode() {
		m_zlenLo = 0;
		m_suffixLow = 0;
		m_suffixHig = 0;
		m_child0 = nil_state;
		m_parent = nil_state;
	}

	void setZstrLen(size_t zlen) {
		BOOST_STATIC_ASSERT(MaxDepth == 255);
		assert(zlen <= 255);
		m_zlenLo = zlen;
	}
	size_t getZstrLen() const {
		return m_zlenLo;
	}

	void set_term_bit() {}
	void set_free_bit() { m_parent = nil_state; }
	void set_child0(state_id_t x) { m_child0 = x; }
	void set_parent(state_id_t x) {
		assert(x < max_state);
		// also clear free flag
		m_parent = x;
	}

	bool is_term() const { return true; }
	bool is_free() const { return nil_state == m_parent; }
	size_t child0() const { /*assert(!is_free());*/ return m_child0; }
	size_t parent() const { /*assert(!is_free());*/ return m_parent; }
};
#pragma pack(pop)

class SuffixDictCacheDFA::MyDoubleArrayTrie
	: public DoubleArrayTrie<SuffixDictCacheDFA::MyDoubleArrayNode>
{
	friend class SuffixDictCacheDFA;
	friend class HashSuffixDictCacheDFA;
public:
	BOOST_STATIC_ASSERT(sizeof(SuffixDictCacheDFA::MyDoubleArrayNode) == 16);
	void useHugePage();
};

void SuffixDictCacheDFA::MyDoubleArrayTrie::useHugePage() {
#if defined(_MSC_VER)
#else
	use_hugepage_advise(&states);
#endif
}

//#define SuffixDictCacheDFA_EnableProfiling

#ifdef SuffixDictCacheDFA_EnableProfiling
	static long long g_hash_time = 0;
	static long long g_dfa_time = 0;
	static long long g_sa_time = 0;
	profiling g_pf;
	#define SuffixDictCacheDFA_Profiling_t0() long long t0 = g_pf.now()
	#define SuffixDictCacheDFA_Profiling(which) \
		TERARK_SCOPE_EXIT( \
			long long t1 = g_pf.now(); \
			g_##which##_time += t1 - t0; \
			t0 = t1; \
		)
#else
	#define SuffixDictCacheDFA_Profiling_t0()
	#define SuffixDictCacheDFA_Profiling(which)
#endif

SuffixDictCacheDFA::SuffixDictCacheDFA() {
	m_sa_data = NULL;
	m_sa_size = 0;
	m_str = NULL;
}

SuffixDictCacheDFA::~SuffixDictCacheDFA() {
#ifdef SuffixDictCacheDFA_EnableProfiling
	printf("hash_time = %f's  dfa_time = %f's  sa_time = %f's\n"
		, g_pf.sf(0,g_hash_time)
		, g_pf.sf(0,g_dfa_time), g_pf.sf(0, g_sa_time));
#endif
}

void SuffixDictCacheDFA::build_sa(valvec<byte>& str) {
	profiling pf;
	size_t nStrLen = str.size();
	size_t nStrLenAligned = align_up(str.size(), sizeof(saidx_t));
	size_t nTotalBytes = nStrLenAligned + sizeof(saidx_t) * str.size();
	if (g_useHugePage) {
		use_hugepage_resize_no_init(&str, nTotalBytes);
	} else {
		str.resize_no_init(nTotalBytes);
	}
	str.risk_set_size(nStrLen);
	memset(str.data() + nStrLen, 0, nStrLenAligned - nStrLen);
	auto sa_data = (saidx_t*)(str.data() + nStrLenAligned);
	m_sa_data = sa_data;
	m_sa_size = nStrLen;
	m_str = str.data();
	llong t0 = pf.now();
	if (g_useDivSufSort == 1)
		divsufsort((byte*)str.data(), sa_data, nStrLen, 0);
	else
		sufarr_inducedsort((byte*)str.data(), sa_data, nStrLen);
	llong t1 = pf.now();
	if (g_suffixDictShowState) {
		printf("SuffixDictCacheDFA::build_sa(): g_useHugePage = %d\n"
			"%s: %zd bytes, time: %f seconds, through-put: %f MB/s\n"
			, g_useHugePage, g_useDivSufSort == 1 ? "divsufsort" : "SAIS"
			, nStrLen, pf.sf(t0,t1), nStrLen/pf.uf(t0,t1));
	}
}

#ifdef SuffixDictCacheDebug
void
SuffixDictCacheDFA::MyBitmapSmartTrie::
dot_write_one_state(FILE* fp, size_t s, const char* ext_attr) const {
	long ls = s;
	long lo = this->states[s].m_suffixLow;
	long hi = this->states[s].m_suffixHig;
	if (v_is_pzip(ls)) {
		MatchContext ctx;
		fstring zs = v_get_zpath_data(ls, &ctx);
		char buf[1040];
		dot_escape(zs.data(), zs.size(), buf, sizeof(buf)-1);
		if (v_is_term(ls))
			fprintf(fp, "\tstate%ld[label=\"%ld(%ld %ld)\\n%s\" shape=\"doublecircle\" %s];\n", ls, ls, lo, hi, buf, ext_attr);
		else
			fprintf(fp, "\tstate%ld[label=\"%ld(%ld %ld)\\n%s\" %s];\n", ls, ls, lo, hi, buf, ext_attr);
	}
	else {
		if (v_is_term(ls))
			fprintf(fp, "\tstate%ld[label=\"%ld(%ld %ld)\" shape=\"doublecircle\" %s];\n", ls, ls, lo, hi, ext_attr);
		else
			fprintf(fp, "\tstate%ld[label=\"%ld(%ld %ld)\" %s];\n", ls, ls, lo, hi, ext_attr);
	}
}
#endif // SuffixDictCacheDebug

struct SuffixDictCacheDFA::BfsQueueElem {
	uint32_t depth; // matching depth, pos
	uint32_t state;
};
void
SuffixDictCacheDFA::bfs_build_cache(size_t minFreq, size_t maxBfsDepth) {
#ifdef SuffixDictCacheDebug
	auto trie = new MyBitmapSmartTrie();
	m_bm.reset(trie);
	tpl_bfs_build_cache(trie, minFreq, maxBfsDepth);
	m_bm.reset();
#else
	std::unique_ptr<MyAppendOnlyTrie> trie(new MyAppendOnlyTrie());
	tpl_bfs_build_cache(trie.get(), minFreq, maxBfsDepth);
#endif
}
template<class TrieClass>
void
SuffixDictCacheDFA::tpl_bfs_build_cache(TrieClass* trie, size_t minFreq, size_t maxBfsDepth) {
	profiling pf;
	long long t0 = pf.now();
{
	const byte_t* str = m_str;
	const int   * sa  = m_sa_data;
	const size_t  sa_size = m_sa_size;
	trie->erase_all();
	trie->states.erase_all();
	trie->states.push_back({});
	trie->states[0].m_suffixLow = 0;
	trie->states[0].m_suffixHig = sa_size;
	AutoFree<CharTarget<size_t> > children(trie->sigma);
	valvec<BfsQueueElem> q1, q2;
	q1.push_back({0, 0});
	size_t bfsDepth = 0;
	while (!q1.empty() && bfsDepth < maxBfsDepth) {
		for(auto e : q1) {
			size_t lo = trie->states[e.state].m_suffixLow;
			size_t hi = trie->states[e.state].m_suffixHig;
			size_t depth = e.depth;
			if (sa[lo] + depth < sa_size) {
				size_t saLo = sa[lo];
				size_t saHi = sa[hi-1];
				if (str[saLo + depth] == str[saHi + depth]) {
					size_t maxPos = std::min(saLo + depth + MaxDepth, sa_size);
					do ++depth;
					while (	    saLo + depth  < maxPos &&
							str[saLo + depth] == str[saHi + depth] );
					trie->states[e.state].setZstrLen(depth - e.depth);
				//	trie->states[e.state].set_pzip_bit();
				}
			}
			if (sa[lo] + depth >= sa_size) {
				lo++;
			}
			size_t childcnt = 0;
			while (lo < hi) {
				byte_t c = str[sa[lo] + depth];
				size_t u = sa_upper_bound(lo, hi, depth, c);
				if (u - lo >= minFreq) {
					size_t child = trie->new_state();
					q2.push_back({uint32_t(depth) + 1, uint32_t(child)});
					children[childcnt].ch = c;
					children[childcnt].target = child;
					trie->states[child].m_suffixLow = lo;
					trie->states[child].m_suffixHig = u;
					childcnt++;
				}
				lo = u;
			}
			trie->add_all_move(e.state, children, childcnt);
		//	int keepStackFrameForMSVC = 1;
		}
		q1.swap(q2);
		q2.erase_all();
		bfsDepth++;
	}
	trie->finish_append();
	trie->shrink_to_fit();
}
	long long t1 = pf.now();
	valvec<uint32_t> t2d, d2t;
	auto dart = new MyDoubleArrayTrie();
	m_da.reset(dart);
	const char* walkMethod = "CFS";
	if (auto env = getenv("SuffixDictCacheDFA_WalkMethod")) {
		if (strcasecmp(env, "BFS") == 0
		 || strcasecmp(env, "CFS") == 0
		 || strcasecmp(env, "DFS") == 0
		) {
			walkMethod = env;
		}
		else {
			fprintf(stderr
				, "WARN: env SuffixDictCacheDFA_WalkMethod=%s is invalid, use default: \"%s\"\n"
				, env, walkMethod);
		}
	}
	dart->build_from(*trie, d2t, t2d, walkMethod, 0);
	for(size_t i = 0; i < d2t.size(); ++i) {
		size_t j = d2t[i];
		if (j < trie->states.size()) {
			dart->states[i].setZstrLen(trie->states[j].getZstrLen());
			dart->states[i].m_suffixLow = trie->states[j].m_suffixLow;
			dart->states[i].m_suffixHig = trie->states[j].m_suffixHig;
		}
	}
	long long t2 = pf.now();
//	this->write_dot_file("suffix-array.dot");
	if (g_suffixDictShowState) {
		printf("build dynamic trie time: %7.3f seconds\n", pf.sf(t0,t1));
		printf("build double array time: %7.3f seconds\n", pf.sf(t1,t2));
		sa_print_stat();
	}
	if (g_useHugePage)
		m_da->useHugePage();
}

#ifdef SuffixDictCacheDebug
void
SuffixDictCacheDFA::pfs_build_cache(size_t minFreq) {
	const byte_t* str = m_str;
	const int   * sa  = m_sa_data;
	const size_t  sa_size = m_sa_size;
	auto trie = new MyBitmapSmartTrie();
	auto dart = new MyDoubleArrayTrie();
	m_bm.reset(trie);
	m_da.reset(dart);
	trie->erase_all();
	trie->states.erase_all();
	trie->states.push_back({});
	trie->states[0].m_suffixLow = 0;
	trie->states[0].m_suffixHig = sa_size;
	AutoFree<CharTarget<size_t> > children(trie->sigma);
	valvec<BfsQueueElem> stack;
	stack.push_back({0, 0});
	while (!stack.empty()) {
		auto e = stack.pop_val();
		size_t lo = trie->states[e.state].m_suffixLow;
		size_t hi = trie->states[e.state].m_suffixHig;
		size_t depth = e.depth;
		if (sa[lo] + depth < sa_size) {
			size_t saLo = sa[lo];
			size_t saHi = sa[hi-1];
			if (str[saLo + depth] == str[saHi + depth]) {
				size_t maxPos = std::min(saLo + depth + MaxDepth, sa_size);
				do ++depth;
				while (	    saLo + depth < maxPos &&
						str[saLo + depth] == str[saHi + depth] );
				trie->states[e.state].setZstrLen(depth - e.depth);
			//	trie->states[e.state].set_pzip_bit();
			}
		}
		if (sa[lo] + depth >= sa_size) {
			lo++;
		}
		size_t childcnt = 0;
		while (lo + minFreq <= hi) {
			byte_t c = str[sa[lo] + depth];
			size_t u = sa_upper_bound(lo, hi, depth, c);
			if (u - lo >= minFreq) {
				size_t child = trie->new_state();
				stack.push_back({uint32_t(depth + 1), uint32_t(child)});
				children[childcnt].ch = c;
				children[childcnt].target = child;
				trie->states[child].m_suffixLow = lo;
				trie->states[child].m_suffixHig = u;
				childcnt++;
			}
			lo = u;
		}
		trie->add_all_move(e.state, children, childcnt);
	//	int keepStackFrameForMSVC = 1;
	}
//	trie->write_dot_file("suffix-array.dot");
//	sa_print_stat();
}
#endif // SuffixDictCacheDebug

static inline
std::pair<size_t, size_t>
s_sa_equal_range(const byte* str, const int* sa, size_t saLen,
				 size_t lo, size_t hi, size_t depth, byte_t ch) {
	assert(lo < hi);
	assert(hi <= saLen);
	assert(sa[lo] + depth <= saLen);
	if (terark_unlikely(sa[lo] + depth >= saLen)) {
		lo++;
		assert(sa[lo] + depth < saLen);
	}
	while (lo < hi) {
		assert(sa[lo] + depth < saLen);
		size_t mid = (lo + hi) / 2;
		assert(sa[mid] + depth < saLen);
		byte_t hitChr = str[sa[mid] + depth];
		if (hitChr < ch)
			lo = mid + 1;
		else if (hitChr > ch)
			hi = mid;
		else
			goto Found;
	}
	return std::make_pair(lo, lo);
Found:
	size_t lo1 = lo, hi1 = hi;
	while (lo1 < hi1) {
		size_t mid = (lo1 + hi1) / 2;
		byte_t hitChr = str[sa[mid] + depth];
		if (hitChr < ch) // lower bound
			lo1 = mid + 1;
		else
			hi1 = mid;
	}
	size_t lo2 = lo, hi2 = hi;
	while (lo2 < hi2) {
		size_t mid = (lo2 + hi2) / 2;
		byte_t hitChr = str[sa[mid] + depth];
		if (hitChr <= ch) // upper bound
			lo2 = mid + 1;
		else
			hi2 = mid;
	}
	return std::make_pair(lo1, lo2);
}

// many params may get better compiler optimization
static inline
SuffixDictCacheDFA::MatchStatus
s_sa_match(const byte* str, const int* sa, size_t saLen,
			size_t lo, size_t hi, size_t pos,
			const byte* input, size_t len, size_t minFreq) {
	using std::min;
	using std::max;
//	printf("freq =%4zd  pos =%3zd : \33[1;31m%.*s\33[0m%.*s\n", hi-lo, pos
//		, int(pos), input, min(int(len-pos), 40), input + pos);
#if 0
	// not ready yet
	if (minFreq <= 1) {
		auto mr = sa_match_exact(lo, hi, pos, input, len);
		lo = mr.first;
		hi = mr.first + 1;
		pos = mr.second;
//		printf("freq =%4zd  pos =%3zd : \33[1;32m%.*s\33[0m%.*s\n", hi-lo, pos
//			, int(pos), input, min(int(len-pos), 40), input + pos);
		return {lo, hi, pos};
	}
#endif
	assert(hi - lo >= minFreq); // cache should has a bigger freq
	while (pos < len) {
		if (terark_unlikely(lo + 1 == hi)) {
			size_t saLo = sa[lo];
			size_t minLen = min(len, saLen - saLo);
			const byte* pLo = str + saLo;
			while (pos < minLen && input[pos] == pLo[pos])
				pos++;
			break;
		}
		else {
			size_t saLo = sa[lo+0];
			size_t saHi = sa[hi-1];
			size_t minLen = min(saLen - max(saLo, saHi), len);
			byte_t const* pLo = str + saLo;
			byte_t const* pHi = str + saHi;
			while ( pos < minLen &&
					pLo[pos] == input[pos] &&
					pHi[pos] == input[pos] )
				pos++;
			if (pos == len)
				break;
		}
		auto rng = s_sa_equal_range(str, sa, saLen, lo, hi, pos, input[pos]);
		if (rng.second - rng.first >= minFreq) {
			lo = rng.first;
			hi = rng.second;
			pos++;
			SuffixDict_prefetch(&sa[lo+0]);
			SuffixDict_prefetch(&sa[hi-1]);
		} else
			break;
	}
//	printf("freq =%4zd  pos =%3zd : \33[1;32m%.*s\33[0m%.*s\n", hi-lo, pos
//		, int(pos), input, min(int(len-pos), 40), input + pos);
	return {lo, hi, pos};
}

#ifdef SuffixDictCacheDebug
#ifdef __GNUC__
 __attribute__((flatten))
#endif
SuffixDictCacheDFA::MatchStatus
SuffixDictCacheDFA::sa_match_max_length(const byte* input, size_t len, size_t minFreq)
const {
	assert(minFreq >= 1);
	assert(m_bm.get() != NULL);
	const byte_t* str = m_str;
	const int* sa = m_sa_data;
	size_t lo = 0, hi = m_sa_size, pos = 0;
	SuffixDictCacheDFA_Profiling_t0();
{
	SuffixDictCacheDFA_Profiling(dfa);
	size_t state = 0;
	const auto trie = m_bm.get();
	const auto lstates = trie->states.data();
	while (pos < len) {
		if (size_t zlen = lstates[state].getZstrLen()) {
			size_t zend = std::min(len, pos + zlen);
			for (auto zptr = str + sa[lo]; pos < zend; pos++) {
				if (zptr[pos] != input[pos])
					return {lo, hi, pos};
			}
			if (terark_unlikely(pos == len))
				return {lo, hi, pos};
		}
		size_t  child = trie->state_move(lstates[state], input[pos]);
		if (terark_likely(trie->nil_state != child)) {
			state = child;
			lo = lstates[child].m_suffixLow;
			hi = lstates[child].m_suffixHig;
			assert(hi - lo >= minFreq); // cache should has a bigger freq
			assert(str[sa[lo] + pos] == input[pos]);
			pos++;
		}
		else
			goto SearchSA;
	}
	return {lo, hi, pos};
}
SearchSA:
	SuffixDictCacheDFA_Profiling(sa);
	return s_sa_match(str, sa, m_sa_size, lo, hi, pos, input, len, minFreq);
}
#endif // SuffixDictCacheDebug

#ifdef __GNUC__
 __attribute__((flatten))
#endif
SuffixDictCacheDFA::MatchStatus
SuffixDictCacheDFA::da_match_max_length(const byte* input, size_t len, size_t minFreq)
const {
	assert(minFreq >= 1);
	assert(m_da.get() != NULL);
	const byte_t* str = m_str;
	const int* sa = m_sa_data;
	size_t lo = 0, hi = m_sa_size, pos = 0;
	SuffixDictCacheDFA_Profiling_t0();
{
	SuffixDictCacheDFA_Profiling(dfa);
	size_t state = 0;
	const auto lstates = m_da->states.data();
	while (pos < len) {
#if defined(SuffixDict_EnablePrefetch)
		size_t child;
#endif
		if (size_t zlen = lstates[state].getZstrLen()) {
			SuffixDict_prefetch(&sa[lo]);
#if defined(SuffixDict_EnablePrefetch)
			child = lstates[state].m_child0 + input[pos+zlen];
			SuffixDict_prefetch(&lstates[child]);
#endif
			size_t zend = std::min(len, pos + zlen);
			for (auto zptr = str + sa[lo]; pos < zend; pos++) {
				if (zptr[pos] != input[pos])
					return {lo, hi, pos};
			}
			if (terark_unlikely(pos == len))
				return {lo, hi, pos};
		}
		else {
#if defined(SuffixDict_EnablePrefetch)
			child = lstates[state].m_child0 + input[pos];
#endif
		}
#if !defined(SuffixDict_EnablePrefetch)
		size_t  child = lstates[state].m_child0 + input[pos];
#endif
		if (terark_likely(lstates[child].m_parent == state)) {
			state = child;
			lo = lstates[child].m_suffixLow;
			hi = lstates[child].m_suffixHig;
			assert(hi - lo >= minFreq); // cache should has a bigger freq
			assert(str[sa[lo] + pos] == input[pos]);
			pos++;
		}
		else
			goto SearchSA;
	}
	return {lo, hi, pos};
}
SearchSA:
	SuffixDictCacheDFA_Profiling(sa);
	return s_sa_match(str, sa, m_sa_size, lo, hi, pos, input, len, minFreq);
}

size_t
SuffixDictCacheDFA::sa_lower_bound(size_t lo, size_t hi, size_t depth, byte_t ch)
const {
	assert(lo < hi);
	assert(hi <= m_sa_size);
	const int* sa = m_sa_data;
	const byte_t* str = m_str;
	assert(sa[lo] + depth <= m_sa_size);
	if (terark_unlikely(sa[lo] + depth >= m_sa_size)) {
		lo++;
		assert(sa[lo] + depth < m_sa_size);
	}
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		byte_t hitChr = str[sa[mid] + depth];
		if (hitChr < ch) // lower bound
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t
SuffixDictCacheDFA::sa_upper_bound(size_t lo, size_t hi, size_t depth, byte_t ch)
const {
	assert(lo < hi);
	assert(hi <= m_sa_size);
	const int* sa = m_sa_data;
	const byte_t* str = m_str;
	assert(sa[lo] + depth <= m_sa_size);
	if (terark_unlikely(sa[lo] + depth >= m_sa_size)) {
		lo++;
		assert(sa[lo] + depth < m_sa_size);
	}
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		byte_t hitChr = str[sa[mid] + depth];
		if (hitChr <= ch) // upper bound
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

std::pair<size_t, size_t>
SuffixDictCacheDFA::sa_equal_range(size_t lo, size_t hi, size_t depth, byte_t ch)
const {
	return s_sa_equal_range(m_str, m_sa_data, m_sa_size, lo, hi, depth, ch);
}

static inline
intptr_t
sa_strcmp(const byte* x, size_t xn, const byte* y, size_t yn, size_t i) {
	size_t n = std::min(xn, yn);
	for (; i < n; ++i) {
		if (x[i] != y[i])
			return  x[i] - y[i];
	}
	return  xn - yn;
}

SuffixDictCacheDFA::MatchStatus
SuffixDictCacheDFA::sa_match_range(size_t lo, size_t hi, size_t depth, const byte_t* input, size_t len)
const {
	assert(lo < hi);
	assert(hi <= m_sa_size);
	const int* sa = m_sa_data;
	const byte_t* str = m_str;
	const size_t  saLen = m_sa_size;
	assert(sa[lo] + depth <= saLen);
	while (lo < hi) {
		assert(sa[lo] + depth <= saLen);
		size_t mid = (lo + hi) / 2;
		size_t sam = sa[mid];
		assert(sam + depth <= saLen);
		intptr_t cmp = sa_strcmp(str + sam, saLen - sam, input, len, depth);
		if (cmp < 0)
			lo = mid + 1;
		else if (cmp > 0)
			hi = mid;
		else
			goto Found;
	}
	if (lo > 0)
		goto TwoMatch;
	else {
		size_t saLo = sa[lo];
		return {lo, lo+1,
			unmatchPos(str+saLo, input, depth, std::min(saLen-saLo, len))
		};
	}
Found:
	{
		size_t lo1 = lo, hi1 = hi;
		while (lo1 < hi1) {
			assert(sa[lo1] + depth < saLen);
			size_t mid = (lo1 + hi1) / 2;
			size_t sam = sa[mid];
			assert(sam + depth < saLen);
			intptr_t cmp = sa_strcmp(str + sam, saLen - sam, input, len, depth);
			if (cmp <= 0)
				lo1 = mid + 1;
			else
				hi1 = mid;
		}
		size_t lo2 = lo, hi2 = hi;
		while (lo2 < hi2) {
			assert(sa[lo2] + depth < saLen);
			size_t mid = (lo2 + hi2) / 2;
			size_t sam = sa[mid];
			assert(sam + depth <= saLen);
			intptr_t cmp = sa_strcmp(str + sam, saLen - sam, input, len, depth);
			if (cmp < 0)
				lo2 = mid + 1;
			else
				hi2 = mid;
		}
		if (terark_unlikely(lo1 < lo2))
			return {lo1, lo2, len};
		lo = lo1;
	}
TwoMatch:
	{
		size_t saLo = sa[lo-1];
		size_t saHi = sa[lo-0];
		size_t mcLo = unmatchPos(str+saLo, input, depth, std::min(saLen-saLo,len));
		size_t mcHi = unmatchPos(str+saHi, input, depth, std::min(saLen-saHi,len));
		if (mcLo > mcHi)
			return {lo-1, lo+0, mcLo};
		else
			return {lo-0, lo+1, mcHi};
	}
}

std::pair<size_t, size_t>
SuffixDictCacheDFA::sa_match_exact(size_t lo, size_t hi, size_t depth, const byte_t* input, size_t len)
const {
	assert(lo < hi);
	assert(hi <= m_sa_size);
	const int* sa = m_sa_data;
	const byte_t* str = m_str;
	const size_t  saLen = m_sa_size;
	assert(sa[lo] + depth <= saLen);
	while (lo < hi) {
		assert(sa[lo] + depth <= saLen);
		size_t mid = (lo + hi) / 2;
		size_t sam = sa[mid];
		assert(sam + depth <= saLen);
		if (sa_strcmp(str + sam, saLen - sam, input, len, depth) < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	if (lo > 0) {
		size_t saLo = sa[lo-1];
		size_t saHi = sa[lo-0];
		size_t mcLo = unmatchPos(str+saLo, input, depth, std::min(saLen-saLo,len));
		size_t mcHi = unmatchPos(str+saHi, input, depth, std::min(saLen-saHi,len));
		if (mcLo > mcHi)
			return {lo-1, mcLo};
		else
			return {lo-0, mcHi};
	}
	else {
		size_t saLo = sa[lo];
		return {lo, unmatchPos(str+saLo, input, depth, std::min(saLen-saLo, len))};
	}
}

void SuffixDictCacheDFA::sa_print_stat() const {
	printf("total string length: %10zd\n", m_sa_size);
	printf("SuffixArray  memory: %10zd\n", m_sa_size * (1 + sizeof(saidx_t)));
#ifdef SuffixDictCacheDebug
	if (m_bm) {
		printf("dynamic trie states: %10zd\n", m_bm->states.size());
		printf("dynamic free states: %10zd\n", m_bm->num_free_states());
		printf("dynamic trie memory: %10zd, avg size per state: %f\n"
			, m_bm->mem_size(), m_bm->mem_size() / (m_bm->states.size()+0.0));
	}
#endif
	if (m_da) {
		printf("double array trie states: %10zd\n", m_da->states.size());
		printf("double array free states: %10zd\n", m_da->num_free_states());
		printf("double array fill  ratio: %10.8f\n", 1.0*m_da->num_used_states()/m_da->states.size());
		printf("double array trie memory: %10zd, avg size per state: %f\n"
			, m_da->mem_size(), m_da->mem_size() / (m_da->states.size()+0.0));
	}
}

void SuffixDictCacheDFA::da_swapout(FileStream& f) {
	f.ensureWrite(m_da->states.data(), m_da->states.full_mem_size());
	free(m_da->states.data());
	m_da->states.risk_set_data(nullptr);
}

void SuffixDictCacheDFA::da_swapin(FileStream& f) {
	assert(m_da->states.data() == nullptr);
	AutoFree<MyDoubleArrayNode> da(m_da->states.capacity());
	f.ensureRead(da.p, m_da->states.full_mem_size());
	m_da->states.risk_set_data(da.release());
}

///////////////////////////////////////////////////////////////////////////////
#if defined(Enable_HashSuffixDictCacheDFA)

struct HashSuffixDictCacheDFA::TabValue {
	uint32_t lo;
	uint32_t hi    : 31; // or DA root
	uint32_t hasDA :  1;
	uint32_t link;
	uint32_t root() const { return hi; }
	void set_root(uint32_t root) { hi = root; hasDA = true; }

	TabValue(uint32_t lo1, uint32_t hi1, uint32_t link1)
		: lo(lo1), hi(hi1), link(link1)
	{
		hasDA = 0;
	}
};

static const size_t MinMatchLen = 6;
static const uint64_t SixBytesMask = uint64_t(-1) >> 16;

static inline uint64_t HashBytes(uint64_t bytes, unsigned shift) {
	size_t kMul = 0x1e35a7bd;
	return (bytes * kMul) >> shift;
}
static inline unsigned HashPtr(const byte* p, unsigned shift) {
	return HashBytes(unaligned_load<uint64_t>(p) & SixBytesMask, shift);
}

HashSuffixDictCacheDFA::HashSuffixDictCacheDFA() {
}

HashSuffixDictCacheDFA::~HashSuffixDictCacheDFA() {
}

void
HashSuffixDictCacheDFA::bfs_build_cache(size_t minFreq, size_t maxBfsDepth) {
	profiling pf;
	auto sa = m_sa_data;
	auto str = m_str;
	auto sa_size = m_sa_size;
	long long t0 = pf.now();
// build hash table
{
	auto getKey = [=](int x){ return str + x; };
	auto cmpKey = [ ](const byte* x, const byte* y) {
		return memcmp(x, y, MinMatchLen) < 0;
	};
	size_t sixByteKeys = 0;
	size_t i = 0;
	while (i + MinMatchLen <= sa_size) {
		size_t j = i+upper_bound_ex_0(sa+i, sa_size-i, str+sa[i], getKey, cmpKey);
		i = j;
		sixByteKeys++;
	}
	fprintf(stderr, "sa_size = %zd, sixByteKeys = %zd\n", sa_size, sixByteKeys);
	if (sixByteKeys * 8 >= sa_size) {
		SuffixDictCacheDFA::bfs_build_cache(minFreq, maxBfsDepth);
		return;
	}
	unsigned const bits = terark_bsr_u64(sixByteKeys-1) + 1;
	unsigned const shift = 64 - bits;
	m_nodes.reserve(sixByteKeys);
	m_bucket.resize_fill(size_t(1) << bits, UINT32_MAX);
	i = 0;
	while (i + MinMatchLen <= sa_size) {
		size_t j = i+upper_bound_ex_0(sa+i, sa_size-i, str+sa[i], getKey, cmpKey);
		m_nodes.push_back({uint32_t(i), uint32_t(j), UINT32_MAX});
		i = j;
	}
	m_nodes.shrink_to_fit();
	for (i = 0; i < m_nodes.size(); ++i) {
		size_t h = HashPtr(str+sa[m_nodes[i].lo], shift);
		m_nodes[i].link = m_bucket[h];
		m_bucket[h] = i;
	}
	m_shift = shift;

	valvec<int> hist(512, 0);
	for (i = 0; i < m_bucket.size(); ++i) {
		size_t len = 0;
		unsigned p = m_bucket[i];
		while (UINT32_MAX != p) {
			len++;
			p = m_bucket[p];
		}
		if (hist.size() < len+1)
			hist.resize(len+1);
		hist[len]++;
	}
	fprintf(stderr, "HashSuffixDictCacheDFA: hash histogram:\n");
	for (i = 0; i < hist.size(); ++i) {
		int cnt = hist[i];
		if (cnt) {
			fprintf(stderr, "len =%3zd  cnt =%5d\n", i, cnt);
		}
	}
}

	auto trie = new MyBitmapSmartTrie();
	m_bm.reset(trie);
	trie->erase_all();
	trie->states.erase_all();
	trie->states.reserve(m_nodes.size()/4);
	size_t numRoots = 0;

// BFS build Trie
{
	valvec<BfsQueueElem> q1, q2;
	for (size_t i = 0; i < m_nodes.size(); ++i) {
		auto lo = m_nodes[i].lo;
		auto hi = m_nodes[i].hi;
		if (hi - lo >= 64) {
			const size_t root = trie->states.size();
			trie->states.push_back({});
			trie->states[root].m_suffixLow = lo;
			trie->states[root].m_suffixHig = hi;
		//	size_t bound = sa_size - std::max(sa[lo],sa[hi-1]);
		//	size_t depth = unmatchPos(str+sa[lo], str+sa[hi-1], 6, bound);
			size_t depth = MinMatchLen;
			m_nodes[i].set_root(uint32_t(root)); // use hi as root
			q1.push_back({uint32_t(depth), uint32_t(root)});
		}
	}
	if (q1.empty()) {
		m_bm.reset();
		return;
	}
	numRoots = q1.size();
	const size_t maxBfsDepth = 5;
	const size_t minFreq = 8;
	AutoFree<CharTarget<size_t> > children(256);
	size_t bfsDepth = 0;
	while (!q1.empty() && bfsDepth < maxBfsDepth) {
		for(auto e : q1) {
			size_t lo = trie->states[e.state].m_suffixLow;
			size_t hi = trie->states[e.state].m_suffixHig;
			size_t depth = e.depth;
			if (sa[lo] + depth < sa_size) {
				size_t saLo = sa[lo];
				size_t saHi = sa[hi-1];
				if (str[saLo + depth] == str[saHi + depth]) {
					size_t maxPos = std::min(saLo + depth + MaxDepth, sa_size);
					do ++depth;
					while (	    saLo + depth  < maxPos &&
							str[saLo + depth] == str[saHi + depth] );
					trie->states[e.state].setZstrLen(depth - e.depth);
				//	trie->states[e.state].set_pzip_bit();
				}
			}
			if (sa[lo] + depth >= sa_size) {
				lo++;
			}
			size_t childcnt = 0;
			while (lo < hi) {
				byte_t c = str[sa[lo] + depth];
				size_t u = sa_upper_bound(lo, hi, depth, c);
				if (u - lo >= minFreq) {
					size_t child = trie->new_state();
					q2.push_back({uint32_t(depth) + 1, uint32_t(child)});
					children[childcnt].ch = c;
					children[childcnt].target = child;
					trie->states[child].m_suffixLow = lo;
					trie->states[child].m_suffixHig = u;
					childcnt++;
				}
				lo = u;
			}
			trie->add_all_move(e.state, children, childcnt);
		//	int keepStackFrameForMSVC = 1;
		}
		q1.swap(q2);
		q2.erase_all();
		bfsDepth++;
	}
	trie->shrink_to_fit();
}
	long long t1 = pf.now();
	valvec<uint32_t> t2d, d2t;
	auto dart = new MyDoubleArrayTrie();
	m_da.reset(dart);
{
	valvec<uint32_t> roots(numRoots, valvec_no_init());
	for (size_t i = 0; i < numRoots; ++i) {
		roots[i] = uint32_t(i);
	}
	dart->build_from(*trie, roots, d2t, t2d, "DFS", 0);
}
	for(size_t i = 0; i < d2t.size(); ++i) {
		size_t j = d2t[i];
		if (j < trie->states.size()) {
			dart->states[i].setZstrLen(trie->states[j].getZstrLen());
			dart->states[i].m_suffixLow = trie->states[j].m_suffixLow;
			dart->states[i].m_suffixHig = trie->states[j].m_suffixHig;
		}
	}
	long long t2 = pf.now();
//	this->write_dot_file("suffix-array.dot");
	if (g_suffixDictShowState) {
		printf("hash: build dynamic trie time: %f seconds\n", pf.sf(t0,t1));
		printf("hash: build double array time: %f seconds\n", pf.sf(t1,t2));
		sa_print_stat();
	}
	m_bm.reset();
}

void
HashSuffixDictCacheDFA::pfs_build_cache(size_t minFreq) {
	THROW_STD(invalid_argument, "Not Implemented");
}

SuffixDictCacheDFA::MatchStatus
HashSuffixDictCacheDFA::da_match_max_length(const byte* input, size_t len, size_t minFreq)
const {
	if (terark_unlikely(len < MinMatchLen)) {
		return {0,0,0};
	}
	SuffixDictCacheDFA_Profiling_t0();
	const byte_t* str = m_str;
	const int* sa = m_sa_data;
	size_t lo = 0, hi = 0;
	if (!m_nodes.empty()) {
		SuffixDictCacheDFA_Profiling(hash);
		unsigned h = HashPtr(input, m_shift);
		unsigned p = m_bucket[h];
		auto nodes = m_nodes.data();
	//	uint64_t key48 = unaligned_load<uint64_t>(input) & SixBytesMask;
		while (UINT32_MAX != p) {
			lo = nodes[p].lo;
		//	uint64_t hit48 = unaligned_load<uint64_t>(str + sa[lo]) & SixBytesMask;
		//	if (hit48 == key48)
			if (memcmp(input, str + sa[lo], MinMatchLen) == 0)
			{
				hi = nodes[p].hi;
				if (nodes[p].hasDA)
					goto SearchTrie; // now hi is trie root
				else
					return s_sa_match(str, sa, m_sa_size,
							lo, hi, MinMatchLen, input, len, minFreq);
			}
			p = nodes[p].link;
		}
		return {0,0,0}; // not found
	}
SearchTrie:
	SuffixDictCacheDFA_Profiling(dfa);
	const auto lstates = m_da->states.data();
	size_t state = hi; // now hi is trie root
	assert(lo == lstates[state].m_suffixLow);
	hi = lstates[state].m_suffixHig;
	size_t pos = MinMatchLen;
	while (pos < len) {
		if (size_t zlen = lstates[state].getZstrLen()) {
			size_t zend = std::min(len, pos + zlen);
			for (auto zptr = str + sa[lo]; pos < zend; pos++) {
				if (zptr[pos] != input[pos])
					return{ lo, hi, pos };
			}
			if (terark_unlikely(pos == len))
				return{ lo, hi, pos };
		}
		size_t  child = lstates[state].m_child0 + input[pos];
		if (terark_likely(lstates[child].m_parent == state)) {
			state = child;
			lo = lstates[child].m_suffixLow;
			hi = lstates[child].m_suffixHig;
			assert(hi - lo >= minFreq); // cache should has a bigger freq
			assert(str[sa[lo] + pos] == input[pos]);
			pos++;
		}
		else {
			SuffixDictCacheDFA_Profiling(sa);
			return s_sa_match(str, sa, m_sa_size,
							  lo, hi, pos, input, len, minFreq);
		}
	}
	return{ lo, hi, pos };
}

#endif // Enable_HashSuffixDictCacheDFA

} // namespace terark

