//#define USE_SUFFIX_ARRAY_TRIE

#if defined(USE_SUFFIX_ARRAY_TRIE)
#include "suffix_array_trie.hpp"
#include <divsufsort.h>
#else
namespace terark {
#define SuffixTrieCacheDFA DummySuffixTrieCacheDFA
	class DummySuffixTrieCacheDFA {};
}
#endif

#include "nest_louds_trie_inline.hpp"
#include "dfa_mmap_header.hpp"
#include "tmplinst.hpp"
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>
#include <terark/num_to_str.hpp>

// This is initially designed for using NestLoudsTrie to compress long keys as
// database record/value, it is proved this is a bad idea.
// Now database record/value is compressed by DictZibBlobStore, but this idea
// still has a few benefits: to prevent very rare long keys to fullfill the
// length bits -- (m_next_link[rank1(i)] | m_label[i])
#define NestLoudsTrie_EnableDelim

namespace terark {

const size_t MaxShortStrLen = 3;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

NestLoudsTrieConfig::NestLoudsTrieConfig() {
	nestLevel = 4;
	minFragLen = 8;
	maxFragLen = 512;
	corestrCompressLevel = 1;
	minLinkStrLen = 3;

	// only used when USE_SUFFIX_ARRAY_TRIE is defined
	// but always keep it for binary compatible
	suffixTrie.reset(NULL);
	saFragMinFreq = 0;
	bzMinLen = 0;
	bestZipLenArr = NULL;
	// ------------------------

	flags.set1(optSearchDelimForward);
	flags.set1(optCutFragOnPunct);

	// compression rate is more excellent
	flags.set1(optUseDawgStrPool);
	bestDelimBits.set1('\0');
	bestDelimBits.set1('\r');
	bestDelimBits.set1('\n');

	tmpLevel = 0;
	isInputSorted = false;
	debugLevel = 0;
	nestScale = 8;
	useMixedCoreLink = true;
	enableQueueCompression = true;
	speedupNestTrieBuild = false;
}

NestLoudsTrieConfig::~NestLoudsTrieConfig() {
	if (bestZipLenArr)
		::free(bestZipLenArr);
}

void NestLoudsTrieConfig::initFromEnv() {
    if (const char* env = getenv("NestLoudsTrie_debugLevel")) {
        debugLevel = (byte)atoi(env);
    }
    if (const char* env = getenv("NestLoudsTrie_nestLevel")) {
        nestLevel = strtol(env, NULL, 10);
    }
    if (const char* env = getenv("NestLoudsTrie_nestScale")) {
        nestScale = strtol(env, NULL, 10);
    }
    if (const char* env = getenv("NestLoudsTrie_minFragLen")) {
        minFragLen = strtol(env, NULL, 10);
    }
    if (const char* env = getenv("NestLoudsTrie_maxFragLen")) {
        maxFragLen = strtol(env, NULL, 10);
    }
    if (const char* env = getenv("NestLoudsTrie_saFragMinFreq")) {
        saFragMinFreq = strtol(env, NULL, 10);
    }
    if (const char* env = getenv("NestLoudsTrie_bzMinLen")) {
        bzMinLen = strtol(env, NULL, 10);
    }
    if (minFragLen > 256) {
        fprintf(stderr, "NestLoudsTrie_minFragLen=%d is too large, set to 256\n", minFragLen);
        minFragLen = 256;
    }
    if (maxFragLen < 16) {
        fprintf(stderr, "NestLoudsTrie_maxFragLen=%d is too small, set to 16\n", maxFragLen);
        maxFragLen = 16;
    }
    if (const char* env = getenv("NestLoudsTrie_bestDelim")) {
    	fprintf(stderr, "NestLoudsTrie_bestDelim=%s\n", env);
		setBestDelims(env);
    }
    if (const char* env = getenv("NestLoudsTrie_isInputSorted")) {
    	isInputSorted = atoi(env) ? true : false;
    }
    if (const char* env = getenv("NestLoudsTrie_isSearchDelimForward")) {
    	flags.set(optSearchDelimForward, atoi(env) ? 1 : 0);
    }
    if (const char* env = getenv("NestLoudsTrie_optCutFragOnPunct")) {
    	flags.set(optCutFragOnPunct, atoi(env) ? 1 : 0);
    }
	if (const char* env = getenv("NestLoudsTrie_minLinkStrLen")) {
		minLinkStrLen = atoi(env);
	}
	if (const char* env = getenv("NestLoudsTrie_corestrCompressLevel")) {
		corestrCompressLevel = atoi(env);
	}
	if (const char* env = getenv("NestLoudsTrie_tmpDir")) {
		tmpDir = env;
	}
	if (const char* env = getenv("NestLoudsTrie_tmpLevel")) {
		tmpLevel = strtol(env, NULL, 10);
	}
	enableQueueCompression = getEnvBool("NestLoudsTrie_enableQueueCompression", true);
	useMixedCoreLink = getEnvBool("NestLoudsTrie_useMixedCoreLink", true);
	speedupNestTrieBuild = getEnvBool("NestLoudsTrie_speedupNestTrieBuild", false);
	if (debugLevel >= 1) {
		fprintf(stderr, "debugLevel            = %d\n", debugLevel);
		fprintf(stderr, "optSearchDelimForward = %d\n", flags[optSearchDelimForward]);
		fprintf(stderr, "optCutFragOnPunct     = %d\n", flags[optCutFragOnPunct]);
		fprintf(stderr, "nestLevel             = %d\n", nestLevel);
		fprintf(stderr, "nestScale             = %d\n", nestScale);
		fprintf(stderr, "minFragLen            = %d\n", minFragLen);
		fprintf(stderr, "maxFragLen            = %d\n", maxFragLen);
		fprintf(stderr, "minLinkStrLen         = %d\n", minLinkStrLen);
		fprintf(stderr, "saFragMinFreq         = %d\n", saFragMinFreq);
		fprintf(stderr, "corestrCompressLevel  = %d\n", corestrCompressLevel);
		fprintf(stderr, "isInputSorted         = %d\n", isInputSorted);
		fprintf(stderr, "enableQueueCompression= %d\n", enableQueueCompression);
		fprintf(stderr, "useMixedCoreLink      = %d\n", useMixedCoreLink);
		fprintf(stderr, "speedupNestTrieBuild  = %d\n", speedupNestTrieBuild);
	}
}

void NestLoudsTrieConfig::setBestDelims(const char* delims) {
    size_t len = strlen(delims);
    std::string bestDelim;
    for (size_t i = 0; i < len; ) {
    	if ('\\' == delims[i]) {
    		byte_t ch;
    		switch (delims[++i]) {
    		case '0': ch = '\0'; ++i; break;
    		case 'n': ch = '\n'; ++i; break;
    		case 'r': ch = '\r'; ++i; break;
    		case 't': ch = '\t'; ++i; break;
    		case 'v': ch = '\v'; ++i; break;
    		case '\\':ch = '\\'; ++i; break;
    		case 'x':
    			ch = hexlcast(delims+i+1, 2);
    			i += 3;
    			break;
    		default:
    			ch = delims[i];
    			break;
    		}
    		bestDelim.push_back(ch);
    	} else {
    		bestDelim.push_back(delims[i++]);
    	}
    }
//	bestDelimBits.fill_all(0);
    for (byte_t ch : bestDelim) bestDelimBits.set1(ch);
}

static int getRealTmpLevel(int tmpLevel, size_t strnum, size_t poolsize) {
	assert(strnum > 0);
	if (0 == tmpLevel) {
		size_t avglen = poolsize / strnum;
		// adjust tmpLevel for linkVec, which is proportional to num of keys
		if (avglen <= 50) {
			// not need any mem in BFS, instead 8G file of 4G mem (linkVec)
			// this reduce 10% peak mem when avg keylen is 24 bytes
			if (avglen <= 30) {
				// write str data(each len+data) of nestStrVec to tmpfile
				return 4;
			} else {
				return 3;
			}
		} else if (avglen <= 80) {
			return 2;
		}
		return 1; // avglen is very long
	}
	return tmpLevel;
}
static int getRealTmpLevel(const NestLoudsTrieConfig& conf, size_t strnum, size_t poolsize) {
	return getRealTmpLevel(conf.tmpLevel, strnum, poolsize);
}

static int getRealMinLinkStrLen(const NestLoudsTrieConfig& conf) {
    if (conf.useMixedCoreLink)
        return std::max<int>(conf.minLinkStrLen, 2);
    else
        return conf.minLinkStrLen;
}
/////////////////////////////////////////////////////////////////////////////

namespace {

#pragma pack(push, 1)
struct DfsElem {
	uint32_t lo;
	uint32_t hi;
	uint32_t pos;
	DfsElem(size_t lo, size_t hi, size_t pos)
		: lo(uint32_t(lo))
		, hi(uint32_t(hi))
		, pos(uint32_t(pos))
	{}
};
#pragma pack(pop)

#if defined(USE_SUFFIX_ARRAY_TRIE)
size_t sa_upper_bound(const byte* str, const int* sa,
						size_t lo, size_t hi, size_t pos) {
	assert(lo < hi);
	byte_t ch = str[sa[lo] + pos];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		byte_t hitChr = str[sa[mid] + pos];
		if (hitChr <= ch) // upper bound
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}
#endif

#if defined(USE_SUFFIX_ARRAY_TRIE)
void
ComputeBestZipLen(const SortableStrVec& strVec, size_t minZipLen, uint16_t* lenArr) {
	AutoFree<int> sa(strVec.str_size());
	const byte_t* str = strVec.m_strpool.data();
	const size_t  saLen = strVec.str_size();
	profiling pf;
	long long t0 = pf.now();
	if (divsufsort(str, sa, saLen) != 0) {
		throw std::bad_alloc();
	}
	long long t1 = pf.now();
	std::fill_n(lenArr, saLen, (uint16_t)minZipLen);
	valvec<DfsElem> stack(256, valvec_reserve());
	stack.emplace_back(0, strVec.str_size(), 0);
	size_t minFreq = 2;
	size_t maxLen = 1024;
	while (!stack.empty()) {
		DfsElem top = stack.pop_val();
		size_t lo = top.lo;
		size_t hi = top.hi;
		size_t pos = top.pos;
		if (pos > minZipLen) {
			for(size_t i = lo; i < hi; ++i)
				lenArr[sa[lo]] = pos;
		}
		while (lo + minFreq <= hi) {
			while (terark_unlikely(sa[lo] + pos >= saLen)) {
				if (lo + minFreq <= hi)
					lo++;
				else
					goto Done;
			}
			{
				size_t lo2 = sa_upper_bound(str, sa, lo, hi, pos);
				size_t end = std::min(saLen - std::max(sa[lo], sa[lo2-1]), maxLen);
				if (pos < end) {
					size_t pos2 = unmatchPos(str + sa[lo], str + sa[lo2-1], pos+1, end);
					if (lo2 - lo >= minFreq && pos2 > pos) {
						stack.emplace_back(lo, lo2, pos2);
					}
				}
				lo = lo2;
			}
			Done:;
		}
	}
	long long t2 = pf.now();
	for(size_t i = 0; i < strVec.m_index.size(); ++i) {
		size_t pos = strVec.m_index[i].offset;
		size_t end = strVec.m_index[i].endpos();
		for (; pos < end; ++pos) {
			if (lenArr[pos] > end - pos)
				lenArr[pos] = end - pos;
		}
	}
//	long long t3 = pf.now();
	printf("ComputeBestZipLen: divsufsort = %f sec, compute = %f sec\n", pf.sf(t0,t1), pf.sf(t1, t2));
}
#endif

} // anonymous namespace

/////////////////////////////////////////////////////////////////////////////
template<class T>
class fixed_vec {
    T*     m_beg;
    T*     m_end;
    TERARK_IF_DEBUG(T* m_limit,);
public:
    typedef T value_type;
    fixed_vec(T* q, size_t cap) : m_beg(q), m_end(q) {
        TERARK_IF_DEBUG(m_limit = q + cap,);
    }
    void push_back(const T x) {
        assert(m_end < m_limit);
        T* endp = m_end;
        *endp = x;
        m_end = endp + 1;
    }
    void append(const T* arr, size_t len) {
        assert(m_end + len <= m_limit);
        assert(len > 0);
        T* endp = m_end;
    #if 0 // len is large
        memcpy(endp, arr, len);
        m_end = endp + len;
    #else // len is small, this is true
        do *endp++ = *arr++, len--; while (len);
        m_end = endp;
    #endif
    }
    size_t size() const { return m_end - m_beg; }
    T* begin() { return m_beg; }
    T* end  () { return m_end; }
};
/////////////////////////////////////////////////////////////////////////////

template<class RankSelect, class RankSelect2, bool FastLabel>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::NestLoudsTrieTpl() {
	m_label_data = NULL;
	m_core_data = NULL;
	m_next_trie = NULL;
	m_core_size = 0;
	m_core_min_len = 0;
	m_core_len_bits = 0;
	m_core_len_mask = 0;
	m_core_max_link_val = 0;
	m_total_zpath_len = 0;
	m_max_layer_id = 0;
	m_max_layer_size = 0;
    m_max_strlen = 0;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
NestLoudsTrieTpl(const NestLoudsTrieTpl& y)
	: m_louds(y.m_louds)
	, m_is_link(y.m_is_link)
	, m_next_link(y.m_next_link)
{
	m_core_size = y.m_core_size;
	AutoFree<byte_t> label_data(total_states() + m_core_size, y.m_label_data);
	if (y.m_next_trie) {
		m_next_trie = new NestLoudsTrieTpl<RankSelect>(*y.m_next_trie);
	}
	m_label_data = label_data.release();
	if (y.m_core_data) {
		m_core_data = m_label_data + total_states();
	}
	else {
		m_core_data = NULL;
	}
	m_core_min_len = y.m_core_min_len;
	m_core_len_bits = y.m_core_len_bits;
	m_core_len_mask = y.m_core_len_mask;
	m_core_max_link_val = y.m_core_max_link_val;
	m_total_zpath_len = y.m_total_zpath_len;
	m_max_layer_id = y.m_max_layer_id;
	m_max_layer_size = y.m_max_layer_size;
    m_max_strlen = y.m_max_strlen;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>&
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
operator=(const NestLoudsTrieTpl& y) {
	if (&y != this) {
		this->~NestLoudsTrieTpl();
		this->risk_release_ownership();
		NestLoudsTrieTpl(y).swap(*this);
	}
	return *this;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
risk_release_ownership() {
	// this call also reset NestLoudsTrie to constructed stats
	m_louds.risk_release_ownership();
	m_is_link.risk_release_ownership();
	m_next_link.risk_release_ownership();
	m_label_data = NULL;
	m_core_data = NULL;
	m_core_size = 0;
	m_core_len_mask = 0;
	m_core_len_bits = 0;
	m_core_min_len = 0;
	m_core_max_link_val = 0;
	m_total_zpath_len = 0;
	m_layer_id.clear();
	m_layer_rank.clear();
	m_layer_ref.clear();
	m_max_layer_id = 0;
	m_max_layer_size = 0;
    m_max_strlen = 0;
	if (m_next_trie)
		m_next_trie->risk_release_ownership();
}

template<class RankSelect, class RankSelect2, bool FastLabel>
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
~NestLoudsTrieTpl() {
	if (m_label_data)
		free(m_label_data);
	delete m_next_trie;
	m_next_trie = NULL;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
swap(NestLoudsTrieTpl& y) {
	m_louds.swap(y.m_louds);
	m_is_link.swap(y.m_is_link);
	m_next_link.swap(y.m_next_link);
	std::swap(m_label_data, y.m_label_data);
	std::swap(m_core_data, y.m_core_data);
	std::swap(m_core_size, y.m_core_size);
	std::swap(m_core_len_bits, y.m_core_len_bits);
	std::swap(m_core_len_mask, y.m_core_len_mask);
	std::swap(m_core_min_len, y.m_core_min_len);
	std::swap(m_core_max_link_val, y.m_core_max_link_val);
	std::swap(m_total_zpath_len, y.m_total_zpath_len);
	std::swap(m_next_trie, y.m_next_trie);
	m_layer_id.swap(y.m_layer_id);
	m_layer_rank.swap(y.m_layer_rank);
	m_layer_ref.swap(y.m_layer_ref);
	std::swap(m_max_layer_id, y.m_max_layer_id);
	std::swap(m_max_layer_size, y.m_max_layer_size);
	std::swap(m_max_strlen, y.m_max_strlen);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::mem_size() const {
	return m_louds.mem_size()
		 + m_is_link.mem_size()
		 + m_next_link.mem_size()
		 + total_states() // m_label_data
		 + (m_next_trie ? m_next_trie->mem_size() : m_core_size)
		 ;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::nest_level() const {
	size_t num = 1;
	for (auto trie = m_next_trie; trie; trie = trie->m_next_trie) {
		num++;
	}
	return num;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::core_mem_size() const {
	auto trie = m_next_trie;
	for (; trie; trie = trie->m_next_trie) {
		if (trie->m_core_size)
			return trie->m_core_size;
	}
	return 0;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
get_parent(size_t child) const {
	assert(child > 0); // 0 is root
	assert(child < total_states());
	return m_louds.select1(child) - child - 1;
//	return m_louds.rank0(m_louds.select1(child)) - 1;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
fstring
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
get_core_str(size_t node_id) const {
	assert(node_id > 0);
	assert(node_id < m_is_link.size());
	assert(NULL != m_core_data);
	assert(m_core_size > 0);
	uint64_t linkVal = get_link_val(node_id);
	size_t length = size_t(linkVal  & m_core_len_mask) + m_core_min_len;
	size_t offset = size_t(linkVal >> m_core_len_bits);
	assert(offset < m_core_size);
	assert(offset + length <= m_core_size);
	return fstring(m_core_data + offset, length);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
        restore_string_append(size_t node_id, valvec<byte_t>* str) const {
	tpl_restore_string_append(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
        restore_string_append(size_t node_id, std::string* str) const {
	tpl_restore_string_append(node_id, str);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
        restore_dawg_string_append(size_t node_id, valvec<byte_t>* str) const {
	tpl_restore_dawg_string_append(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
        restore_dawg_string_append(size_t node_id, std::string* str) const {
	tpl_restore_dawg_string_append(node_id, str);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_string(size_t node_id, valvec<byte_t>* str) const {
	str->erase_all();
	restore_string_append(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_string(size_t node_id, std::string* str) const {
	str->resize(0);
	restore_string_append(node_id, str);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrBuf>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
tpl_restore_string_append(size_t node_id, StrBuf* str) const {
	assert(NULL != str);
	assert(node_id < m_is_link.size());
	str->reserve(64);
	tpl_restore_string_loop(node_id, str);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_dawg_string(size_t node_id, valvec<byte_t>* str) const {
	str->erase_all();
	tpl_restore_dawg_string_append(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_dawg_string(size_t node_id, std::string* str) const {
	str->resize(0);
	tpl_restore_dawg_string_append(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrBuf>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
tpl_restore_dawg_string_append(size_t node_id, StrBuf* str) const {
    assert(NULL != str);
    assert(node_id < m_is_link.size());
    str->reserve(64);
    tpl_restore_string_loop_ex(node_id, str, true);
}

///@{
///@param node_id node id of nest trie
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_next_string(size_t node_id, valvec<byte_t>* str) const {
	tpl_restore_next_string(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_next_string(size_t node_id, std::string* str) const {
	tpl_restore_next_string(node_id, str);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrBuf>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
tpl_restore_next_string(size_t node_id, StrBuf* str) const {
	assert(NULL != str);
	assert(node_id > 0);
	assert(node_id < m_is_link.size());
	str->resize(0);
	str->reserve(64);
    uint64_t linkVal = this->get_link_val(node_id);
    typedef typename StrBuf::value_type char_type;
    BOOST_STATIC_ASSERT(sizeof(char_type) == 1);
    auto coreData = (const char_type*)m_core_data;
    if (linkVal < m_core_max_link_val) {
        size_t length = size_t(linkVal &  m_core_len_mask) + m_core_min_len;
        size_t offset = size_t(linkVal >> m_core_len_bits);
        assert(offset < m_core_size);
        assert(offset + length <= m_core_size);
        str->append((const char_type*)coreData + offset, length);
    }
    else {
        assert(NULL != m_next_trie);
        linkVal -= m_core_max_link_val;
        m_next_trie->restore_string_loop(size_t(linkVal), str);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
fstring
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
get_zpath_data(size_t node_id, MatchContext* ctx) const {
    assert(NULL != ctx);
    assert(node_id > 0);
    assert(node_id < m_is_link.size());
    assert(m_is_link[node_id]);
    uint64_t linkVal = get_link_val(node_id);
    if (linkVal < m_core_max_link_val) {
        assert(NULL != m_core_data);
        size_t length = size_t(linkVal &  m_core_len_mask) + m_core_min_len;
        size_t offset = size_t(linkVal >> m_core_len_bits);
        assert(offset < m_core_size);
        assert(offset + length <= m_core_size);
        if (FastLabel)
            return fstring(m_core_data + offset, length);
        else
            return fstring(m_core_data + offset + 1, length - 1);
    }
    else {
        assert(NULL != m_next_trie);
        size_t nest_id = size_t(linkVal - m_core_max_link_val);
        ctx->zbuf.reserve(256);
        fixed_vec<byte_t> buf(ctx->zbuf.data(), 256);
        m_next_trie->tpl_restore_string_loop(nest_id, &buf);
        if (FastLabel)
            return fstring(buf.begin(), buf.size());
        else
            return fstring(buf.begin() + 1, buf.size() - 1);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
size_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
getZpathFixed(size_t node_id, byte_t* buf, size_t cap) const {
    assert(NULL != buf);
    assert(node_id > 0);
    assert(node_id < m_is_link.size());
    assert(m_is_link[node_id]);
    uint64_t linkVal = get_link_val(node_id);
    if (linkVal < m_core_max_link_val) {
        assert(NULL != m_core_data);
        size_t length = size_t(linkVal &  m_core_len_mask) + m_core_min_len;
        size_t offset = size_t(linkVal >> m_core_len_bits);
        assert(offset < m_core_size);
        assert(offset + length <= m_core_size);
        assert(length >= 2);
        if (FastLabel) {
            const byte_t* src = m_core_data + offset;
            size_t cnt = length;
            do *buf++ = *src++, cnt--; while (cnt);
            return length;
        }
        else {
            const byte_t* src = m_core_data + offset + 1;
            size_t cnt = --length;
            do *buf++ = *src++, cnt--; while (cnt);
            return length;
        }
    }
    else {
        assert(NULL != m_next_trie);
        size_t nest_id = size_t(linkVal - m_core_max_link_val);
        fixed_vec<byte_t> zbuf(buf, cap);
        m_next_trie->tpl_restore_string_loop(nest_id, &zbuf);
        assert(zbuf.size() >= 2);
        if (FastLabel)
            return zbuf.size();
        else {
            size_t len = zbuf.size()-1;
            size_t cnt = len;
            do buf[0] = buf[1], buf++, cnt--; while (cnt);
            return len;
        }
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
intptr_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
matchZpath(size_t node_id, const byte_t* str, size_t slen) const {
    assert(node_id > 0);
    assert(node_id < m_is_link.size());
    assert(m_is_link[node_id]);
    uint64_t linkVal = get_link_val(node_id);
    size_t coreMaxLinkVal = m_core_max_link_val;
    if (linkVal < coreMaxLinkVal) {
        assert(NULL != m_core_data);
        size_t length = size_t(linkVal &  m_core_len_mask) + m_core_min_len;
        size_t offset = size_t(linkVal >> m_core_len_bits);
        assert(offset < m_core_size);
        assert(offset + length <= m_core_size);
        assert(length >= 2);
        if (!FastLabel) {
            length--;
        }
        const byte_t* zpath = m_core_data + offset + (FastLabel?0:1);
        size_t lim = std::min(slen, length);
        size_t pos = 0;
        while (pos < lim && str[pos] == zpath[pos]) ++pos;
        if (pos < length)
            return -intptr_t(pos);
        else
            return +intptr_t(pos);
    }
    else {
        assert(NULL != m_next_trie);
        size_t nest_id = size_t(linkVal - coreMaxLinkVal);
        if (FastLabel)
            return m_next_trie->matchZpath_loop(nest_id, 0, str, slen);
        else
            return m_next_trie->matchZpath_loop(nest_id, -1, str, slen);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_string_loop(size_t node_id, valvec<byte_t>* str) const {
    const size_t maxlen = m_max_strlen + 1;
    const size_t oldsize = str->size();
    str->ensure_capacity(oldsize + maxlen);
    fixed_vec<byte_t> buf(str->begin() + oldsize, maxlen);
	tpl_restore_string_loop(node_id, &buf);
    assert(buf.size() < maxlen);
    *buf.end() = '\0';
    str->risk_set_size(oldsize + buf.size());
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
restore_string_loop(size_t node_id, std::string* str) const {
    const size_t maxlen = m_max_strlen;
    const size_t oldsize = str->size();
    str->resize(oldsize + maxlen);
    fixed_vec<byte_t> buf((byte_t*)&*str->begin() + oldsize, maxlen);
	tpl_restore_string_loop(node_id, &buf);
    str->resize(oldsize + buf.size());
}
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrBuf>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
tpl_restore_string_loop(size_t node_id, StrBuf* str) const {
    tpl_restore_string_loop_ex(node_id, str, false);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrBuf>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
tpl_restore_string_loop_ex(size_t node_id, StrBuf* str, bool reverse) const {
//	assert(node_id > 0);
	assert(node_id < total_states());
	typedef RankSelect RS;
	typedef RankSelect2 RS2;
	size_t strOldsize = str->size();
	size_t parent = node_id;
	auto isLinkBits = m_is_link.bldata();
	auto isLinkRank = m_is_link.get_rank_cache();
	auto loudsBits = m_louds.bldata();
	auto loudsSel1 = m_louds.get_sel1_cache();
	auto loudsRank = m_louds.get_rank_cache();
	auto labelData = m_label_data;
	auto linkData = m_next_link.data();
	auto linkBits = m_next_link.uintbits();
	auto linkMask = m_next_link.uintmask();
	auto linkMinVal = m_next_link.min_val();
	auto coreMaxLinkVal = m_core_max_link_val;
	auto get_label = [labelData,this]
		(size_t child, size_t child_one_bitpos)
	-> size_t {
#if !defined(NDEBUG)
		size_t debug_parent = child_one_bitpos - child - 1;
		size_t debug_bitpos = m_louds.select0(debug_parent) + 1;
		size_t debug_lcount = m_louds.one_seq_len(debug_bitpos);
#endif
		size_t bitpos_beg = child_one_bitpos - m_louds.one_seq_revlen(child_one_bitpos);
		size_t bitpos_end = child_one_bitpos + m_louds.one_seq_len(child_one_bitpos);
		size_t lcount = bitpos_end - bitpos_beg;
		assert(lcount == debug_lcount);
		assert(bitpos_beg == debug_bitpos);
		if (lcount < 36) {
			return labelData[child];
		} else {
			size_t ith = child_one_bitpos - bitpos_beg;
			size_t first_sibling = child - ith;
			const byte_t* lbm = labelData + first_sibling;
			// labels[0..4) is bitmap rank index
			if (ith < lbm[2]) {
				if (ith < lbm[1])
					return 0*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+0*8), ith);
				else
					return 1*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+1*8), ith-lbm[1]);
			}
			else {
				if (ith < lbm[3])
					return 2*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+2*8), ith-lbm[2]);
				else
					return 3*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+3*8), ith-lbm[3]);
			}
		}
	};
	auto nextTrie = m_next_trie;
	auto coreData = m_core_data;
	auto coreLenMask = m_core_len_mask;
	auto coreLenBits = m_core_len_bits;
	size_t coreMinLen = m_core_min_len;
	while (parent != initial_state) {
		assert(parent < m_is_link.size());
        //RS2::fast_prefetch_rank1(isLinkRank, parent);
        _mm_prefetch((const char*)&labelData[parent], _MM_HINT_T0);
		if (RS2::fast_is1(isLinkBits, parent)) {
			size_t linkRank1 = RS2::fast_rank1(isLinkBits, isLinkRank, parent);
			size_t hig_bits = UintVector::fast_get(linkData, linkBits, linkMask, linkMinVal, linkRank1);
		//	size_t hig_bits = m_next_link[linkRank1];
			uint64_t linkVal;
			if (FastLabel) {
				linkVal = hig_bits;
			} else {
				size_t low_bits = labelData[parent];
				linkVal = uint64_t(hig_bits) << 8 | low_bits;
			}
			size_t oldsize = str->size();
			if (linkVal < coreMaxLinkVal) {
				assert(NULL != coreData);
				size_t length = size_t(linkVal &  coreLenMask) + coreMinLen;
				size_t offset = size_t(linkVal >> coreLenBits);
				assert(offset < m_core_size);
				assert(offset + length <= m_core_size);
				typedef typename StrBuf::value_type char_type;
				BOOST_STATIC_ASSERT(sizeof(char_type) == 1);
				str->append((const char_type*)coreData + offset, length);
			}
			else {
				size_t link_id = size_t(linkVal - coreMaxLinkVal);
				nextTrie->tpl_restore_string_loop(link_id, str);
			}
			if (reverse)
				std::reverse(str->begin() + oldsize, str->end());
		}
		else {
			if (!FastLabel)
				str->push_back(labelData[parent]);
		}
		size_t one_bitpos = RS::fast_select1(loudsBits, loudsSel1, loudsRank, parent);
		if (FastLabel) {
			byte_t ch = byte_t(get_label(parent, one_bitpos));
			str->push_back(ch);
		}
		parent = one_bitpos - parent - 1;
	//	parent = m_louds.rank0(m_louds.select1(parent)) - 1;
	}
	if (reverse)
		std::reverse(str->begin() + strOldsize, str->end());
}

template<class RankSelect, class RankSelect2, bool FastLabel>
intptr_t
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
matchZpath_loop(size_t node_id, intptr_t pos, const byte_t* str, intptr_t slen) const {
//	assert(node_id > 0);
	assert(node_id < total_states());
    assert(pos <= slen);
	typedef RankSelect RS;
	typedef RankSelect2 RS2;
	size_t parent = node_id;
	auto isLinkBits = m_is_link.bldata();
	auto isLinkRank = m_is_link.get_rank_cache();
	auto loudsBits = m_louds.bldata();
	auto loudsSel1 = m_louds.get_sel1_cache();
	auto loudsRank = m_louds.get_rank_cache();
	auto labelData = m_label_data;
	auto linkData = m_next_link.data();
	auto linkBits = m_next_link.uintbits();
	auto linkMask = m_next_link.uintmask();
	auto linkMinVal = m_next_link.min_val();
	auto coreMaxLinkVal = m_core_max_link_val;
	auto get_label = [labelData,this]
		(size_t child, size_t child_one_bitpos)
	-> size_t {
#if !defined(NDEBUG)
		size_t debug_parent = child_one_bitpos - child - 1;
		size_t debug_bitpos = m_louds.select0(debug_parent) + 1;
		size_t debug_lcount = m_louds.one_seq_len(debug_bitpos);
#endif
		size_t bitpos_beg = child_one_bitpos - m_louds.one_seq_revlen(child_one_bitpos);
		size_t bitpos_end = child_one_bitpos + m_louds.one_seq_len(child_one_bitpos);
		size_t lcount = bitpos_end - bitpos_beg;
		assert(lcount == debug_lcount);
		assert(bitpos_beg == debug_bitpos);
		if (lcount < 36) {
			return labelData[child];
		} else {
			size_t ith = child_one_bitpos - bitpos_beg;
			size_t first_sibling = child - ith;
			const byte_t* lbm = labelData + first_sibling;
			// labels[0..4) is bitmap rank index
			if (ith < lbm[2]) {
				if (ith < lbm[1])
					return 0*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+0*8), ith);
				else
					return 1*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+1*8), ith-lbm[1]);
			}
			else {
				if (ith < lbm[3])
					return 2*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+2*8), ith-lbm[2]);
				else
					return 3*64 + UintSelect1(unaligned_load<uint64_t>(lbm+4+3*8), ith-lbm[3]);
			}
		}
	};
	auto nextTrie = m_next_trie;
	auto coreData = m_core_data;
	auto coreLenMask = m_core_len_mask;
	auto coreLenBits = m_core_len_bits;
	size_t coreMinLen = m_core_min_len;
	while (parent != initial_state) {
		assert(parent < m_is_link.size());
        if (pos == slen) { // zpath is longer than str
            return -pos;
        }
        //RS2::fast_prefetch_rank1(isLinkRank, parent);
        _mm_prefetch((const char*)&labelData[parent], _MM_HINT_T0);
		if (RS2::fast_is1(isLinkBits, parent)) {
			size_t linkRank1 = RS2::fast_rank1(isLinkBits, isLinkRank, parent);
			size_t hig_bits = UintVector::fast_get(linkData, linkBits, linkMask, linkMinVal, linkRank1);
		//	size_t hig_bits = m_next_link[linkRank1];
			uint64_t linkVal;
			if (FastLabel) {
				linkVal = hig_bits;
			} else {
				size_t low_bits = labelData[parent];
				linkVal = uint64_t(hig_bits) << 8 | low_bits;
			}
			if (linkVal < coreMaxLinkVal) {
				assert(NULL != coreData);
				size_t length = size_t(linkVal &  coreLenMask) + coreMinLen;
				size_t offset = size_t(linkVal >> coreLenBits);
				assert(offset < m_core_size);
				assert(offset + length <= m_core_size);
                const byte_t* zpath = coreData + offset;
                const byte_t* pstr = str + pos;
                const size_t  lim = std::min<size_t>(slen-pos, length);
                size_t i = 0;
                while (i < lim && zpath[i] == pstr[i]) ++i;
                pos += i;
                if (i < length)
                    return -pos;
			}
			else {
				size_t link_id = size_t(linkVal - coreMaxLinkVal);
				intptr_t pos2 = nextTrie->matchZpath_loop(link_id, pos, str, slen);
                if (pos2 > 0)
                    pos = pos2;
                else
                    return pos2;
			}
		}
		else {
			if (!FastLabel) {
				if (str[pos] == labelData[parent])
                    pos++;
                else
                    return -pos;
            }
		}
		size_t one_bitpos = RS::fast_select1(loudsBits, loudsSel1, loudsRank, parent);
		if (FastLabel) {
			const byte_t ch = byte_t(get_label(parent, one_bitpos));
            if (str[pos] == ch)
                pos++;
            else
                return -pos;
		}
        assert(pos <= intptr_t(slen));
		parent = one_bitpos - parent - 1;
	//	parent = m_louds.rank0(m_louds.select1(parent)) - 1;
	}
    return pos;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
dot_file_write_node_loop(const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
						 FILE* fp, size_t trie_no) {
	for (size_t i = 0; i < trie->m_is_link.size(); ++i) {
		fprintf(fp, "\tnode%zd_%zd[label=\"%zd:%zd\"];\n",
			trie_no, i, trie_no, i);
		if (trie->m_is_link[i]) {
			if (trie->m_core_data) {
				assert(!"FastLabel must be false when calling this function");
				fstring corestr = trie->get_core_str(i);
				fprintf(fp, "\tcore%zd[label=\"%.*s\" shape=rectangle];\n",
					i, corestr.ilen(), corestr.data());
			}
			fprintf(fp, "\tlink%zd_%zd[shape=octagon];\n", trie_no, i);
		}
	}
	if (trie->m_next_trie)
		dot_file_write_node_loop(trie->m_next_trie, fp, trie_no+1);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
dot_file_write_edge_loop(const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
						 FILE* fp, size_t trie_no) {
	for(size_t i = 1; i < trie->m_is_link.size(); ++i) {
		size_t parent = trie->get_parent(i);
		if (trie->m_is_link[i]) {
			fprintf(fp, "\tnode%zd_%zd -> link%zd_%zd;\n",
				trie_no, i, trie_no, i);
			uint64_t linkVal = trie->get_link_val(i);
			if (linkVal < trie->m_core_max_link_val) {
				fprintf(fp, "\tlink%zd_%zd -> core%zd;\n", trie_no, i, i);
			}
			else {
				size_t link_id = size_t(linkVal - trie->m_core_max_link_val);
				fprintf(fp,
					"\tlink%zd_%zd -> node%zd_%zd [color=red];\n",
					trie_no, i, trie_no+1, link_id);
			}
			fprintf(fp, "\tlink%zd_%zd -> node%zd_%zd;\n",
				trie_no, i, trie_no, parent);
		}
		else {
			byte_t ch = trie->m_label_data[i];
			fprintf(fp, "\tnode%zd_%zd -> node%zd_%zd [label=\"%c(0x%02X)\"];\n",
				trie_no, i, trie_no, parent, ch, ch);
		}
	}
	if (trie->m_next_trie)
		dot_file_write_edge_loop(trie->m_next_trie, fp, trie_no+1);
}

// The trie graph is reversed: children point to parents
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
write_dot_file(FILE* fp) const {
	fprintf(fp, "digraph G {\n");
	dot_file_write_node_loop(this, fp, 0);
	dot_file_write_edge_loop(this, fp, 0);
	fprintf(fp, "}\n");
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
write_dot_file(fstring fname) const {
	Auto_fclose fp(fopen(fname.c_str(), "w"));
	if (NULL == fp) {
		THROW_STD(invalid_argument, "fopen(\"%s\", \"w\") = %s"
			, fname.c_str(), strerror(errno));
	}
	write_dot_file(fp);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
str_stat_loop(const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
              AutoGrownMemIO& buf, size_t nestLevel) {
	assert(trie->m_is_link.max_rank1() == trie->m_next_link.size());
	buf.printf("-------------------------------------------------\n");
	buf.printf("NestLevel  : %11zd\n", nestLevel);
	buf.printf("ZpathLength: %11lld\n", (llong)trie->total_zpath_len());
	buf.printf("NodeNumber : %11zd\n", trie->total_states());
	buf.printf("LinkNumber : %11zd\n", trie->m_next_link.size());
    if (FastLabel) {
	    buf.printf("LinkIdxBits: %11zd (FastLabel)\n", trie->m_next_link.uintbits());
	    buf.printf("LinkMemSize: %11zd (FastLabel)\n", trie->m_next_link.mem_size());
    }
    else {
	    buf.printf("LinkIdxBits: %11zd (- low 8 bits)\n", trie->m_next_link.uintbits());
	    buf.printf("LinkIdxBits: %11zd (+ low 8 bits)\n", trie->m_next_link.uintbits() + 8);
	    buf.printf("LinkMemSize: %11zd (- low 8 bits)\n", trie->m_next_link.mem_size());
	    buf.printf("LinkMemSize: %11zd (+ low 8 bits)\n", trie->m_next_link.mem_size() + trie->m_next_link.size());
    }
	buf.printf("LinkingRate: %11.7f\n", trie->m_next_link.size() / (double)trie->total_states());
	buf.printf("CoreLenBits: %11zd\n", (size_t)trie->m_core_len_bits);
	buf.printf("CoreMemSize: %11zd\n", (size_t)trie->m_core_size);
	buf.printf("CoreMinLen : %11zd\n", (size_t)trie->m_core_min_len);
	if (trie->m_next_trie)
		str_stat_loop(trie->m_next_trie, buf, nestLevel+1);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
str_stat(std::string* str) const {
	AutoGrownMemIO buf;
	str_stat_loop(this, buf, 0);
	str->assign((char*)buf.begin(), buf.tell());
}

#if 0
static void print_strVec(const SortableStrVec& strVec) {
	for (size_t i = 0; i < strVec.size(); ++i) {
		fstring s(strVec[i]);
		printf("%03zd: %.*s\n", i, s.ilen(), s.data());
	}
}
#endif

///@param[inout] strVec
///@param[out]   termFlag
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(SortableStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(SortThinStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(FixedLenStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(VoSortedStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(ZoSortedStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(DoSortedStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia(QoSortedStrVec& strVec,
               function<void(const valvec<index_t>&)> buildTerm,
               const NestLoudsTrieConfig& conf)
{
    build_patricia_tpl(strVec, buildTerm, conf);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrVecType>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_patricia_tpl(StrVecType& strVec,
                   function<void(const valvec<index_t>& linkVec)> buildTerm,
                   const NestLoudsTrieConfig& conf)
{
	assert(conf.nestLevel > 0);
	if (strVec.size() == 0) {
		THROW_STD(invalid_argument, "input strVec is empty");
	}
	SortableStrVec nestStrVec;
	valvec<byte_t> label;
	size_t inputStrVecSize = strVec.size();
	size_t inputStrVecBytes = strVec.str_size();
	{
		if (!conf.isInputSorted) {
			strVec.sort();
		}
		valvec<index_t> linkVec;
		build_self_trie_tpl(strVec, nestStrVec, linkVec, label, conf.nestLevel, conf);
		if (conf.debugLevel >= 2)
			fprintf(stderr
				, "done top trie: nodes=%zd; nest strVec: size=%zd str_size=%zd\n"
				, this->m_is_link.size(), nestStrVec.size(), nestStrVec.str_size()
			);
		buildTerm(linkVec);
	}
	if (nestStrVec.size() > 0) {
	#if defined(USE_SUFFIX_ARRAY_TRIE)
		if (conf.suffixTrie) {
			nextStrVec.compact();
			conf.suffixTrie.reset();
		}
	#endif
        if (nestStrVec.str_size() * conf.nestScale > inputStrVecBytes) {
            nestStrVec.reverse_keys();
            build_nested(nestStrVec, label, conf.nestLevel, conf);
        }
        else {
            this->build_core_no_reverse_keys(nestStrVec, label, conf);
        }
	}
	else {
		assert(m_is_link.max_rank1() == 0);
		m_label_data = label.risk_release_ownership();
		if (conf.debugLevel >= 1) {
			fprintf(stderr
				, "WARN: %s:%d: %s: strVec is empty: curNestLevel = %d  maxNestLevel = %d  inputStrVecSize = %zd\n"
				, __FILE__ , __LINE__
				, BOOST_CURRENT_FUNCTION, conf.nestLevel-1, conf.nestLevel, inputStrVecSize
			);
		}
	}
	this->m_louds.build_cache(1, 1);

	if (FastLabel) {
		size_t bitpos = 1; // parent's bitpos
		size_t child0 = 1;
		for(size_t parent = 0; parent < m_is_link.size(); ++parent) {
			assert(m_louds.is0(bitpos));
			size_t lcount = m_louds.one_seq_len(bitpos + 1);
			if (lcount >= 36) {
				// use bitmap rank1 to search child
				BOOST_STATIC_ASSERT(TERARK_WORD_BITS == 64);
				byte_t* mylabel = m_label_data + child0;
				size_t  bits[4] = {0};
				for(size_t i = 0; i < lcount; ++i) {
					terark_bit_set1(bits, mylabel[i]);
				}
				size_t  pc = 0;
				for(size_t i = 0; i < 4; ++i) {
					mylabel[i] = byte_t(pc);
					pc += fast_popcount64(bits[i]);
				}
				memcpy(mylabel + 04, bits, sizeof(bits));
				memset(mylabel + 36, 0, lcount - 36);
			}
			bitpos += lcount + 1;
			child0 += lcount;
		}
		assert(m_louds.size() == bitpos + 1);
	}
}

/// Same with patricia trie, but has no termFlags, return out linkVec
/// use restore_dawg_string to restore from a node_id
/// not used as DFA, just used for record store
/// it shows the result of build_strpool2 is smaller than build_strpool
///@param[inout] strVec
///@param[out]   linkVec linkVec[i] is ith record leaf node
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_strpool2(SortableStrVec& strVec, valvec<index_t>& linkVec,
               const NestLoudsTrieConfig& conf) {
	assert(conf.nestLevel > 0);
	if (strVec.size() == 0) {
		THROW_STD(invalid_argument, "input strVec is empty");
	}
	valvec<byte_t> label;
	if (!conf.isInputSorted) {
		strVec.sort();
	}
	size_t inputStrVecBytes = strVec.str_size();
	this->build_self_trie(strVec, linkVec, label, conf.nestLevel, conf);
	if (strVec.size() > 0) {
	#if defined(USE_SUFFIX_ARRAY_TRIE)
		if (conf.suffixTrie) {
			strVec.compact();
			conf.suffixTrie.reset();
		}
	#endif
        if (strVec.str_size() * conf.nestScale > inputStrVecBytes) {
            strVec.reverse_keys();
            build_nested(strVec, label, conf.nestLevel, conf);
        }
        else {
            this->build_core_no_reverse_keys(strVec, label, conf);
        }
	}
	else {
		assert(m_is_link.max_rank1() == 0);
		m_label_data = label.risk_release_ownership();
		fprintf(stderr
			, "WARN: %s: strVec is empty: curNestLevel = %d  maxNestLevel = %d\n"
			, BOOST_CURRENT_FUNCTION, conf.nestLevel-1, conf.nestLevel
		);
	}
	this->m_louds.build_cache(1, 1);
}

///@param[inout] strVec
///@param[out]  linkVec
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_strpool(SortableStrVec& strVec, valvec<index_t>& linkVec,
              const NestLoudsTrieConfig& conf)
{
	if (strVec.size() == 0) {
		THROW_STD(invalid_argument, "input strVec is empty");
	}
	strVec.reverse_keys();
	build_strpool_loop(strVec, linkVec, conf.nestLevel, conf);
}

///@param[inout] strVec
///@param[out]  linkVec
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_strpool_loop(SortableStrVec& strVec, valvec<index_t>& linkVec,
                   size_t curNestLevel, const NestLoudsTrieConfig& conf)
{
	assert(!FastLabel);
	if (strVec.size() == 0) {
		return; // when all strings of input strVec are empty
	}
	size_t inputStrVecBytes = strVec.str_size();
//	fprintf(stderr, "build_strpool_loop: nestLevel=%zd\n", curNestLevel);
	valvec<byte_t> label;
	strVec.sort();
	build_self_trie(strVec, linkVec, label, curNestLevel, conf);
#if 0 // this helps find linux gcc-4.9 memcpy bugs
	printf("build_strpool_loop: level=%zd, strVec.size=%zd\n", curNestLevel, strVec.size());
	strVec.sort();
	for (size_t i = 0; i < strVec.size(); ++i) {
		std::string s = strVec[i].str();
		std::reverse(s.begin(), s.end());
		printf("str[%zd]=%s\n", i, s.c_str());
	}
#endif
	if (strVec.size() > 0) {
		if (curNestLevel > 1 && strVec.str_size() * conf.nestScale > inputStrVecBytes) {
			build_nested(strVec, label, curNestLevel, conf);
		}
		else {
			build_core(strVec, label, conf);
		}
	}
	else {
		assert(m_is_link.max_rank1() == 0);
		m_label_data = label.risk_release_ownership();
		fprintf(stderr
			, "WARN: %s: strVec is empty: curNestLevel = %zd  maxNestLevel = %d\n"
			, BOOST_CURRENT_FUNCTION, curNestLevel, conf.nestLevel
		);
	}
	m_louds.build_cache(0, 1); // need not speed select0
}

///@param[inout] nextLinkVec
///@param[inout] label
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_link(valvec<index_t>& nextLinkVec, valvec<byte_t>& label) {
	if (!FastLabel) {
		for (size_t j = 0, k = 0; k < m_is_link.size(); ++k) {
			if (m_is_link[k]) {
				label[k] = byte_t(nextLinkVec[j]); // low 8 bits
				nextLinkVec[j] >>= 8; // shift off low 8 bits
				j++;
			}
		}
	}
	m_next_link.build_from(nextLinkVec);
	assert(m_next_link.size() == m_is_link.max_rank1());
	m_label_data = label.risk_release_ownership();
}

static void
compress_core(SortableStrVec& strVec, const NestLoudsTrieConfig& conf) {
    if (conf.corestrCompressLevel > 0) {
        profiling pf;
        long long t0 = pf.now();
        strVec.compress_strpool(conf.corestrCompressLevel);
        if (conf.debugLevel >= 2) {
            long long t1 = pf.now();
            long long len = strVec.str_size();
            size_t dup = 0;
            for (size_t i = 1, n = strVec.size(); i < n; ++i) {
                if (strVec.nth_offset(i-1) == strVec.nth_offset(i))
                    dup++;
            }
            fprintf(stderr,
                "compress_core: compressed={dup: %zd, cnt: %zd, len: %lld, dupRatio: %f} time=%f\n",
                dup, strVec.size(), len, 1.0*dup/strVec.size(), pf.sf(t0, t1));
        }
    }
    strVec.sort_by_seq_id();
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_mixed(SortableStrVec& strVec, valvec<byte_t>& label,
            size_t curNestLevel, const NestLoudsTrieConfig& conf) {
    valvec<index_t> nextLinkVec;
    valvec<index_t> coreLinkVec;
    febitvec isShort(strVec.size(), false);
    SortableStrVec coreStrVec;
    size_t coreStrLen = 0;
    size_t coreStrNum = 0;
    for(size_t i = 0, n = strVec.size(); i < n; ++i) {
        size_t l = strVec.nth_size(i);
        if (l <= MaxShortStrLen) {
            isShort.set1(strVec.m_index[i].seq_id);
            coreStrLen += l;
            coreStrNum += 1;
        }
    }
    if (coreStrNum) {
        coreStrVec.reserve(coreStrNum, coreStrLen);
        strVec.erase_if2([&](size_t i, fstring str) {
            if (str.size() <= MaxShortStrLen) {
                coreStrVec.push_back(str);
                coreStrVec.m_index.back().seq_id = strVec.m_index[i].seq_id;
                return true;
            }
            return false;
        });
        coreStrVec.reverse_keys();
        strVec.shrink_to_fit();
        strVec.sort_by_seq_id(); // before here, it was sorted by offset
        strVec.make_ascending_seq_id();
        compress_core(coreStrVec, conf);
        coreStrVec.make_ascending_seq_id();
        coreLinkVec.resize_no_init(coreStrVec.size());
        const size_t minLen = MaxShortStrLen - 1;
        const size_t lenBits = 1;
        for(size_t i = 0; i < coreStrVec.size(); ++i) {
            size_t offset = coreStrVec.m_index[i].offset;
            size_t keylen = coreStrVec.m_index[i].length;
            size_t val = (offset << lenBits) | (keylen - minLen);
            coreLinkVec[i] = index_t(val);
        }
        assert(label.size() == m_is_link.size());
        coreStrVec.m_index.clear(); // free memory earlier
        label.reserve(label.size() + coreStrVec.str_size()); // alloc exact
        label.append(coreStrVec.m_strpool);
        m_core_size = coreStrVec.str_size();
        m_core_data = label.data() + m_is_link.size();
        m_core_len_bits = byte_t(lenBits);
        m_core_len_mask = (size_t(1) << lenBits) - 1;
        m_core_min_len = byte_t(minLen);
        m_core_max_link_val = coreStrVec.str_size() << lenBits;
        coreStrVec.clear();
    }
    if (strVec.size()) {
        m_next_trie = new NestLoudsTrieTpl<RankSelect>();
        m_next_trie->build_strpool_loop(strVec, nextLinkVec, curNestLevel-1, conf);
    }
    size_t coreMaxLinkVal = m_core_max_link_val;
    size_t j = coreLinkVec.size();
    size_t k = isShort.size();
    // it is likely that coreLinkVec.size() > nextLinkVec.size()
    coreLinkVec.resize_no_init(isShort.size());
    while (k-- > 0) {
        if (isShort[k])
            coreLinkVec[k] = coreLinkVec[--j];
        else
            coreLinkVec[k] = nextLinkVec[k-j] + coreMaxLinkVal;
    }
    nextLinkVec.clear();
    isShort.clear();
    build_link(coreLinkVec, label);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_nested(SortableStrVec& strVec, valvec<byte_t>& label,
             size_t curNestLevel, const NestLoudsTrieConfig& conf) {
    if (conf.useMixedCoreLink) {
        this->build_mixed(strVec, label, curNestLevel, conf);
    }
    else {
        valvec<index_t> nextLinkVec;
        m_next_trie = new NestLoudsTrieTpl<RankSelect>();
        m_next_trie->build_strpool_loop(strVec, nextLinkVec, curNestLevel-1, conf);
        this->build_link(nextLinkVec, label);
    }
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
debug_equal_check(const NestLoudsTrieTpl& y) const {
    auto& x = *this;
    assert(x.m_louds.size() == y.m_louds.size());
    assert(memcmp(x.m_louds.data(), y.m_louds.data(), x.m_louds.mem_size()) == 0);

    assert(x.m_is_link.size() == y.m_is_link.size());
    assert(memcmp(x.m_is_link.data(), y.m_is_link.data(), y.m_is_link.mem_size()) == 0);

    assert(x.m_next_link.size() == y.m_next_link.size());
    assert(memcmp(x.m_next_link.data(), y.m_next_link.data(), x.m_next_link.mem_size()) == 0);

    assert(memcmp(x.m_label_data, y.m_label_data, x.m_is_link.size()) == 0);

    assert(x.m_core_size == y.m_core_size);
    assert(x.m_core_len_mask == y.m_core_len_mask);
    assert(x.m_core_len_bits == y.m_core_len_bits);
    assert(x.m_core_min_len == y.m_core_min_len);
    assert(x.m_core_max_link_val == y.m_core_max_link_val);
    assert(memcmp(x.m_core_data, y.m_core_data, x.m_core_size) == 0);

    if (x.m_next_trie && y.m_next_trie) {
        x.m_next_trie->debug_equal_check(*y.m_next_trie);
    } else {
        assert(NULL == x.m_next_trie);
        assert(NULL == y.m_next_trie);
    }
}

namespace nlt_detail {

    template<class UintType>
    struct RangeTpl {
        UintType begRow;
        UintType endRow;
        UintType begCol;
        RangeTpl(size_t beg, size_t end, size_t pos) {
            begRow = UintType(beg);
            endRow = UintType(end);
            begCol = UintType(pos);
        }
        RangeTpl() {}
        bool empty() const { return begRow == endRow; }
    };
#pragma pack(push, 1)
    template<>
    struct RangeTpl<uint64_t> {
        uint64_t begRow : 48;
        uint64_t endRow : 48;
        uint64_t begCol : 32;
        RangeTpl(size_t beg, size_t end, size_t pos) {
            begRow = beg;
            endRow = end;
            begCol = pos;
        }
        RangeTpl() {}
        bool empty() const { return begRow == endRow; }
    };
#pragma pack(pop)
#if defined(_MSC_VER)
    // visual c++ 2015 sucks
#else
    BOOST_STATIC_ASSERT(sizeof(RangeTpl<uint64_t>) == 16);
#endif

	struct RangeFast {
		size_t begRow;
		size_t endRow;
		size_t begCol;
		RangeFast(size_t beg, size_t end, size_t pos) {
			begRow = beg;
			endRow = end;
			begCol = pos;
		}
		RangeFast(const RangeTpl<uint64_t>& y) {
			begRow = y.begRow;
			endRow = y.endRow;
			begCol = y.begCol;
		}
		operator RangeTpl<uint64_t>() const {
			return RangeTpl<uint64_t>(begRow, endRow, begCol);
		}
	};
	template<class T>
	struct NoneBitField { typedef T type; };
	template<>
	struct NoneBitField<RangeTpl<uint64_t> > { typedef RangeFast type; };

	void printDup(const char* sig, size_t bfsDepth, size_t dupCnt, fstring s0, size_t beg, size_t end) {
		std::string s = s0.str();
		std::reverse(s.begin(), s.end());
		printf("%s: bfsDepth=%zd, cnt=%zd, len=%zd: %.*s\33[1;31m%.*s\33[0m%.*s\n"
			, sig, bfsDepth, dupCnt, end - beg
			, int(s.size() - end), s.data()
			, int(end - beg), s.data() + beg
			, int(beg), s.data() + end
			);
	}

	template<class T>
	size_t max_n(const T* a, size_t n) {
		assert(n > 0);
		T x = a[0];
		size_t pos = 0;
		for (size_t i = 1; i < n; ++i) {
			if (x < a[i])
				x = a[i], pos = i;
		}
		return pos;
	}

	template<class StrVecType>
	void tryPrintNestStrOutput(const StrVecType& strVec, size_t nthNest) {
		const char* env = getenv("NestLoudsTrie_nestStrOutputFile");
		if (NULL == env)
			return;
		valvec<char> fpath(strlen(env) + 32);
		snprintf(fpath.data(), fpath.size(), "%s-%zd.txt", env, nthNest);
		Auto_fclose fp(fopen(fpath.data(), "w"));
		if (fp) {
			for (size_t i = 0; i < strVec.size(); ++i) {
				fstring s = strVec[i];
				fprintf(fp, "%.*s\n", s.ilen(), s.data());
			}
		}
		else {
			fprintf(stderr, "ERROR(ignored): fopen(%s, w) = %s\n"
				, fpath.data(), strerror(errno));
		}
	}
}

using namespace nlt_detail;

template<class T>
class OnePassQueue : boost::noncopyable {
public:
	class InFile;
	class InMem;
	typedef typename NoneBitField<T>::type FastT;
	static std::unique_ptr<OnePassQueue> create(fstring tmpDir, fstring prefix);
	virtual ~OnePassQueue() {}
	virtual void push_back(const FastT& x) = 0;
	virtual FastT pop_front_val() = 0;
	virtual void complete_write() = 0;
	virtual void swap_out(valvec<T>* vec) = 0;
	virtual void read_all(valvec<T>* vec) = 0;
	virtual void rewind_for_write() = 0;
	virtual bool empty() const = 0;
	virtual size_t size() const = 0;
};

class TempFile {
public:
	std::string  tmpFpath;
	FileStream   tmpFile;
	NativeDataOutput<OutputBuffer> oTmpBuf;
	NativeDataInput <InputBuffer > iTmpBuf;

	TempFile(fstring tmpDir, fstring prefix) {
//#if defined(_WIN32) || defined(_WIN64)
#if _MSC_VER
		tmpFpath = tmpDir + "\\" + prefix + "XXXXXX";
		int err = _mktemp_s(&tmpFpath[0], tmpFpath.size()+1);
		if (err) {
			fprintf(stderr
				, "ERROR: _mktemp_s(%s) failed with: %s, so we may use large memory\n"
				, tmpFpath.c_str(), strerror(err));
		}
		tmpFile.open(tmpFpath.c_str(), "wb+");
#else
		tmpFpath = tmpDir + "/" + prefix + "XXXXXX";
		int fd = mkstemp(&tmpFpath[0]);
		if (fd > 0) {
			tmpFile.dopen(fd, "wb+");
		}
		else {
			THROW_STD(runtime_error, "ERROR: mkstemp(%s) = %s\n"
				, tmpFpath.c_str(), strerror(errno));
		}
#endif
		tmpFile.disbuf();
		oTmpBuf.attach(&tmpFile);
		iTmpBuf.attach(&tmpFile);
	}
	~TempFile() {
    if (oTmpBuf.bufpos() > 0) {
      fprintf(stderr
        , "ERROR: %s, file(%s) %zd bytes in write buffer, it should have been flushed\n"
        , BOOST_CURRENT_FUNCTION, tmpFpath.c_str(), oTmpBuf.bufpos());
      fflush(stderr);
      // if do not reset, it should coredump
      TERARK_IF_DEBUG(abort(), oTmpBuf.resetbuf());
    }
    if (iTmpBuf.buf_remain_bytes() > 0) {
      fprintf(stderr
        , "ERROR: %s, file(%s) %zd bytes in read buffer, it should have been read in\n"
        , BOOST_CURRENT_FUNCTION, tmpFpath.c_str(), oTmpBuf.buf_remain_bytes());
      fflush(stderr);
      // if do not reset, it should coredump
      TERARK_IF_DEBUG(abort(), iTmpBuf.resetbuf());
    }
    tmpFile.close();
		if (::remove(tmpFpath.c_str()) < 0) {
			fprintf(stderr, "ERROR: remove(%s) = %s\n", tmpFpath.c_str(), strerror(errno));
		}
	}
	void complete_write() {
		oTmpBuf.flush_buffer();
		tmpFile.rewind();
		iTmpBuf.resetbuf();
	}
	// ssv.m_strpool and ssv.m_index has already allocated
	void read_strvec(SortableStrVec& ssv) {
		size_t  count = ssv.m_index.size();
		size_t  offset = 0;
		size_t  strSize = ssv.m_strpool.size();
		byte_t* strBase = ssv.m_strpool.data();
		SortableStrVec::SEntry* pSEntry = ssv.m_index.data();
		for(size_t i = 0; i < count; ++i) {
			size_t l = iTmpBuf.read_var_uint32();
			assert(offset + l <= strSize);
			if (terark_unlikely(offset + l > strSize)) {
				THROW_STD(logic_error
					, "Unexpected: offset = %zd, len = %zd, total = %zd"
					, offset, l, strSize);
			}
			pSEntry[i].offset = offset;
			pSEntry[i].length = l;
			pSEntry[i].seq_id = i;
			iTmpBuf.ensureRead(strBase + offset, l);
			offset += l;
		}
		assert(offset == strSize);
		if (offset != strSize) {
			THROW_STD(logic_error
				, "Unexpected: offset = %zd, total = %zd", offset, strSize);
		}
	}
};

template<class T>
class OnePassQueue<T>::InFile : public OnePassQueue<T>, private TempFile {
	size_t    m_cur;
	size_t    m_size;
public:
	typedef typename NoneBitField<T>::type FastT;
	InFile(fstring tmpDir, fstring prefix) : TempFile(tmpDir, prefix) {
		m_cur = size_t(-1);
		m_size = 0;
	}
	virtual ~InFile() {}
	virtual void push_back(const FastT& x) override {
		if (std::is_same<T, FastT>::value) {
			oTmpBuf.ensureWrite(&x, sizeof(T));
		} else {
			T t(x);
			oTmpBuf.ensureWrite(&t, sizeof(T));
		}
		m_size++;
	}
	virtual FastT pop_front_val() override {
		assert(m_cur < m_size);
		T x;
		iTmpBuf.ensureRead(&x, sizeof(T));
		m_cur++;
		return x;
	}
	virtual void complete_write() override {
		TempFile::complete_write();
		m_cur = 0;
	}
	virtual void swap_out(valvec<T>* vec) override {
		tmpFile.rewind();
		oTmpBuf.ensureWrite(vec->data(), vec->used_mem_size());
		oTmpBuf.flush_buffer();
		m_size = vec->size();
		m_cur = 0;
		vec->clear();
	}
	virtual void read_all(valvec<T>* vec) override {
		assert(0 == m_cur);
		vec->resize_no_init(m_size);
		tmpFile.rewind();
		iTmpBuf.ensureRead(vec->data(), vec->used_mem_size());
		m_cur = size_t(-1);
	}
	virtual void rewind_for_write() override {
		tmpFile.rewind();
        iTmpBuf.resetbuf();
		oTmpBuf.resetbuf();
		m_cur = size_t(-1);
		m_size = 0;
	}
	virtual bool empty() const override {
		assert(m_cur <= m_size);
		return m_cur >= m_size;
	}
	virtual size_t size() const override { return m_size; }
};

template<class T>
class OnePassQueue<T>::InMem : public OnePassQueue<T> {
	valvec<T> m_vec;
	size_t    m_cur;
public:
	typedef typename NoneBitField<T>::type FastT;
	InMem() : m_cur(size_t(-1)) {}
	virtual ~InMem() {}
	virtual void push_back(const FastT& x) override {
		m_vec.push_back(x);
	}
	virtual FastT pop_front_val() override {
		assert(m_cur < m_vec.size());
		FastT x = m_vec[m_cur];
		m_cur++;
		return x;
	}
	virtual void complete_write() override {
		m_cur = 0;
	}
	virtual void swap_out(valvec<T>* vec) override {
		assert(false);
		THROW_STD(invalid_argument, "not implemented");
	}
	virtual void read_all(valvec<T>* vec) override {
		assert(0 == m_cur);
		m_vec.swap(*vec);
		m_vec.clear();
		m_cur = size_t(-1);
	}
	virtual void rewind_for_write() override {
		m_vec.erase_all();
		m_cur = size_t(-1);
	}
	virtual bool empty() const override {
		assert(m_cur <= m_vec.size());
		return m_cur >= m_vec.size();
	}
	virtual size_t size() const override { return m_vec.size(); }
};

template<class T>
std::unique_ptr<OnePassQueue<T> >
OnePassQueue<T>::create(fstring tmpDir, fstring prefix) {
	if (tmpDir.empty()) {
		return std::unique_ptr<OnePassQueue>(new OnePassQueue::InMem());
	}
	return std::unique_ptr<OnePassQueue>(new OnePassQueue::InFile(tmpDir, prefix));
}

template<class UintType>
struct LinkSeqTpl {
    UintType link_id;
    UintType seq_id;
    LinkSeqTpl(size_t link, size_t seq)
        : link_id(UintType(link)), seq_id(UintType(seq)) {}
    LinkSeqTpl() {}
    DATA_IO_DUMP_RAW_MEM(LinkSeqTpl)
};
template<>
struct LinkSeqTpl<uint64_t> {
    uint64_t link_id;
    uint64_t seq_id;
    LinkSeqTpl(size_t link, size_t seq)
        : link_id(link), seq_id(seq) {}
    LinkSeqTpl() {}
    DATA_IO_DUMP_RAW_MEM(LinkSeqTpl)
};

template<class T>
class CompressedRangeQueueBase : public OnePassQueue<T> {
	size_t    m_cur;
	size_t    m_size;
	size_t    m_prevEndRow;
protected:
	typedef typename NoneBitField<T>::type FastT;
	CompressedRangeQueueBase() { init_for_write(); }
	virtual ~CompressedRangeQueueBase(){} // to avoid eclipse warning
	void init_for_write() {
		m_cur = size_t(-1);
		m_size = 0;
		m_prevEndRow = 0;
	}
	virtual void complete_write() override {
		m_cur = 0;
		m_prevEndRow = 0;
	}
	virtual void swap_out(valvec<T>* vec) override {
		THROW_STD(invalid_argument, "this function should not be called");
	}
	virtual void read_all(valvec<T>* vec) override {
		THROW_STD(invalid_argument, "this function should not be called");
	}
	virtual bool empty() const override {
		assert(m_cur <= m_size);
		return m_cur >= m_size;
	}
	virtual size_t size() const override { return m_size; }

	template<class DataIO>
	void push_impl(DataIO& dio, const FastT& x) {
		size_t begRow = x.begRow;
		size_t endRow = x.endRow;
		size_t begCol = x.begCol;
		size_t prevEndRow = m_prevEndRow;
		assert(begRow <= endRow); // can be empty
		assert(prevEndRow <= begRow);
		size_t rlen = endRow - begRow;
		size_t diff = begRow - prevEndRow;
		if (rlen <= 15 && diff <= 7) {
			// write both rlen and diff, lowest bit is always 1
			dio.writeByte(byte_t((rlen << 4) | (diff << 1) | 1));
		}
		else {
			dio << var_size_t(rlen << 1); // lowest bit is always 0
			dio << var_size_t(diff);
		}
		if (begRow < endRow) {
			dio << var_size_t(begCol);
		}
		m_prevEndRow = endRow;
		m_size++;
	}

	template<class DataIO>
	FastT pop_impl(DataIO& dio) {
		assert(m_cur < m_size);
		size_t prevEndRow = m_prevEndRow;
		size_t b = dio.readByte();
		size_t rlen, diff;
		if (b & 1) { // high compressed
			rlen = (b >> 4);
			diff = (b >> 1) & 7;
		} else {
			if (b & 0x80) {
				size_t  rlenHi = dio.template load_as<var_size_t>();
				rlen = (rlenHi << 7) | (b & byte_t(~0x81));
			} else {
				rlen = (b & byte_t(~0x81));
			}
			rlen >>= 1;
			diff = dio.template load_as<var_size_t>();
		}
		size_t currBegRow = prevEndRow + diff;
		size_t currEndRow = currBegRow + rlen;
		size_t currBegCol;
		if (currBegRow < currEndRow) {
			currBegCol = dio.template load_as<var_size_t>();
		} else {
			currBegCol = 0; // will not be used
		}
		m_cur++;
		m_prevEndRow = currEndRow;
		return FastT(currBegRow, currEndRow, currBegCol);
	}
};

/// T is a RangeTpl<UintType>
template<class T>
class CompressedRangeQueueInFile : public CompressedRangeQueueBase<T> {
	TempFile tf;
public:
	typedef typename NoneBitField<T>::type FastT;
	CompressedRangeQueueInFile(fstring tmpDir, fstring prefix)
		: tf(tmpDir, prefix) {}
	virtual ~CompressedRangeQueueInFile(){}
	virtual void push_back(const FastT& x) override {
		this->push_impl(tf.oTmpBuf, x);
	}
	virtual FastT pop_front_val() override {
		return this->pop_impl(tf.iTmpBuf);
	}
	virtual void complete_write() override {
		tf.complete_write();
		CompressedRangeQueueBase<T>::complete_write();
	}
	virtual void rewind_for_write() override {
		tf.tmpFile.rewind();
		tf.iTmpBuf.resetbuf();
		tf.oTmpBuf.resetbuf();
		this->init_for_write();
	}
};

/// T is a RangeTpl<UintType>
template<class T>
class CompressedRangeQueueInMem : public CompressedRangeQueueBase<T> {
	AutoGrownMemIO m_mem;
public:
	typedef typename NoneBitField<T>::type FastT;
	CompressedRangeQueueInMem() {
		m_mem.init(4096);
	}
	virtual ~CompressedRangeQueueInMem(){}
	virtual void push_back(const FastT& x) override {
		auto& dio = static_cast<NativeDataOutput<AutoGrownMemIO>&>(m_mem);
		this->push_impl(dio, x);
	}
	virtual FastT pop_front_val() override {
		auto& dio = static_cast<NativeDataInput<AutoGrownMemIO>&>(m_mem);
		return this->pop_impl(dio);
	}
	virtual void complete_write() override {
		m_mem.rewind();
		CompressedRangeQueueBase<T>::complete_write();
	}
	virtual void rewind_for_write() override {
		m_mem.rewind();
		this->init_for_write();
	}
};

template<class UintType>
static
std::unique_ptr<OnePassQueue<RangeTpl<UintType> > >
createRangeQueue(const NestLoudsTrieConfig& conf, fstring prefix) {
	typedef RangeTpl<UintType> Range;
	if (conf.enableQueueCompression) {
		if (conf.tmpDir.size()) {
			std::string zprefix = prefix + "z-";
			return std::unique_ptr<OnePassQueue<Range> >(
				new CompressedRangeQueueInFile<Range>(conf.tmpDir, zprefix));
		} else {
			return std::unique_ptr<OnePassQueue<Range> >(
				new CompressedRangeQueueInMem<Range>());
		}
	} else {
		return OnePassQueue<Range>::create(conf.tmpDir, prefix);
	}
}

///@param[inout] strVec
///@param[out]   linkVec
///@param[out]   label
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_self_trie(SortableStrVec& strVec,
                valvec<index_t>& linkVec,
                valvec<byte_t>& label,
                size_t curNestLevel,
                const NestLoudsTrieConfig& conf)
{
    SortableStrVec nestStrVec;
    build_self_trie_tpl(strVec, nestStrVec, linkVec, label, curNestLevel, conf);
    strVec.swap(nestStrVec);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
template<class StrVecType>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_self_trie_tpl(StrVecType& strVec, SortableStrVec& nestStrVec,
                    valvec<index_t>& linkVec,
                    valvec<byte_t>& label,
                    size_t curNestLevel,
                    const NestLoudsTrieConfig& conf)
{
	TERARK_VERIFY(strVec.size() > 0);
	TERARK_VERIFY_EZ(label.size(), "%zd"); // NOLINT
	if (sizeof(index_t) == 4 && strVec.str_size() > 0x1E0000000) { // 7.5G
		THROW_STD(length_error
			, "strVec is too large: size = %zd str_size = %zd"
			, strVec.size(), strVec.str_size()
		);
	}
	if (m_label_data) {
		THROW_STD(invalid_argument, "trie must be empty");
	}
    m_max_strlen = strVec.max_strlen();
	if (curNestLevel < size_t(conf.nestLevel)) {
		tryPrintNestStrOutput(strVec, conf.nestLevel - curNestLevel);
	}
	if (conf.debugLevel >= 2) {
		// must have been sorted
		for (size_t i = 1; i < strVec.size(); ++i) {
			assert(strVec[i-1] <= strVec[i]);
		}
	}
	size_t minLinkStrLen = getRealMinLinkStrLen(conf);
#if defined(NestLoudsTrie_EnableDelim)
	double realMaxFragLen = conf.maxFragLen; // ? conf.maxFragLen : double(strVec.str_size())/strVec.size();
	double q = pow(realMaxFragLen/conf.minFragLen, 1.0/(conf.nestLevel + 1));
	double fmaxFragLen1 = conf.minFragLen * pow(q, curNestLevel + 1);
	double fmaxFragLen2 = fmaxFragLen1/q;
	double fmaxFragLen3 = fmaxFragLen2/q;
	size_t maxFragLen0 = std::min<size_t>(conf.maxFragLen, 253);
	size_t maxFragLen1 = ceil(fmaxFragLen1);
	size_t maxFragLen2 = ceil(fmaxFragLen2);
	size_t maxFragLen3 = ceil(fmaxFragLen3);
	size_t minFragLen1 = std::max<long>(4, conf.minFragLen);
	if (conf.debugLevel >= 1) {
		fprintf(stderr
		, "build_self_trie: prefix=%s q=%f cur=%zd frag=(%f %f) cnt=%zd len=%zd avglen=%f\n"
        , conf.commonPrefix.c_str()
		, q, curNestLevel, fmaxFragLen1, fmaxFragLen2
		, strVec.size(), strVec.str_size(), strVec.avg_size()
		);
	}
	TERARK_VERIFY_LE(minFragLen1, maxFragLen1, "%zd %zd");
	maxFragLen1 = std::min<size_t>(maxFragLen1, 253);
	maxFragLen2 = std::min<size_t>(maxFragLen2, 253);
	maxFragLen3 = std::min<size_t>(maxFragLen3, 253);
#endif
	std::unique_ptr<TempFile> linkSeqStore;
	const size_t strVecSize = strVec.size();
	const int realTmpLevel = getRealTmpLevel(conf, strVecSize, strVec.str_size());
	if (!conf.tmpDir.empty() && realTmpLevel >= 3) {
		linkSeqStore.reset(new TempFile(conf.tmpDir, "linkSeqVec-"));
	}
	else {
		linkVec.resize_fill(strVecSize, index_t(-1));
	}
    typedef LinkSeqTpl<index_t> LinkSeq;
	auto q1 = createRangeQueue<index_t>(conf, "q1-");
	auto q2 = createRangeQueue<index_t>(conf, "q2-");
	auto labelStore = OnePassQueue<byte_t>::create(conf.tmpDir, "label-");
	std::unique_ptr<TempFile> nestStrPoolFile;
	std::unique_ptr<OnePassQueue<SortableStrVec::OffsetLength> > nextStrVecStore;
	size_t nestStrPoolSize = 0;
	size_t nestStrVecSize = 0;
	if (conf.tmpDir.size() && realTmpLevel >= 4) {
		nestStrPoolFile.reset(new TempFile(conf.tmpDir, "nestStrPool-"));
	} else {
		nextStrVecStore = OnePassQueue<SortableStrVec::OffsetLength>::create(conf.tmpDir, "nestStrVec-");
	}
	size_t depth = 0;
	{
		// allowing empty strings
		size_t firstNonEmpty = upper_bound_0<StrVecType&>(strVec, strVecSize, "");
		for(size_t i = 0; i < firstNonEmpty; ++i) {
			size_t seq_id = strVec.nth_seq_id(i);
			if (linkSeqStore)
				linkSeqStore->oTmpBuf << LinkSeq(0, seq_id);
			else
				linkVec[seq_id] = 0;
		}
		if (conf.debugLevel >= 1) {
			fprintf(stderr, "build_self_trie: firstNonEmpty = %zd\n", firstNonEmpty);
		}
		q1->push_back({firstNonEmpty, strVecSize, 0});
		q1->complete_write();
	}
#if defined(USE_SUFFIX_ARRAY_TRIE)
	if (conf.saFragMinFreq &&
(!conf.suffixTrie || strVec.m_real_str_size < strVec.str_size() * 0.7)) {
		size_t minTokenLen = 8;
		size_t minCacheDist = 32;
		size_t bfsMaxDepth = 8;
		conf.suffixTrie.reset(new SuffixTrieCacheDFA());
		conf.suffixTrie->build_sa(strVec);
		conf.suffixTrie->bfs_build_cache(minTokenLen, minCacheDist, bfsMaxDepth);
		if (conf.debugLevel >= 1)
			conf.suffixTrie->sa_print_stat();
	}
	else if (conf.bzMinLen) {
		size_t minTokenLen = 8;
	//	size_t minCacheDist = 32;
		strVec.make_ascending_offset();
		conf.bestZipLenArr = (uint16_t*)::realloc(conf.bestZipLenArr, strVec.str_size()*sizeof(uint16_t));
		ComputeBestZipLen(strVec, minTokenLen, conf.bestZipLenArr);
	}
#endif
	m_is_link.push_back(false); // reserve unused
	m_louds.push_back(true);
	m_louds.push_back(false);
	labelStore->push_back(0); // reserve unused

    auto appendPrefix = [&](fstring pref) {
        m_louds.push_back(true);
        m_louds.push_back(false);
        if (pref.size() > 1) {
            nestStrVecSize++;
            nestStrPoolSize += pref.size()-1;
            if (nestStrPoolFile) {
                nestStrPoolFile->oTmpBuf << pref.substr(1);
            } else { // reserve for change/patch later
                nextStrVecStore->push_back({0,0});
            }
            if (FastLabel) {
                labelStore->push_back(pref[0]);
            } else {
                labelStore->push_back(0); // reserved for latter use
            }
            m_is_link.push_back(true);
        } else {
            labelStore->push_back(pref[0]);
            m_is_link.push_back(false);
        }
    };
    const size_t prefixLen = conf.commonPrefix.size(); // whole prefix len
    const size_t prefixNum = conf.nestLevel == int(curNestLevel)
                           ? prefixLen/253 + (prefixLen%253 > 1)
                           : 0;
    if (prefixNum) {
        m_max_strlen += prefixLen - prefixNum;
        // reserve a node for common prefix
        fstring pref = conf.commonPrefix;
        while (pref.size() >= 253) {
            appendPrefix(pref.substr(0, 253));
            pref = pref.substr(253);
        }
        if (!pref.empty())
            appendPrefix(pref);
    }
    auto patchNestStrVec = [&]() {
        TERARK_VERIFY(nullptr == nestStrPoolFile); // NOLINT
        if (0 == prefixNum)
            return;
        TERARK_VERIFY_EQ(nestStrVec.size(), nestStrVecSize, "%zd %zd");
        for (size_t i = 0; i < prefixNum; ++i) {
            TERARK_VERIFY_LT(size_t(nestStrVec.m_index[i].seq_id), prefixNum, "%zd %zd");
            TERARK_VERIFY_EZ(size_t(nestStrVec.m_index[i].offset), "%zd");
            TERARK_VERIFY_EZ(size_t(nestStrVec.m_index[i].length), "%zd");
        }
        sort_0(nestStrVec.m_index.begin(), prefixNum, TERARK_CMP(seq_id, <));
        size_t strIncSize = prefixLen - prefixNum;
        nestStrVec.m_strpool.ensure_unused(strIncSize);
        byte_t* data = nestStrVec.m_strpool.data();
        memmove(data + strIncSize, data, nestStrVec.m_strpool.size());
        nestStrVec.m_strpool.grow_no_init(strIncSize);
        fstring pref = conf.commonPrefix;
        size_t offset = 0;
        for (size_t i = 0; i + 1 < prefixNum; ++i) {
            TERARK_VERIFY_EQ(size_t(nestStrVec.m_index[i].seq_id), i, "%zd %zd");
            auto& x = nestStrVec.m_index[i];
            x.offset = offset;
            x.length = 252;
            memcpy(data + offset, pref.p+1, 252);
            pref = pref.substr(253);
            offset += 252;
        }
        TERARK_VERIFY_GT(pref.size(),   1, "%zd %d");
        TERARK_VERIFY_LT(pref.size(), 253, "%zd %d");
        auto& x = nestStrVec.m_index[prefixNum-1];
        x.offset = offset;
        x.length = uint32_t(pref.size() - 1);
        memcpy(data + offset, pref.p+1, pref.n-1);
        TERARK_VERIFY_EQ(offset + pref.size() - 1, strIncSize, "%zd %zd");
        for (size_t i = prefixNum; i < nestStrVecSize; i++) {
            nestStrVec.m_index[i].offset += strIncSize;
        }
    };
	const byte_t* strBase = strVec.m_strpool.data();
	while (!q1->empty()) {
		while (!q1->empty()) {
			auto parent = q1->pop_front_val();
			size_t parentBegRow = parent.begRow;
			size_t parentEndRow = parent.endRow;
			size_t parentBegCol = parent.begCol;
			size_t childBegRow = parentBegRow;
			while (childBegRow < parentEndRow) {
				fstring childBegStr = strVec[childBegRow];
				assert(parentBegCol < childBegStr.size());
				size_t childEndRow = strVec.upper_bound_at_pos(childBegRow, parentEndRow, parentBegCol, childBegStr[parentBegCol]);
				size_t childBegCol;
			//	size_t childEndCol = std::min(childBegStr.size(), parentBegCol + MAX_ZPATH_LEN);
				size_t childEndCol = std::min(childBegStr.size(), parentBegCol + maxFragLen0);
				if (childEndRow - childBegRow > 1) {
					childBegCol = unmatchPos(childBegStr.udata(),
											 strVec.nth_data(childEndRow - 1),
											 parentBegCol + 1, childEndCol);
					if (terark_unlikely(conf.debugLevel >= 3))
						printDup("found dup1", depth, childEndRow - childBegRow,
								 childBegStr, parentBegCol, childBegCol);
				}
#if defined(USE_SUFFIX_ARRAY_TRIE)
				else if (conf.saFragMinFreq) {
				//	size_t saMinFragLen = maxFragLen3;
					size_t saMinFragLen = conf.minFragLen;
				//	size_t saMinFragLen = 12;
					if (childEndCol - parentBegCol > saMinFragLen) {
				#if 1
						auto res = conf.suffixTrie->sa_match_max_score(
							childBegStr.substr(parentBegCol), saMinFragLen, conf.saFragMinFreq);
				#else
						auto res = conf.suffixTrie->sa_match_max_length(
							childBegStr.substr(parentBegCol), conf.saFragMinFreq);
				#endif
						childBegCol = parentBegCol + res.depth;
						if (terark_unlikely(conf.debugLevel >= 3))
							printDup("found dup2", depth, res.freq(),
									childBegStr, parentBegCol, childBegCol);
					} else
						childBegCol = childEndCol;
				}
#endif
#if defined(NestLoudsTrie_EnableDelim) && defined(USE_SUFFIX_ARRAY_TRIE)
				else if (conf.bestZipLenArr) {
					size_t offset = childBegStr.udata() - strBase + parentBegCol;
					size_t length = std::min(childBegStr.size() - parentBegCol, maxFragLen2);
					auto   zipPtr = conf.bestZipLenArr + offset;
					size_t currLen = max_n(zipPtr, length);
					size_t bestLen = zipPtr[currLen];
					if (currLen >= (size_t)conf.minFragLen)
						childBegCol = parentBegCol + currLen;
					else if (bestLen >= (size_t)conf.minFragLen)
						childBegCol = parentBegCol + bestLen;
					else
						childBegCol = childEndCol;
					if (terark_unlikely(conf.debugLevel >= 3)) {
						printDup("found dup3", depth, childEndRow - childBegRow,
								 childBegStr, parentBegCol, childBegCol);
						printf("currLen=%zd bestLen=%zd\n", currLen, bestLen);
					}
				}
#endif
#if defined(NestLoudsTrie_EnableDelim)
				else if (childEndCol - parentBegCol > maxFragLen3) {
					auto str = childBegStr.udata();
					childEndCol = std::min(childEndCol, parentBegCol + maxFragLen1);
					if (conf.flags[NestLoudsTrieConfig::optSearchDelimForward]) {
						childBegCol = parentBegCol + maxFragLen3;
						for (; childBegCol < childEndCol; ++childBegCol) {
							byte_t c = str[childBegCol];
							if (conf.bestDelimBits.is1(c))
								break;
							if (childBegCol >= parentBegCol + maxFragLen2) {
								if (conf.flags[NestLoudsTrieConfig::optCutFragOnPunct] && ispunct(c))
									break;
							}
						}
					} else {
						size_t lastPunctPos = 0;
						size_t min_pos = parentBegCol + minFragLen1;
						for (childBegCol = childEndCol; childBegCol > min_pos; --childBegCol) {
							byte_t c = str[childBegCol - 1];
							if (conf.bestDelimBits.is1(c))
								goto BackwardSearchDone;
							else if (0 == lastPunctPos) {
								if (conf.flags[NestLoudsTrieConfig::optCutFragOnPunct] && ispunct(c))
									lastPunctPos = childBegCol;
							}
						}
						childBegCol = lastPunctPos ? lastPunctPos : childEndCol;
						BackwardSearchDone:;
					}
				}
#endif
				else {
					childBegCol = childEndCol;
					assert(childBegCol > parentBegCol);
				}
				size_t fragStrLen = childBegCol - parentBegCol;
				assert(fragStrLen <= 253);
				if (FastLabel)
					fragStrLen--;
				if (fragStrLen >= minLinkStrLen) {
					nestStrVecSize++; // should == m_is_link.max_rank1()
					nestStrPoolSize += fragStrLen;
					size_t nestBegCol = parentBegCol + (FastLabel ? 1 : 0);
					if (nestStrPoolFile) {
						nestStrPoolFile->oTmpBuf <<
							childBegStr.substr(nestBegCol, fragStrLen);
					}
					else {
						SortableStrVec::OffsetLength nextKey;
						nextKey.offset = size_t(childBegStr.udata() - strBase + nestBegCol);
						nextKey.length = uint32_t(fragStrLen);
						nextStrVecStore->push_back(nextKey);
					}
					if (FastLabel) {
						labelStore->push_back(childBegStr[parentBegCol]);
					} else {
						labelStore->push_back(0); // reserved for latter use
					}
					m_is_link.push_back(true);
				}
				else {
					childBegCol = parentBegCol + 1;
					labelStore->push_back(childBegStr[parentBegCol]);
					m_is_link.push_back(false);
				}
				if (terark_unlikely(conf.debugLevel >= 4))
					fprintf(stderr
						, "build_self_trie: parent=(%zd, %zd, %zd), child=(%zd %zd %zd %zd)\n"
						, parentBegRow, parentEndRow, parentBegCol
						, childBegRow, childEndRow, childBegCol, childEndCol
						);
				assert(childBegRow < childEndRow);
				// strVec.nth_size(childBegRow) may be expesive and this loop may be small
				if (childBegStr.size() == childBegCol) {
					do {
						size_t linked_node_id = m_is_link.size() - 1;
						size_t seq_id = strVec.nth_seq_id(childBegRow);
						if (linkSeqStore) {
							linkSeqStore->oTmpBuf << LinkSeq(linked_node_id, seq_id);
						} else {
							assert(index_t(-1) == linkVec[seq_id]);
							linkVec[seq_id] = linked_node_id;
						}
						childBegRow++;
					} while (childBegRow < childEndRow && strVec.nth_size(childBegRow) == childBegCol);
				}
#if !defined(NDEBUG)
                for (size_t i = childBegRow; i < childEndRow; ++i) {
                    fstring s = strVec[i];
                    assert(s.size() > childBegCol);
                }
#endif
				q2->push_back({childBegRow, childEndRow, childBegCol});
				m_louds.push_back(true);
				childBegRow = childEndRow;
			}
			m_louds.push_back(false);
		}
		q1->rewind_for_write();
		q2->complete_write();
		q1.swap(q2);
		depth++;
	}
	labelStore->complete_write();
	q1.reset();
	q2.reset();
#if !defined(NDEBUG)
	if (!linkSeqStore) {
		for (size_t i = 0; i < linkVec.size(); ++i) {
			assert(linkVec[i] < m_is_link.size());
		}
	}
#endif
	TERARK_VERIFY_EQ(m_is_link.size(), labelStore->size(), "%zd %zd");
	TERARK_VERIFY_EQ(m_louds.size(), 2 * m_is_link.size() + 1, "%zd %zd");
	if (nestStrPoolFile) {
		nestStrPoolFile->complete_write();
		nestStrVec.clear();
	} else {
		nestStrVec.m_strpool.swap(strVec.m_strpool);
		std::swap(nestStrVec.m_strpool_mem_type, strVec.m_strpool_mem_type);
	}
	strVec.clear(); // free memory
	std::unique_ptr<typename OnePassQueue<index_t>::InFile> linkVecStore;
	if (!conf.tmpDir.empty() && realTmpLevel == 2) {
		assert(!linkSeqStore);
		linkVecStore.reset(new typename OnePassQueue<index_t>::InFile(conf.tmpDir, "linkVec-"));
		linkVecStore->swap_out(&linkVec);
	}
if (nestStrPoolFile) {
	nestStrVec.m_index.resize(nestStrVecSize);
	nestStrVec.m_strpool.resize(nestStrPoolSize);
	nestStrPoolFile->read_strvec(nestStrVec);
}
else
{
	// use less memory, SEntry reduced to OffsetLength before using it
	// now restore OffsetLength to SEntry
	valvec<SortableStrVec::OffsetLength> olvec;
	nextStrVecStore->complete_write();
	nextStrVecStore->read_all(&olvec);
	nextStrVecStore.reset();
	if (sizeof(index_t) == 4 && olvec.size() >= INT32_MAX/8 * 7) {
	// olvec is used for build nextStrVecStore
		THROW_STD(length_error
			, "nextStrVec is too large, size = %zd", olvec.size());
	}
	auto pSEntry = (SortableStrVec::SEntry*)
		realloc(olvec.data(), sizeof(SortableStrVec::SEntry) * olvec.size());
	if (!pSEntry) {
		fprintf(stderr
			, "FATAL: %s:%d: realloc(%zd bytes) for nextStrVec failed\n"
			, __FILE__, __LINE__
			, sizeof(SortableStrVec::SEntry) * olvec.size());
		throw std::bad_alloc();
	}
	auto pOffsetLength = (SortableStrVec::OffsetLength*)pSEntry;
	for (size_t i = olvec.size(); i > 0; ) {
		auto ol = pOffsetLength[--i];
		pSEntry[i].offset = ol.offset;
		pSEntry[i].length = ol.length;
		pSEntry[i].seq_id = i; // now set seq_id
	}
	TERARK_VERIFY(nestStrVec.m_index.data() == nullptr);
	nestStrVec.m_index.risk_set_data(pSEntry, olvec.size());
	olvec.risk_release_ownership();
	nestStrVec.build_subkeys(conf.speedupNestTrieBuild);
    patchNestStrVec();
}
//	m_total_zpath_len = nestStrVec.sync_real_str_size();
	m_total_zpath_len = nestStrVec.str_size();
	m_is_link.build_cache(0, 0);
	TERARK_VERIFY_EQ(m_is_link.max_rank1(), nestStrVec.size(), "%zd %zd");
	if (linkVecStore) {
		linkVecStore->read_all(&linkVec);
		linkVecStore.reset();
		TERARK_VERIFY(!linkSeqStore);
	}
	else if (linkSeqStore) {
		linkSeqStore->complete_write();
#ifdef NDEBUG
		linkVec.resize_no_init(strVecSize);
#else
		linkVec.resize_fill(strVecSize, index_t(-1));
#endif
		index_t* lv = linkVec.data();
		for (size_t i = 0; i < strVecSize; ++i) {
			LinkSeq ls;
			linkSeqStore->iTmpBuf >> ls;
			lv[ls.seq_id] = ls.link_id;
		}
#ifndef NDEBUG
		for (size_t i = 0; i < strVecSize; ++i) {
			assert(lv[i] < m_is_link.size());
		}
#endif
		linkSeqStore.reset();
	}
	labelStore->read_all(&label);
}

///@param[inout] strVec
///@param[inout] label
template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_core(SortableStrVec& strVec, valvec<byte_t>& label,
           const NestLoudsTrieConfig& conf) {
	if (strVec.size() == 0) {
		return;
	}
#if defined(USE_SUFFIX_ARRAY_TRIE)
	if (conf.suffixTrie) {
		conf.suffixTrie.reset();
		strVec.compact();
	}
#endif
	strVec.reverse_keys();
	build_core_no_reverse_keys(strVec, label, conf);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
build_core_no_reverse_keys(SortableStrVec& strVec, valvec<byte_t>& label,
                           const NestLoudsTrieConfig& conf) {
	int maxLen = 0, maxIdx = -1;
	int minLen = INT_MAX;
	for (int i = 0; i < (int)strVec.size(); ++i) {
		if (maxLen < (int)strVec.m_index[i].length)
			maxLen = (int)strVec.m_index[i].length, maxIdx = i;
		minLen = std::min(minLen, (int)strVec.m_index[i].length);
	}
	minLen = std::min(255, minLen);
	int lenBits = maxLen == minLen
				? 0
				: terark_bsr_u64(maxLen - minLen) + 1
				;
	if (conf.debugLevel >= 2) {
		fprintf(stderr, "build_core: cnt=%d poolsize=%d avg=%f\n"
				, (int)strVec.size(), (int)strVec.str_size(), strVec.avg_size());
		fprintf(stderr, "build_core: maxIdx=%d maxLen=%d minLen=%d lenBits=%d data: %.*s\n"
				, maxIdx, maxLen, minLen, lenBits, maxLen, strVec.nth_data(maxIdx));
	}
	assert(strVec.m_strpool.size() >= 3);
	compress_core(strVec, conf);
	typedef typename std::conditional<FastLabel,uint64_t,index_t>::type link_uint_t;
	valvec<link_uint_t> linkVec(strVec.size(), valvec_no_init());
	for (size_t j = 0, k = 0; k < m_is_link.size(); ++k) {
		if (m_is_link[k]) {
			size_t offset = strVec.m_index[j].offset;
			size_t keylen = strVec.m_index[j].length;
		//	size_t seq_id = strVec.m_index[j].seq_id;
		//	assert(seq_id == j);
			long long val = (long long)(offset) << lenBits | (keylen - minLen);
			if (sizeof(index_t) == 4 && (val >> 8) > UINT32_MAX) {
				fprintf(stderr,
					"FATAL: %s: lenBits=%d, (val >> 8) = 0x%llX\n"
					"   Please try greater numTries!\n"
					, BOOST_CURRENT_FUNCTION
					, lenBits
					, val >> 8
					);
				abort(); // can not continue
			}
			if (FastLabel) {
				linkVec[j] = link_uint_t(val);
			} else {
				label[k] = byte_t(val);
				linkVec[j] = index_t(val >> 8);
			}
			j++;
		}
	}
	assert(label.size() == m_is_link.size());
	label.append(strVec.m_strpool);
	m_core_size = strVec.m_strpool.size();
	m_core_data = label.data() + m_is_link.size();
	m_label_data = label.risk_release_ownership();
	m_next_link.build_from(linkVec);
	m_core_len_bits = byte_t(lenBits);
	m_core_len_mask = (size_t(1) << lenBits) - 1;
	m_core_min_len = byte_t(minLen);
	m_core_max_link_val = strVec.str_size() << lenBits;
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
load_mmap_loop(NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
               size_t version,
               NativeDataInput<MemIO>& mem, size_t trieNum, size_t i) {
	typedef typename RankSelect::index_t index_t;
	index_t nth_trie = 0, node_num = 0, core_size = 0;
	uint08_t core_high_bits = 0, core_len_bits = 0;
	index_t nextNodeNum = 0;
	index_t louds_mem_size = 0, is_link_mem_size = 0;
	index_t next_link_mem_size = 0;
	index_t next_link_min_val = 0;
	mem >> nth_trie; // for padding and correct-check
	mem >> node_num;
	if (i < trieNum - 1) {
		mem >> nextNodeNum;
	}
	else {
		mem >> core_high_bits;
		mem >> core_len_bits;
		mem.skip(sizeof(index_t) - 2);
	}
	mem >> core_size;
	mem >> louds_mem_size;
	mem >> is_link_mem_size;
	mem >> next_link_mem_size;
	mem >> next_link_min_val;
	mem >> trie->m_total_zpath_len; // 8 bytes
	assert(nth_trie == i);
	if (version >= 1) {
	//	trie->m_core_min_len = mem.template load_as<index_t>() & 255; // false warning in gcc
	//	trie->m_core_max_link_val = mem.template load_as<index_t>(); // false warning in gcc
	//  to suppress gcc's false warning
		index_t minLen = 0;
		index_t maxLinkVal = 0;
		mem >> minLen;
		mem >> maxLinkVal;
		trie->m_core_min_len = byte_t(minLen & 255);
		trie->m_core_max_link_val = maxLinkVal;

		mem.skip(16); // padding
	}

	trie->m_louds.risk_mmap_from(mem.skip(louds_mem_size), louds_mem_size);
	trie->m_is_link.risk_mmap_from(mem.skip(is_link_mem_size), is_link_mem_size);
	assert(trie->m_louds.size()==2*node_num+1);
	assert(trie->m_is_link.size()==node_num);

	if (core_size && i < trieNum - 1 && nextNodeNum) {
		// new format: a trie can has both core and nested trie
		assert(version >= 1);
		// nextState=nextNodeNum-1 must be a linked node
		size_t max_link_val = trie->m_core_max_link_val + (nextNodeNum-1);
		size_t LowBits = FastLabel ? 0 : 8;
		size_t nextLinkBits
			= (max_link_val - next_link_min_val) >> LowBits == 0
			? 0
			: 1 + terark_bsr_u64((max_link_val - next_link_min_val) >> LowBits)
			;
		trie->m_next_link.risk_set_data(
			mem.skip((next_link_mem_size + 7) & ~7),
			trie->m_is_link.max_rank1(),
			size_t(next_link_min_val),
			nextLinkBits);
		assert(trie->m_next_link.size() == trie->m_is_link.max_rank1());
		trie->m_label_data = mem.skip((node_num + core_size + 7) & ~7);
		trie->m_core_data = trie->m_label_data + node_num;
		trie->m_core_len_bits = 1;
		trie->m_core_len_mask = (size_t(1) << trie->m_core_len_bits) - 1;
	}
	else {
		if (core_size) {
			trie->m_next_link.risk_set_data(
				mem.skip((next_link_mem_size + 7) & ~7),
				trie->m_is_link.max_rank1(),
				size_t(next_link_min_val),
				core_high_bits);
			assert(trie->m_next_link.size() == trie->m_is_link.max_rank1());
			trie->m_label_data = mem.skip((node_num + core_size + 7) & ~7);
			trie->m_core_data = trie->m_label_data + node_num;
			trie->m_core_len_bits = core_len_bits;
			trie->m_core_len_mask = (size_t(1) << core_len_bits) - 1;
			trie->m_core_max_link_val = size_t(core_size) << core_len_bits;
		}
		else {
			// nextState=nextNodeNum-1 must be a linked node
			size_t LowBits = FastLabel ? 0 : 8;
			size_t nextLinkBits
				= (nextNodeNum-1) >> LowBits == next_link_min_val
				? 0
				: 1 + terark_bsr_u64(((nextNodeNum-1)>>LowBits) - next_link_min_val)
				;
			trie->m_next_link.risk_set_data(
				mem.skip((next_link_mem_size + 7) & ~7),
				trie->m_is_link.max_rank1(),
				size_t(next_link_min_val),
				nextLinkBits);
			assert(trie->m_next_link.size() == trie->m_is_link.max_rank1());
			trie->m_label_data = mem.skip((node_num + 7) & ~7);
			trie->m_core_data = NULL;
			trie->m_core_len_bits = 0;
			trie->m_core_len_mask = 0;
			trie->m_core_max_link_val = 0;
		}
	}
	trie->m_core_size = core_size;
	if (i < trieNum-1 && nextNodeNum) {
		trie->m_next_trie = new NestLoudsTrieTpl<RankSelect>();
		load_mmap_loop(trie->m_next_trie, version, mem, trieNum, i+1);
	}
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
load_mmap_debug_check(NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
                      size_t trieNum, size_t i) {
#if !defined(NDEBUG)
	size_t next_node_num = trie->m_next_trie
		? trie->m_next_trie->total_states()	: 0;
	for (size_t j = 0; j < trie->m_is_link.size(); ++j) {
		if (trie->m_is_link[j]) {
			uint64_t linkVal = trie->get_link_val(j);
			if (linkVal >= trie->m_core_max_link_val) {
				size_t link_id = size_t(linkVal - trie->m_core_max_link_val);
				assert(link_id < next_node_num);
			}
			else {
				size_t offset = size_t(linkVal >> trie->m_core_len_bits);
				size_t length = size_t(linkVal &  trie->m_core_len_mask) + trie->m_core_min_len;
				assert(offset + length <= trie->m_core_size);
			}
		}
	}
	if (i < trieNum-1 && next_node_num) {
		assert(NULL != trie->m_next_trie);
		load_mmap_debug_check(trie->m_next_trie, trieNum, i+1);
	}
	else {
		assert(NULL == trie->m_next_trie);
	}
#endif
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static
byte_t&
GetLastTrie_core_min_len(const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* self) {
	auto trie = self->m_next_trie;
	if (trie) {
		while (trie->m_next_trie)
			trie = trie->m_next_trie;
		return trie->m_core_min_len;
	}
	return const_cast<byte_t&>(self->m_core_min_len);
}

template<class RankSelect, class RankSelect2, bool FastLabel>
void
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
load_mmap(const void* data, size_t size) {
	NativeDataInput<MemIO> mem; mem.set(const_cast<void*>(data), size);
	uint32_t trieNum = 0;
	uint08_t coreMinLen = 0;
	uint08_t padding0 = 0;
	uint08_t version = 0;
	mem >> trieNum;
	mem >> coreMinLen;
	mem >> padding0;
	mem >> padding0;
	mem >> version;
	load_mmap_loop(this, version, mem, trieNum, 0);
	load_mmap_debug_check(this, trieNum, 0);
	if (0 == version) {
		GetLastTrie_core_min_len(this) = coreMinLen;
	}
#if defined(TERARK_NLT_ENABLE_SEL0_CACHE)
    m_sel0_cache.resize_no_init(m_louds.size() / 256);
    uint32_t bitpos = m_louds.select0(0);
    for (size_t i = 0; i < m_sel0_cache.size(); ++i) {
        m_sel0_cache[i] = bitpos;
        bitpos += m_louds.one_seq_len(bitpos+1) + 1;
        assert(m_louds.is0(bitpos));
    }
#endif
}

template<class RankSelect, class RankSelect2, bool FastLabel>
static void
save_mmap_loop(NativeDataOutput<AutoGrownMemIO>& tmpbuf,
	size_t version,
	const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* trie,
	size_t nth_trie, bool bPrintStat)
{
	typedef typename RankSelect::index_t index_t;
	static const byte_t padding[8] = { 0 };
	if (bPrintStat)
		printf("save_mmap_s: nth_trie=%zd core[size=%zu len_bits=%d] mem[islink=%zu linkid=(%zu %zu %zu %zu) label=%zu]\n"
		, nth_trie
		, trie->m_core_size, trie->m_core_len_bits
		, trie->m_is_link.mem_size()
		, trie->m_next_link.uintbits()
		, trie->m_next_link.size()
		, trie->m_next_link.mem_size()
		, trie->m_next_link.min_val()
		, trie->total_states()
		);
	assert(trie->m_louds.mem_size() % 8 == 0);
	assert(trie->m_is_link.mem_size() % 8 == 0);
	assert(trie->m_is_link.max_rank1() == trie->m_next_link.size());
	tmpbuf << index_t(nth_trie); // for padding and correct-check
	tmpbuf << index_t(trie->total_states());
	if (trie->m_next_trie) {
		tmpbuf << index_t(trie->m_next_trie->total_states());
	}
	else {
		tmpbuf << uint08_t(trie->m_next_link.uintbits());
		tmpbuf << uint08_t(trie->m_core_len_bits);
		tmpbuf.ensureWrite(padding, sizeof(index_t) - 2);
	}
	tmpbuf << index_t(trie->m_core_size);
	tmpbuf << index_t(trie->m_louds.mem_size());
	tmpbuf << index_t(trie->m_is_link.mem_size());
	tmpbuf << index_t(trie->m_next_link.mem_size());
	tmpbuf << index_t(trie->m_next_link.min_val());
	tmpbuf << trie->m_total_zpath_len;
	if (version >= 1) {
		const static byte_t zero[16] = { 0 };
		tmpbuf << index_t(trie->m_core_min_len);
		tmpbuf << index_t(trie->m_core_max_link_val);
		tmpbuf.ensureWrite(zero, sizeof(zero)); // padding
	}
	tmpbuf.ensureWrite(trie->m_louds.data(), trie->m_louds.mem_size());
	tmpbuf.ensureWrite(trie->m_is_link.data(), trie->m_is_link.mem_size());

	tmpbuf.ensureWrite(trie->m_next_link.data(), trie->m_next_link.mem_size());
	assert(trie->m_next_link.mem_size()%8 == 0);

	tmpbuf.ensureWrite(trie->m_label_data, trie->total_states());
	if (trie->m_core_size) {
		assert(NULL != trie->m_core_data);
		tmpbuf.ensureWrite(trie->m_core_data, trie->m_core_size);
		size_t total_chars = trie->total_states() + trie->m_core_size;
		if (total_chars % 8 != 0)
			tmpbuf.ensureWrite(padding, 8 - total_chars % 8);
	}
	else {
		assert(NULL == trie->m_core_data);
		if (trie->total_states()%8 != 0)
			tmpbuf.ensureWrite(padding, 8 - trie->total_states() % 8);
	}
	assert(tmpbuf.tell() % 8 == 0);
	if (trie->m_next_trie)
		save_mmap_loop(tmpbuf, version, trie->m_next_trie, nth_trie+1, bPrintStat);
}

template<class Trie> static bool has_mixed(const Trie* trie) {
	if (NULL == trie)
		return false;
	if (trie->m_next_trie && trie->m_core_data)
		return true;
	return has_mixed(trie->m_next_trie);
}
template<class RankSelect, class RankSelect2, bool FastLabel>
static byte_t*
save_mmap_s(const NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>* self,
            size_t* pSize) {
	NativeDataOutput<AutoGrownMemIO> tmpbuf;
	tmpbuf.resize(8*1024);
	size_t trieNum = self->nest_level();
	size_t version = 0;
	if (has_mixed(self)) {
		version = 1;
	}
	tmpbuf << uint32_t(trieNum);
	tmpbuf << uint08_t(GetLastTrie_core_min_len(self));
	tmpbuf << uint08_t(0); // padding
	tmpbuf << uint08_t(0); // padding
	tmpbuf << uint08_t(version);

	bool bPrintStat = getEnvBool("LOUDS_DFA_PRINT_STAT");
	save_mmap_loop(tmpbuf, version, self, 0, bPrintStat);
	if (bPrintStat) {
		printf("%s: all_trie size = %zd\n", BOOST_CURRENT_FUNCTION, tmpbuf.tell());
	}
	*pSize = tmpbuf.tell();
	tmpbuf.shrink_to_fit();
	return tmpbuf.release();
}

// returned pointer should be freed by the caller
template<class RankSelect, class RankSelect2, bool FastLabel>
byte_t*
NestLoudsTrieTpl<RankSelect, RankSelect2, FastLabel>::
save_mmap(size_t* pSize)
const {
	return save_mmap_s(this, pSize);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
template class NestLoudsTrieTpl<rank_select_se>;
template class NestLoudsTrieTpl<rank_select_il>;
template class NestLoudsTrieTpl<rank_select_se_512>;
template class NestLoudsTrieTpl<rank_select_se_512_64>;

template class NestLoudsTrieTpl<rank_select_se_512, rank_select_mixed_se_512_0>;
template class NestLoudsTrieTpl<rank_select_il_256, rank_select_mixed_il_256_0>;
template class NestLoudsTrieTpl<rank_select_il_256, rank_select_mixed_xl_256_0>;

template class NestLoudsTrieTpl<rank_select_se_256_32, rank_select_se_256_32, true>;
template class NestLoudsTrieTpl<rank_select_il_256_32, rank_select_il_256_32, true>;
template class NestLoudsTrieTpl<rank_select_se_512_32, rank_select_se_512_32, true>;
template class NestLoudsTrieTpl<rank_select_se_512_64, rank_select_se_512_64, true>;

template class NestLoudsTrieTpl<rank_select_se_512_32, rank_select_mixed_se_512_0, true>;
template class NestLoudsTrieTpl<rank_select_il_256_32, rank_select_mixed_il_256_0, true>;
template class NestLoudsTrieTpl<rank_select_il_256_32, rank_select_mixed_xl_256_0, true>;

/*
template class NestLoudsTrieTpl<rank_select_il_256_32_41, rank_select_mixed_il_256_0, true>;
template class NestLoudsTrieTpl<rank_select_il_256_32_41, rank_select_mixed_xl_256_0, true>;
*/

} // namespace terark

