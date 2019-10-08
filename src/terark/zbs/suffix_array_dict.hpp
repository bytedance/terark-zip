#ifndef __terark_suffix_array_dict_hpp__
#define __terark_suffix_array_dict_hpp__

#pragma once

#include <memory>
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>

namespace terark {

// HashSuffixDictCacheDFA is much slower
//#define Enable_HashSuffixDictCacheDFA
#if defined(Enable_HashSuffixDictCacheDFA)
    #define SuffixDictCacheDFA_virtual virtual
#else
    #define SuffixDictCacheDFA_virtual
#endif

class TERARK_DLL_EXPORT SuffixDictCacheDFA {
protected:
	class MyBitmapSmartTrie;
	class MyDoubleArrayNode;
	class MyDoubleArrayTrie;
	const int*   m_sa_data;
	size_t       m_sa_size;
	const byte*  m_str;
	std::unique_ptr<MyBitmapSmartTrie> m_bm;
	std::unique_ptr<MyDoubleArrayTrie> m_da;
	struct BfsQueueElem;
	template<class TrieClass>
	void tpl_bfs_build_cache(TrieClass*, size_t minFreq, size_t maxBfsDepth);
public:
	SuffixDictCacheDFA();
	virtual ~SuffixDictCacheDFA();
	void build_sa(valvec<byte>& str);
	SuffixDictCacheDFA_virtual
	void bfs_build_cache(size_t minFreq, size_t maxBfsDepth);
#ifdef SuffixDictCacheDebug
	SuffixDictCacheDFA_virtual
	void pfs_build_cache(size_t minFreq);
#endif
	struct MatchStatus {
		size_t lo;
		size_t hi;
		size_t depth;
		size_t freq() const { return hi - lo; }
	};
	void sa_print_stat() const;

#ifdef SuffixDictCacheDebug
	MatchStatus sa_match_max_length(const byte*, size_t len) const noexcept;
	MatchStatus sa_match_max_length(fstring input) const noexcept {
		return sa_match_max_length(input.udata(), input.size());
	}
#endif

	SuffixDictCacheDFA_virtual
	MatchStatus da_match_max_length(const byte*, size_t len) const noexcept;
	MatchStatus da_match_max_length(fstring input) const noexcept {
		return da_match_max_length(input.udata(), input.size());
	}

	size_t sa_lower_bound(size_t lo, size_t hi, size_t depth, byte_t ch) const noexcept;
	size_t sa_upper_bound(size_t lo, size_t hi, size_t depth, byte_t ch) const noexcept;
	std::pair<size_t, size_t>
		   sa_equal_range(size_t lo, size_t hi, size_t depth, byte_t ch) const noexcept;

	MatchStatus sa_match_range(size_t lo, size_t hi, size_t depth, const byte_t* input, size_t len) const noexcept;

	/// @returns pair{lo, new_depth}
	std::pair<size_t, size_t>
		sa_match_exact(size_t lo, size_t hi, size_t depth, const byte_t* input, size_t len) const noexcept;

	const int* sa_data() const noexcept { return m_sa_data; }
	const byte* str(size_t pos) const noexcept {
		assert(pos < m_sa_size);
		return m_str + pos;
	}

	void da_swapout(class FileStream&);
	void da_swapin(class FileStream&);
};

#if defined(Enable_HashSuffixDictCacheDFA)
// Hash + DoubleArray + SuffixArray
class TERARK_DLL_EXPORT HashSuffixDictCacheDFA : public SuffixDictCacheDFA {
	struct TabValue;
	valvec<uint32_t> m_bucket;
	valvec<TabValue> m_nodes;
	unsigned m_shift;

public:
	HashSuffixDictCacheDFA();
	~HashSuffixDictCacheDFA();
	void bfs_build_cache(size_t minFreq, size_t maxBfsDepth) override;
	void pfs_build_cache(size_t minFreq) override;
	MatchStatus da_match_max_length(const byte*, size_t len) const noexcept override;
};
#endif

} // namespace terark

#endif // __terark_suffix_array_dict_hpp__

