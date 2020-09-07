#include "sortable_strvec.hpp"
#include <terark/radix_sort.hpp>
#include <terark/gold_hash_map.hpp>
#include <terark/io/DataIO_Basic.hpp>
#include <terark/util/small_memcpy.hpp>

#if defined(__GNUC__) && !defined(__CYGWIN__) && !defined(__clang__)
#include <parallel/algorithm>
#define parallel_sort __gnu_parallel::sort
#endif

// memcpy on gcc-4.9+ linux fails on some corner case
//
// this should be caused by glibc's memcpy is backward copy in a bad version,
// this bad version started in glibc-2.12.90-4, bug url:
// https://bugzilla.redhat.com/show_bug.cgi?id=638477
// now (2017-06-22) glibc.memcpy has reverted to forward copy,
// but I don't know this revert has started in which version
//
// so use memmove, it is always ok
// #define memcpyForward memcpy
#define memcpyForward memmove

// 2020-06-21 00:16 -- prefetch has no observable improve
//#define StrVec_EnablePrefetch
#if defined(StrVec_EnablePrefetch)
	#define StrVec_prefetch(cond, ptr) do if (cond) _mm_prefetch((const char*)ptr, _MM_HINT_T0); while (0)
#else
	#define StrVec_prefetch(cond, ptr)
#endif
namespace terark {

SortableStrVec::SortableStrVec() {
	m_strpool_mem_type = MemType::Malloc;
}
SortableStrVec::~SortableStrVec() {
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void SortableStrVec::reserve(size_t strNum, size_t maxStrPool) {
    m_index.reserve(strNum);
    m_strpool.reserve(maxStrPool);
}

void SortableStrVec::shrink_to_fit() {
    m_strpool.shrink_to_fit();
    m_index.shrink_to_fit();
}

size_t SortableStrVec::sync_real_str_size() {
	size_t len = 0;
	for (auto& x : m_index) len += x.length;
	m_real_str_size = len;
	return len;
}

void SortableStrVec::swap(SortableStrVec& y) {
	m_index.swap(y.m_index);
	m_strpool.swap(y.m_strpool);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void SortableStrVec::push_back(fstring str) {
	assert(str.size() < MAX_STR_LEN);
	assert(m_index.size() < MAX_STR_NUM);
	if (terark_unlikely(str.size() >= MAX_STR_LEN)) {
		THROW_STD(length_error, "str too long, size = %zd", str.size());
	}
	if (terark_unlikely(m_index.size() >= MAX_STR_NUM)) {
		THROW_STD(length_error, "m_index too large, size = %zd", m_index.size());
	}
	SEntry tmp;
	tmp.offset = m_strpool.size();
	tmp.length = uint32_t(str.size());
	tmp.seq_id = TERARK_IF_MSVC(uint32_t,)(m_index.size());
	m_index.push_back(tmp);
	m_strpool.append(str);
}

void SortableStrVec::pop_back() {
	assert(m_index.size() >= 1);
	m_strpool.resize_no_init(m_index.back().offset);
	m_index.pop_back();
}

void SortableStrVec::back_append(fstring str) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + str.size() < MAX_STR_LEN);
	m_index.back().length += str.size();
	m_strpool.append(str);
}

void SortableStrVec::back_shrink(size_t nShrink) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length >= nShrink);
	m_index.back().length -= nShrink;
	m_strpool.resize_no_init(m_strpool.size() - nShrink);
}

void SortableStrVec::back_grow_no_init(size_t nGrow) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + nGrow < MAX_STR_LEN);
	m_strpool.resize_no_init(m_strpool.size() + nGrow);
	m_index.back().length += nGrow;
}

void SortableStrVec::reverse_keys() {
	byte* base = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		assert(length < MAX_STR_LEN);
		std::reverse(base + offset, base + offset + length);
	}
}

void SortableStrVec::sort() {
	const byte* pool = m_strpool.data();
	double avgLen = double(m_strpool.size()+1) / double(m_index.size() + 1);
	double minRadixSortStrLen = UINT32_MAX; // disable radix sort by default
	if (const char* env = getenv("SortableStrVec_minRadixSortStrLen")) {
		minRadixSortStrLen = atof(env);
	}
	if (avgLen < minRadixSortStrLen) {
		auto cmp = [pool](const SEntry& x, const SEntry& y) {
			fstring sx(pool + x.offset, x.length);
			fstring sy(pool + y.offset, y.length);
			return sx < sy;
		};
		if (getEnvBool("SortableStrVec_useMergeSort", false)) {
			std::stable_sort(m_index.begin(), m_index.end(), cmp);
			return;
		}
#if defined(__GNUC__) && !defined(__CYGWIN__) && !defined(__clang__)
		const size_t paralell_threshold = (16<<20);
		if (m_index.size() > paralell_threshold &&
			getEnvBool("SortableStrVec_enableParallelSort", false))
		{
			parallel_sort(m_index.begin(), m_index.end(), cmp);
		}
		else
#endif
		std::sort(m_index.begin(), m_index.end(), cmp);
	} else { // use radix sort
		auto getChar = [pool](const SEntry& x,size_t i){return pool[x.offset+i];};
		auto getSize = [](const SEntry& x) { return x.length; };
		radix_sort_tpl(m_index.data(), m_index.size(), getChar, getSize);
	}
}

void SortableStrVec::clear() {
	m_strpool.risk_destroy(m_strpool_mem_type);
	m_index.clear();
}

struct CompareBy_offset {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.offset < y.offset;
	}
};
struct CompareBy_seq_id {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.seq_id < y.seq_id;
	}
};
struct CompareBy_lex_lenDesc {
	const byte* base;
	CompareBy_lex_lenDesc(const byte* base1) : base(base1) {}
	template<class T>
	bool operator()(const T& x, const T& y) const {
		size_t xlen = x.length, ylen = y.length;
		int ret = memcmp(base + x.offset, base + y.offset, std::min(xlen, ylen));
		if (ret) return ret < 0;
		else     return xlen > ylen; // lenDesc
	}
};
struct CompareBy_pos_lenDesc {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		size_t xpos = x.offset, ypos = y.offset;
		if (xpos != ypos) return xpos < ypos;
		return x.length > y.length; // lenDesc
	}
};

void SortableStrVec::sort_by_offset() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_offset());
}
void SortableStrVec::sort_by_seq_id() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_seq_id());
}

// m_index point to a subset of m_strpool
void SortableStrVec::build_subkeys(bool speed) {
	valvec<SEntry> subkeys;
	subkeys.swap(m_index);
	build_subkeys(speed, subkeys);
}

void SortableStrVec::build_subkeys(bool speed, valvec<SEntry>& subkeys) {
	assert(&subkeys != &m_index);
	byte* base = m_strpool.data();
	if (MemType::Malloc == m_strpool_mem_type && !speed) {
		// to reuse m_strpool, must sort by offset
		std::sort(subkeys.begin(), subkeys.end(), CompareBy_offset());
		size_t offset = 0;
		for(size_t i = 0; i < subkeys.size(); ++i) {
			SEntry s = subkeys[i];
			memcpyForward(base + offset, base + s.offset, s.length);
			subkeys[i].offset = offset;
			offset += s.length;
		}
		assert(offset <= m_strpool.size());
		m_strpool.resize_no_init(offset + 4096); // 4096 for candidate common prefix
		m_strpool.shrink_to_fit();
		m_strpool.risk_set_size(offset); // now, correct size
	}
	else {
		valvec<byte_t> subpool;
		size_t offset = 0;
		size_t subnum = subkeys.size();
		SEntry* subptr = subkeys.data();
		for(size_t i = 0; i < subnum; ++i) {
			offset += subptr[i].length;
		}
		subpool.resize_no_init(offset + 4096); // 4096 for candidate common prefix
		offset = 0;
		for(size_t i = 0; i < subnum; ++i) {
			SEntry s = subptr[i];
			size_t l = s.length;
			StrVec_prefetch(i + 4 < subnum, base + subptr[i + 4].offset);
			memcpy(subpool.data() + offset, base + s.offset, l);
			subptr[i].offset = offset;
			offset += l;
		}
		subpool.risk_set_size(offset); // now, correct size
		m_strpool.risk_destroy(m_strpool_mem_type);
		m_strpool.swap(subpool);
		m_strpool_mem_type = MemType::Malloc;
	}
	m_index.swap(subkeys);
	m_index.shrink_to_fit();
	subkeys.clear();
}

void SortableStrVec::compact() {
	byte* base = m_strpool.data();
	// to reuse m_strpool, must sort by offset
	std::sort(m_index.begin(), m_index.end(), CompareBy_offset());
	size_t offset = 0;
	for(size_t i = 0; i < m_index.size(); ++i) {
		SEntry s = m_index[i];
		memcpyForward(base + offset, base + s.offset, s.length);
		m_index[i].offset = offset;
		offset += s.length;
	}
	assert(offset <= m_strpool.size());
	m_strpool.risk_set_size(offset);
	m_strpool.shrink_to_fit();
	m_index.shrink_to_fit();
}

// only load first Int, if len is too small, set other bytes in Int as zero
template<class Uint, size_t Len>
inline Uint binaryToIntHigh(const unsigned char* bin) {
	Uint val = 0;
	memcpy(&val, bin, sizeof(Uint) < Len ? sizeof(Uint) : Len);
#if defined(BOOST_ENDIAN_LITTLE_BYTE)
	return val << (sizeof(Uint) - Len) * 8;
#else
	return val;
#endif
}

void SortableStrVec::compress_strpool(int compressLevel) {
	if (m_index.size() < 1)
		return;

	if (compressLevel < 1)
		return;
	compress_strpool_level_1();

	// 2 or 3
	if (compressLevel == 2)
		compress_strpool_level_2();
	else if (compressLevel >= 3)
		compress_strpool_level_3();
}

void SortableStrVec::compress_strpool_level_1() {
//	std::sort(m_index.begin(), m_index.end(), CompareBy_lex_lenDesc(m_strpool.data()));
	this->sort();
	std::reverse(m_index.begin(), m_index.end()); // to descending order

	valvec<byte> strpool(m_strpool.size() + 3, valvec_reserve());
	for (size_t i = 0; i < m_index.size(); ++i) {
		strpool.append((*this)[i]);
	}
	m_strpool.clear(); // free memory
	size_t offset = 0;
	for (size_t i = 0; i < m_index.size(); ++i) {
		m_index[i].offset = offset;
		offset += m_index[i].length;
	}
	offset = m_index[0].endpos();
	for (size_t i = 1; i < m_index.size(); ++i) { // dedup loop
		size_t curlen = m_index[i].length;
		if (m_index[i-1].length >= curlen &&
				memcmp(&strpool[m_index[i].offset],
					   &strpool[m_index[i-1].offset], curlen) == 0) {
			m_index[i].offset = m_index[i-1].offset;
		} else {
			memcpyForward(&strpool[offset], &strpool[m_index[i].offset], curlen);
			m_index[i].offset = offset;
			offset += curlen;
		}
	}
	assert(offset <= strpool.size());
	if (getEnvBool("SortableStrVec_statCompressLevel1", false)) {
		long oldsize = strpool.size(), newsize = offset;
		fprintf(stderr, "SortableStrVec::compress1: oldsize=%ld newsize=%ld\n", oldsize, newsize);
	}
	strpool.risk_set_size(offset + 3);
	strpool.fill(offset, 3, 0);
	strpool.shrink_to_fit();
	strpool.risk_set_size(offset);
	m_strpool.swap(strpool);
	std::reverse(m_index.begin(), m_index.end()); ///< to ascending order
}

// dup strings never overlap
void SortableStrVec::compress_strpool_level_2() {
	uint32_t nBucket = (uint32_t)__hsm_stl_next_prime(m_strpool.size());
	AutoFree<uint32_t> bucket(nBucket, UINT32_MAX);
	AutoFree<uint32_t> hhlink(m_strpool.size(), UINT32_MAX);
	size_t zippedSize = 0;
	size_t prevOldPos = size_t(-1);
	byte* pool = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		assert(m_index[i].length >= 3);
		size_t len = m_index[i].length;
		size_t pos = m_index[i].offset;
		if (pos == prevOldPos) {
			assert(i > 0);
			assert(len <= m_index[i-1].length); // compress_level_1 ensures this
			m_index[i].offset = m_index[i-1].offset;
			continue;
		}
		prevOldPos = pos;
		if (len <= 3) {
			size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit;
				assert(hit + 3 <= zippedSize);
				if (memcmp(pool + hit, pool + pos, 3) == 0) {
					m_index[i].offset = hit;
					zippedSize = std::max(zippedSize, hit + 3);
					goto Found3;
				}
				pHit = &hhlink[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			*pHit = zippedSize; // append to hash list end, zippedSize is new pos
		} else {
			for (size_t j = 0; j < len - 2; ++j) { // never overlap
				size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos+j) % nBucket;
				uint32_t* pHit = &bucket[hashMod];
				while (UINT32_MAX != *pHit) {
					size_t hit = *pHit;
					if (memcmp(pool + hit, pool + pos, 3) == 0)
						goto Found3_n;
					pHit = &hhlink[hit];
				}
				*pHit = zippedSize + j;
			Found3_n:;
			}
		}
		m_index[i].offset = zippedSize;
		memcpyForward(pool + zippedSize, pool + pos, len);
		zippedSize += len;
	Found3:;
	}
	fprintf(stderr, "zipLen3: oldlen=%d newlen=%d\n", (int)m_strpool.size(), (int)zippedSize);
	std::fill_n(bucket.p, nBucket, UINT32_MAX);
	std::fill_n(hhlink.p, zippedSize, UINT32_MAX);
	m_strpool.risk_set_size(zippedSize);

	// may be a very long string preceding many short strings which are
	// all reference to a substring of the long string
	size_t prevMaxLen = 0;          // the long string's len
	size_t prevNewPos = size_t(-1); // the long string's new pos
	prevOldPos = m_strpool.size();  // the long string's old pos
	zippedSize = 0;
	std::sort(m_index.begin(), m_index.end(), CompareBy_pos_lenDesc());
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t const len = m_index[i].length;
		size_t const pos = m_index[i].offset;
		assert(len >= 3);
		if (pos >= prevOldPos && pos + len <= prevOldPos + prevMaxLen) {
			assert(i > 0);
			m_index[i].offset = prevNewPos + (pos - prevOldPos);
			continue;
		}
		if (len == 3) {
			m_index[i].offset = zippedSize;
			memcpyForward(pool + zippedSize, pool + pos, 3);
			zippedSize += 3;
			continue;
		}
		assert(len >= 4);
		size_t const maxZipLen = 8; // long string is slow and unlikely to be zipped
		size_t const prefixLen = std::min(len, maxZipLen);
		if (len <= maxZipLen) {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
				assert(hit + 4 <= zippedSize);
				if (hit + len <= zippedSize &&
						memcmp(pool + hit, pool + pos, len) == 0) {
					m_index[i].offset = hit;
					goto Found4;
				}
				pHit = &hhlink[hit];
			}
			// Not found, insert it
			*pHit = zippedSize; // append to hash list end
		}
		for(size_t j = (len <= maxZipLen) ? 1 : 0; j < len - 3; ++j) {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos+j) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit;
				if (memcmp(pool + hit, pool + pos+j, prefixLen) == 0)
					goto Found_prefixLen;
				pHit = &hhlink[hit];
			}
			// Not found, insert it
			*pHit = zippedSize + j; // append to hash list end
		Found_prefixLen:;
		}
		prevMaxLen = len;
		prevOldPos = pos;
		prevNewPos = zippedSize;
		m_index[i].offset = zippedSize;
		memcpyForward(pool + zippedSize, pool + pos, len);
		zippedSize += len;
	Found4:;
	}
	m_strpool.resize_no_init(zippedSize);
}

// dup strings maybe overlap
void SortableStrVec::compress_strpool_level_3() {
	uint32_t nBucket = (uint32_t)__hsm_stl_next_prime(m_strpool.size());
	AutoFree<uint32_t> bucket3(nBucket, UINT32_MAX);
	AutoFree<uint32_t> bucket4(nBucket, UINT32_MAX);
	AutoFree<uint32_t> hhlink3(m_strpool.size(), UINT32_MAX);
	AutoFree<uint32_t> hhlink4(m_strpool.size(), UINT32_MAX);
	size_t zippedSize = 0;
	size_t prevOldPos = size_t(-1);
	byte* pool = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t len = m_index[i].length;
		size_t pos = m_index[i].offset;
		if (pos == prevOldPos) {
			assert(i > 0);
			m_index[i].offset = m_index[i-1].offset;
			continue;
		}
		prevOldPos = pos;
		if (len < 4) {
			assert(3 == len);
			size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket3[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
			// range[zippedSize-minStrLen, zippedSize) would never be hitted
				assert(hit + 3 < zippedSize);
				if (memcmp(pool + hit, pool + pos, 3) == 0) {
					m_index[i].offset = hit;
					zippedSize = std::max(zippedSize, hit + 3);
					goto Found;
				}
				pHit = &hhlink3[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			memmove(pool + zippedSize, pool + pos, 3);
			pos = m_index[i].offset = zippedSize;
			*pHit = pos; // append to hash list end
		}
		else {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket4[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
			// range[zippedSize-minStrLen, zippedSize) would never be hitted
				assert(hit + 3 < zippedSize);
				if (hit + len <= zippedSize) {
					if (memcmp(pool + hit, pool + pos, len) == 0) {
						m_index[i].offset = hit;
						zippedSize = std::max(zippedSize, hit + len);
						goto Found;
					}
				} else { // this would be a very rare case and hard to handle
					// and the compression rate gain would be very little
				}
				pHit = &hhlink4[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			memmove(pool + zippedSize, pool + pos, len);
			pos = m_index[i].offset = zippedSize;
			*pHit = pos; // append to hash list end

	// just insert once
	#define HASH_INSERT_HEAD_3(beg, end) \
			for (size_t k = beg; k < end; ++k) { \
				hashMod = binaryToIntHigh<uint32_t, 3>(pool + k) % nBucket; \
				size_t hit = bucket3[hashMod]; \
				while (UINT32_MAX != hit) { \
					if (memcmp(pool + hit, pool + pos, 3) == 0) \
						goto BOOST_PP_CAT(Found_, __LINE__); \
					hit = hhlink3[hit]; \
				} \
				hhlink3[k] = bucket3[hashMod]; \
				bucket3[hashMod] = uint32_t(k); \
				BOOST_PP_CAT(Found_, __LINE__):; \
			}
	// insert multiple, LEN would be 4 or 8, Uint would be uint32_t or uint64_t
	#define HASH_INSERT_HEAD_4(beg, end) \
			for (size_t k = beg; k < end; ++k) { \
				hashMod = unaligned_load<uint32_t>(pool + k) % nBucket; \
				hhlink4[k] = bucket4[hashMod]; \
				bucket4[hashMod] = uint32_t(k); \
			}
			HASH_INSERT_HEAD_3(pos + 0, pos + len - 2);
			HASH_INSERT_HEAD_4(pos + 1, pos + len - 3);
			if (i) {
				HASH_INSERT_HEAD_3(pos - 2, pos);
				HASH_INSERT_HEAD_4(pos - 3, pos);
			}
		}
		zippedSize += len;
	Found:;
	}
	m_strpool.resize_no_init(zippedSize);

	bool printHistogram = false;
	if (const char* env = getenv("SortableStrVec_printHistogram")) {
		printHistogram = atoi(env) ? 1 : 0;
	}
	if (printHistogram) {
		gold_hash_map<int, int> cntHist, lenHist;
		for(size_t i = 0; i < m_index.size(); ++i) {
			size_t len = m_index[i].length;
			size_t pos = m_index[i].offset;
			for(size_t j = 0; j < len; ++j) {
				size_t hashMod = unaligned_load<uint32_t>(pool + pos + j) % nBucket;
				uint32_t hit = bucket4[hashMod];
				int listLen = 0;
				while (UINT32_MAX != hit) {
					listLen++;
					hit = hhlink4[hit];
				}
				cntHist[listLen]++;
			}
			lenHist[len]++;
		}
		typedef std::pair<int,int> ii;
		auto cmpKeyDest = [](ii x, ii y) { return x.first > y.first; };
		cntHist.sort(cmpKeyDest);
		lenHist.sort(cmpKeyDest);
		for (size_t i = cntHist.beg_i(); i < cntHist.end_i(); ++i) {
			int key = cntHist.key(i), val = cntHist.val(i);
			fprintf(stderr, "histogram: listLen=%d freq=%d\n", key, val);
		}
		for (size_t i = lenHist.beg_i(); i < lenHist.end_i(); ++i) {
			int key = lenHist.key(i), val = lenHist.val(i);
			fprintf(stderr, "histogram: strLen=%d freq=%d\n", key, val);
		}
	}
}

void SortableStrVec::make_ascending_offset() {
	valvec<byte> tmp(m_strpool.size(), valvec_reserve());
	const byte* oldptr = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		m_index[i].offset = tmp.size();
		tmp.append(oldptr + offset, length);
	}
	m_strpool.swap(tmp);
}

void SortableStrVec::make_ascending_seq_id() {
	SEntry* a = m_index.data();
	size_t  n = m_index.size();
	for (size_t i = 0; i < n; ++i) {
		a[i].seq_id = i;
	}
}

size_t SortableStrVec::lower_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset < offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::upper_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset <= offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
	assert(lo < hi);
	assert(hi <= m_index.size());
	assert(pos < m_index[lo].length);
	SEntry const* a = m_index.data();
	byte   const* s = m_strpool.data();
#if !defined(NDEBUG)
	byte_t const kh = s[a[lo].offset + pos];
	assert(kh == ch);
#endif
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		assert(pos < a[mid].length);
		StrVec_prefetch(true, a + (lo+mid)/2);
		StrVec_prefetch(true, a + (hi+mid)/2);
		if (s[a[mid].offset + pos] <= ch)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

static inline
bool str_less(const void* xp, size_t xn, const void* yp, size_t yn) {
	ptrdiff_t n = std::min(xn, yn);
	int ret = memcmp(xp, yp, n);
	if (ret)
		return ret < 0;
	else
		return xn < yn;
}

terark_flatten
size_t SortableStrVec::lower_bound(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (str_less(strMid, lenMid, key.p, key.n))
            lo = mid + 1;
        else
            hi = mid;
    }
	return lo;
}

terark_flatten
size_t SortableStrVec::upper_bound(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (!str_less(key.p, key.n, strMid, lenMid))
            lo = mid + 1;
        else
            hi = mid;
    }
	return lo;
}

terark_flatten
size_t SortableStrVec::find(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (str_less(strMid, lenMid, key.p, key.n))
            lo = mid + 1;
        else
            hi = mid;
    }
    size_t num = m_index.size();
    if (lo < num) {
        const byte_t* strLo = index[lo].offset + strbase;
        const size_t  lenLo = index[lo].length;
        if (lenLo == size_t(key.n) && memcmp(strLo, key.p, lenLo) == 0)
            return lo;
    }
	return num;
}

size_t SortableStrVec::max_strlen() const {
    const SEntry* index = m_index.data();
    const size_t  count = m_index.size();
    size_t maxlen = 0;
    for(size_t i = 0; i < count; ++i) {
        size_t x = index[i].length;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen;
}

///////////////////////////////////////////////////////////////////////////////
SortThinStrVec::SortThinStrVec() {
	m_strpool_mem_type = MemType::Malloc;
}
SortThinStrVec::~SortThinStrVec() {
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void SortThinStrVec::reserve(size_t strNum, size_t maxStrPool) {
    m_index.reserve(strNum);
    m_strpool.reserve(maxStrPool);
}

void SortThinStrVec::shrink_to_fit() {
    m_strpool.shrink_to_fit();
    m_index.shrink_to_fit();
}

void SortThinStrVec::swap(SortThinStrVec& y) {
	m_index.swap(y.m_index);
	m_strpool.swap(y.m_strpool);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void SortThinStrVec::push_back(fstring str) {
	assert(str.size() < MAX_STR_LEN);
	assert(m_index.size() < MAX_STR_NUM);
	if (terark_unlikely(str.size() >= MAX_STR_LEN)) {
		THROW_STD(length_error, "str too long, size = %zd", str.size());
	}
	if (terark_unlikely(m_index.size() >= MAX_STR_NUM)) {
		THROW_STD(length_error, "m_index too large, size = %zd", m_index.size());
	}
	SEntry tmp;
	tmp.offset = m_strpool.size();
	tmp.length = uint32_t(str.size());
	m_index.push_back(tmp);
	m_strpool.append(str);
}

void SortThinStrVec::pop_back() {
	assert(m_index.size() >= 1);
	m_strpool.resize_no_init(m_index.back().offset);
	m_index.pop_back();
}

void SortThinStrVec::back_append(fstring str) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + str.size() < MAX_STR_LEN);
	m_index.back().length += str.size();
	m_strpool.append(str);
}

void SortThinStrVec::back_shrink(size_t nShrink) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length >= nShrink);
	m_index.back().length -= nShrink;
	m_strpool.resize_no_init(m_strpool.size() - nShrink);
}

void SortThinStrVec::back_grow_no_init(size_t nGrow) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + nGrow < MAX_STR_LEN);
	m_strpool.resize_no_init(m_strpool.size() + nGrow);
	m_index.back().length += nGrow;
}

void SortThinStrVec::reverse_keys() {
	byte* base = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		assert(length < MAX_STR_LEN);
		std::reverse(base + offset, base + offset + length);
	}
}

void SortThinStrVec::sort() {
	const byte* pool = m_strpool.data();
	double avgLen = double(m_strpool.size()+1) / double(m_index.size() + 1);
	double minRadixSortStrLen = UINT32_MAX; // disable radix sort by default
	if (const char* env = getenv("SortThinStrVec_minRadixSortStrLen")) {
		minRadixSortStrLen = atof(env);
	}
	if (avgLen < minRadixSortStrLen) {
		auto cmp = [pool](const SEntry& x, const SEntry& y) {
			fstring sx(pool + x.offset, x.length);
			fstring sy(pool + y.offset, y.length);
			return sx < sy;
		};
		if (getEnvBool("SortThinStrVec_useMergeSort", false)) {
			std::stable_sort(m_index.begin(), m_index.end(), cmp);
			return;
		}
#if defined(__GNUC__) && !defined(__CYGWIN__) && !defined(__clang__)
		const size_t paralell_threshold = (16<<20);
		if (m_index.size() > paralell_threshold &&
			getEnvBool("SortThinStrVec_enableParallelSort", false))
		{
			parallel_sort(m_index.begin(), m_index.end(), cmp);
		}
		else
#endif
		std::sort(m_index.begin(), m_index.end(), cmp);
	} else { // use radix sort
		auto getChar = [pool](const SEntry& x,size_t i){return pool[x.offset+i];};
		auto getSize = [](const SEntry& x) { return x.length; };
		radix_sort_tpl(m_index.data(), m_index.size(), getChar, getSize);
	}
}

void SortThinStrVec::clear() {
	m_strpool.risk_destroy(m_strpool_mem_type);
	m_index.clear();
}

void SortThinStrVec::sort_by_offset() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_offset());
}

// m_index point to a subset of m_strpool
void SortThinStrVec::build_subkeys() {
	valvec<SEntry> subkeys;
	subkeys.swap(m_index);
	build_subkeys(subkeys);
}

void SortThinStrVec::build_subkeys(valvec<SEntry>& subkeys) {
	assert(&subkeys != &m_index);
	byte* base = m_strpool.data();
	// to reuse m_strpool, must sort by offset
	std::sort(subkeys.begin(), subkeys.end(), CompareBy_offset());
	size_t offset = 0;
	for(size_t i = 0; i < subkeys.size(); ++i) {
		SEntry s = subkeys[i];
		memcpyForward(base + offset, base + s.offset, s.length);
		subkeys[i].offset = offset;
		offset += s.length;
	}
	assert(offset <= m_strpool.size());
	m_strpool.risk_set_size(offset);
	m_strpool.shrink_to_fit();
	m_index.swap(subkeys);
	m_index.shrink_to_fit();
	subkeys.clear();
}

void SortThinStrVec::make_ascending_offset() {
	valvec<byte> tmp(m_strpool.size(), valvec_reserve());
	const byte* oldptr = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		m_index[i].offset = tmp.size();
		tmp.append(oldptr + offset, length);
	}
	m_strpool.swap(tmp);
}

size_t SortThinStrVec::lower_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset < offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortThinStrVec::upper_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset <= offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortThinStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
	assert(lo < hi);
	assert(hi <= m_index.size());
	assert(pos < m_index[lo].length);
	SEntry const* a = m_index.data();
	byte   const* s = m_strpool.data();
#if !defined(NDEBUG)
	byte_t const kh = s[a[lo].offset + pos];
	assert(kh == ch);
#endif
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		assert(pos < a[mid].length);
		StrVec_prefetch(true, a + (lo+mid)/2);
		StrVec_prefetch(true, a + (hi+mid)/2);
		if (s[a[mid].offset + pos] <= ch)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

terark_flatten
size_t SortThinStrVec::lower_bound(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (str_less(strMid, lenMid, key.p, key.n))
            lo = mid + 1;
        else
            hi = mid;
    }
	return lo;
}

terark_flatten
size_t SortThinStrVec::upper_bound(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (!str_less(key.p, key.n, strMid, lenMid))
            lo = mid + 1;
        else
            hi = mid;
    }
	return lo;
}

terark_flatten
size_t SortThinStrVec::find(fstring key) const {
    const SEntry* index = m_index.data();
    const byte_t* strbase = m_strpool.data();
    size_t lo = 0, hi = m_index.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        const byte_t* strMid = index[mid].offset + strbase;
        const size_t  lenMid = index[mid].length;
        if (str_less(strMid, lenMid, key.p, key.n))
            lo = mid + 1;
        else
            hi = mid;
    }
    size_t num = m_index.size();
    if (lo < num) {
        const byte_t* strLo = index[lo].offset + strbase;
        const size_t  lenLo = index[lo].length;
        if (lenLo == size_t(key.n) && memcmp(strLo, key.p, lenLo) == 0)
            return lo;
    }
	return num;
}

size_t SortThinStrVec::max_strlen() const {
    const SEntry* index = m_index.data();
    const size_t  count = m_index.size();
    size_t maxlen = 0;
    for(size_t i = 0; i < count; ++i) {
        size_t x = index[i].length;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen;
}

///////////////////////////////////////////////////////////////////////////////

FixedLenStrVec::~FixedLenStrVec() {
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void FixedLenStrVec::reserve(size_t strNum, size_t maxStrPool) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(m_fixlen * strNum == maxStrPool);
    m_strpool.reserve(maxStrPool);
}

void FixedLenStrVec::shrink_to_fit() {
    assert(m_fixlen * m_size == m_strpool.size());
    m_strpool.shrink_to_fit();
}

void FixedLenStrVec::risk_release_ownership() {
    m_strpool.risk_release_ownership();
    m_size = 0;
}

void FixedLenStrVec::swap(FixedLenStrVec& y) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(y.m_fixlen * y.m_size == y.m_strpool.size());
    std::swap(m_fixlen, y.m_fixlen);
    std::swap(m_size  , y.m_size);
    m_strpool.swap(y.m_strpool);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void FixedLenStrVec::push_back(fstring str) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(str.size() == m_fixlen);
    if (str.size() != m_fixlen) {
        THROW_STD(length_error,
            "expected: %zd, real: %zd", m_fixlen, str.size());
    }
    m_strpool.append(str.data(), str.size());
    m_size++;
}

void FixedLenStrVec::pop_back() {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(m_strpool.size() > m_fixlen);
    m_strpool.pop_n(m_fixlen);
    m_size--;
}

/*
void FixedLenStrVec::back_append(fstring str) {
}
void FixedLenStrVec::back_shrink(size_t nShrink) {
}
void FixedLenStrVec::back_grow_no_init(size_t nGrow) {
}
*/

void FixedLenStrVec::reverse_keys() {
    assert(m_fixlen > 0);
    assert(m_fixlen * m_size == m_strpool.size());
    byte_t* beg = m_strpool.begin();
    byte_t* end = m_strpool.end();
    size_t  fixlen = m_fixlen;
    while (beg < end) {
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg;
        byte_t* hi = beg + fixlen;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        beg += fixlen;
    }
}

#if defined(_MSC_VER) || defined(__APPLE__)
static int CmpFixLenStr(void* ctx, const void* x, const void* y)
#else
static int CmpFixLenStr(const void* x, const void* y, void* ctx)
#endif
{
    size_t fixlen = (size_t)(ctx);
    return memcmp(x, y, fixlen);
}

void FixedLenStrVec::sort() {
    assert(m_fixlen * m_size == m_strpool.size());
    sort_raw(m_strpool.data(), m_size, m_fixlen);
}

void FixedLenStrVec::sort_raw(void* base, size_t num, size_t fixlen) {
#ifdef _MSC_VER
    #define QSortCtx qsort_s
#elif defined(__APPLE__)
    auto QSortCtx = [](void *base, size_t size, size_t nmemb,
    		           int (*compar)(void *, const void *, const void *),
					   void *thunk) {
		qsort_r(base, nmemb, size, thunk, compar);
    };
#else
    #define QSortCtx qsort_r
#endif
    QSortCtx(base, num, fixlen, CmpFixLenStr, (void*)(fixlen));
}

void FixedLenStrVec::clear() {
//  assert is not needed
//  assert(m_fixlen * m_size == m_strpool.size());
    m_size = 0;
    m_strpool.risk_destroy(m_strpool_mem_type);
}

size_t FixedLenStrVec::lower_bound_by_offset(size_t offset) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(offset <= m_strpool.size());
    size_t  fixlen = m_fixlen;
    size_t  idx = offset / fixlen;
    if (offset % fixlen == 0) {
        return idx;
    }
    else {
        return idx + 1;
    }
}

size_t FixedLenStrVec::upper_bound_by_offset(size_t offset) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(offset <= m_strpool.size());
    size_t  fixlen = m_fixlen;
    size_t  idx = offset / fixlen;
    return  idx + 1;
}

size_t FixedLenStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(lo < hi);
    assert(hi <= m_size);
    assert(pos < m_fixlen);
    size_t  const fixlen = m_fixlen;
    byte_t  const* s = m_strpool.data();
#if !defined(NDEBUG)
    const byte_t kh = s[fixlen * lo + pos];
    assert(kh == ch);
#endif
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        assert(pos < fixlen);
		StrVec_prefetch(true, s + fixlen*((lo+mid)/2));
		StrVec_prefetch(true, s + fixlen*((hi+mid)/2));
        if (s[fixlen * mid + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

terark_flatten
size_t FixedLenStrVec::lower_bound(size_t lo, size_t hi, fstring key) const {
	assert(m_fixlen * m_size == m_strpool.size());
	assert(lo <= hi);
	assert(hi <= m_size);
	auto fixlen = m_fixlen;
    if (key.size() <= fixlen) {
        // lower_bound is correct after zero padding key
        return m_lower_bound_prefix(this, lo, hi, fixlen, key.data());
    }
    else {
        // lower_bound is wrong after cutoff key, so
        // fall through, run general code
    }
	auto data = m_strpool.data();
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * mid_idx;
		if (fstring(data + mid_pos, fixlen) < key)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
size_t FixedLenStrVec::upper_bound(size_t lo, size_t hi, fstring key) const {
	assert(m_fixlen * m_size == m_strpool.size());
	assert(lo <= hi);
	assert(hi <= m_size);
	auto fixlen = m_fixlen;
    if (key.size() >= fixlen) {
        // upper_bound is correct after cutoff key
        return m_upper_bound_fixed(this, lo, hi, fixlen, key.data());
    }
    else {
        // upper_bound is wrong after zero padding key, so
        // fall through, run general code
    }
	auto data = m_strpool.data();
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * mid_idx;
		if (fstring(data + mid_pos, fixlen) <= key)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

template<bool IsFixed>
terark_flatten
static size_t Fixed_lower_bound_slow(const FixedLenStrVec* sv,
                                     size_t lo, size_t hi,
                                     size_t kn, const void* key) {
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	auto fixlen = sv->m_fixlen;
	auto data = sv->m_strpool.data();
    if (IsFixed) {
        assert(kn == fixlen);
    } else {
        kn = std::min(kn, fixlen);
    }
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * mid_idx;
		if (memcmp(data + mid_pos, key, kn) < 0)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

template<bool IsFixed>
terark_flatten
static size_t Fixed_upper_bound_slow(const FixedLenStrVec* sv,
                                     size_t lo, size_t hi,
                                     size_t kn, const void* key) {
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	auto fixlen = sv->m_fixlen;
	auto data = sv->m_strpool.data();
    if (IsFixed) {
        assert(kn == fixlen);
    } else {
        kn = std::min(kn, fixlen);
    }
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * mid_idx;
		if (memcmp(data + mid_pos, key, kn) <= 0)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

template<bool IsFixed, class Uint>
terark_flatten
static size_t Fixed_lower_bound(const FixedLenStrVec* sv,
                                size_t lo, size_t hi,
                                size_t kn, const void* key) {
	assert(sizeof(Uint) == sv->m_fixlen);
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	assert(size_t(sv->m_strpool.data()) % sizeof(Uint) == 0);
	Uint ukey;
    if (IsFixed) {
        assert(kn == sizeof(Uint));
        ukey = unaligned_load<Uint>(key);
    } else {
        ukey = 0;
        tiny_memcpy_align_1(&ukey, key, std::min(kn, sizeof(Uint)));
    }
	auto data = (const Uint*)sv->m_strpool.data();
	BYTE_SWAP_IF_LITTLE_ENDIAN(ukey);
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		Uint   mid_val = data[mid_idx];
		BYTE_SWAP_IF_LITTLE_ENDIAN(mid_val);
		if (mid_val < ukey)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

template<bool IsFixed, class Uint>
terark_flatten
static size_t Fixed_upper_bound(const FixedLenStrVec* sv,
                                size_t lo, size_t hi,
                                size_t kn, const void* key) {
	assert(sizeof(Uint) == sv->m_fixlen);
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	assert(size_t(sv->m_strpool.data()) % sizeof(Uint) == 0);
	Uint ukey;
    if (IsFixed) {
        assert(kn == sizeof(Uint));
        ukey = unaligned_load<Uint>(key);
    } else { // upper_bound_prefix is semantically different from upper_bound
        ukey = Uint(-1); //<- for upper_bound, this is wrong
        tiny_memcpy_align_1(&ukey, key, std::min(kn, sizeof(Uint)));
    }
	auto data = (const Uint*)sv->m_strpool.data();
	BYTE_SWAP_IF_LITTLE_ENDIAN(ukey);
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		Uint   mid_val = data[mid_idx];
		BYTE_SWAP_IF_LITTLE_ENDIAN(mid_val);
		if (mid_val <= ukey)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

FixedLenStrVec::FixedLenStrVec(size_t fixlen) {
    m_lower_bound_fixed = &Fixed_lower_bound_slow<1>;
    m_upper_bound_fixed = &Fixed_upper_bound_slow<1>;
    m_lower_bound_prefix = &Fixed_lower_bound_slow<0>;
    m_upper_bound_prefix = &Fixed_upper_bound_slow<0>;
    m_fixlen = fixlen;
    m_size = 0;
    m_strpool_mem_type = MemType::Malloc;
}

void FixedLenStrVec::optimize_func() {
    switch (m_fixlen) {
    default:
        m_lower_bound_fixed = &Fixed_lower_bound_slow<1>;
        m_upper_bound_fixed = &Fixed_upper_bound_slow<1>;
        m_lower_bound_prefix = &Fixed_lower_bound_slow<0>;
        m_upper_bound_prefix = &Fixed_upper_bound_slow<0>;
        break;
#define SetFuncPtr(Uint) \
    case sizeof(Uint): \
        m_lower_bound_fixed = &Fixed_lower_bound<1, Uint>; \
        m_upper_bound_fixed = &Fixed_upper_bound<1, Uint>; \
        m_lower_bound_prefix = &Fixed_lower_bound<0, Uint>; \
        m_upper_bound_prefix = &Fixed_upper_bound<0, Uint>; \
        break
//----------------------------------------------------------
        SetFuncPtr(uint08_t);
        SetFuncPtr(uint16_t);
        SetFuncPtr(uint32_t);
        SetFuncPtr(uint64_t);
#undef  SetFuncPtr
    }
}

///////////////////////////////////////////////////////////////////////////////

SortedStrVec::SortedStrVec() {
	m_offsets_mem_type = MemType::Malloc;
	m_strpool_mem_type = MemType::Malloc;
}
SortedStrVec::~SortedStrVec() {
	m_offsets.risk_destroy(m_offsets_mem_type);
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void SortedStrVec::reserve(size_t strNum, size_t maxStrPool) {
    m_strpool.reserve(maxStrPool);
    m_offsets.resize_with_wire_max_val(strNum+1, maxStrPool);
    m_offsets.resize(0);
}

void SortedStrVec::shrink_to_fit() {
    if (0 == m_offsets.size()) {
        if (0 == m_offsets.uintbits()) {
            m_offsets.resize_with_wire_max_val(0, 1ul);
        }
        m_offsets.push_back(0);
    }
    m_strpool.shrink_to_fit();
    m_offsets.shrink_to_fit();
}

void SortedStrVec::swap(SortedStrVec& y) {
    m_offsets.swap(y.m_offsets);
    m_strpool.swap(y.m_strpool);
	std::swap(m_offsets_mem_type, y.m_offsets_mem_type);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void SortedStrVec::push_back(fstring str) {
    if (terark_unlikely(m_strpool.size() + str.size() > m_offsets.uintmask())) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    if (terark_unlikely(m_offsets.size() == 0)) {
        if (m_offsets.uintbits() == 0) {
            THROW_STD(invalid_argument,
                "m_offsets.uintbits() == 0, Object is not initialized");
        }
        m_offsets.push_back(0);
    }
    m_strpool.append(str.data(), str.size());
    m_offsets.push_back(m_strpool.size());
}

void SortedStrVec::pop_back() {
    assert(m_offsets.size() > 2);
    m_strpool.risk_set_size(m_offsets.get(m_offsets.size()-2));
    m_offsets.resize(m_offsets.size()-1);
}

void SortedStrVec::back_append(fstring str) {
    if (m_strpool.size() + str.size() > m_offsets.uintmask()) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    m_strpool.append(str.data(), str.size());
    m_offsets.set_wire(m_offsets.size()-1, m_strpool.size());
}

void SortedStrVec::back_shrink(size_t nShrink) {
    size_t lastIdx = m_offsets.size() - 2;
    if (this->nth_size(lastIdx) < nShrink) {
        THROW_STD(length_error,
            "nShrink = %zd, strlen = %zd", nShrink, nth_size(lastIdx));
    }
    m_strpool.pop_n(nShrink);
    m_offsets.set_wire(lastIdx+1, m_strpool.size());
}

void SortedStrVec::back_grow_no_init(size_t nGrow) {
    if (m_strpool.size() + nGrow > m_offsets.uintmask()) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    assert(m_offsets.size() >= 2);
    m_strpool.resize_no_init(m_strpool.size() + nGrow);
    m_offsets.set_wire(m_offsets.size()-1, m_strpool.size());
}

void SortedStrVec::reverse_keys() {
    byte_t* beg = m_strpool.begin();
    size_t  offset = 0;
    for(size_t i = 0, n = m_offsets.size()-1; i < n; ++i) {
        size_t endpos = m_offsets[i+1];
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg + offset;
        byte_t* hi = beg + endpos;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        offset = endpos;
    }
}

void SortedStrVec::sort() {
    THROW_STD(invalid_argument, "This method is not supported");
}

void SortedStrVec::clear() {
    m_offsets.risk_destroy(m_offsets_mem_type);
    m_strpool.risk_destroy(m_strpool_mem_type);
}

terark_flatten
size_t SortedStrVec::lower_bound_by_offset(size_t offset) const {
    return lower_bound_0<const UintVecMin0&>(m_offsets, m_offsets.size()-1, offset);
}

terark_flatten
size_t SortedStrVec::upper_bound_by_offset(size_t offset) const {
    return upper_bound_0<const UintVecMin0&>(m_offsets, m_offsets.size()-1, offset);
}

terark_flatten
size_t SortedStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    assert(m_offsets.size() >= 1);
    assert(lo < hi);
    assert(hi <= m_offsets.size()-1);
    const byte_t* odata = m_offsets.data();
    const size_t  obits = m_offsets.uintbits();
    const size_t  omask = m_offsets.uintmask();
    const byte_t* s = m_strpool.data();
#if !defined(NDEBUG)
    const byte_t kh = s[UintVecMin0::fast_get(odata, obits, omask, lo) + pos];
    assert(kh == ch);
    for (size_t i = lo; i < hi; ++i) {
        fstring s = (*this)[i];
        assert(pos < s.size());
    }
#endif
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        assert(pos < this->nth_size(mid));
        size_t offset = UintVecMin0::fast_get(odata, obits, omask, mid);
        if (s[offset + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

terark_flatten
size_t SortedStrVec::lower_bound(size_t start, size_t end, fstring key) const {
    assert(start <= end);
    assert(end <= m_offsets.size()-1);
//	return lower_bound_n<const SortedStrVec&>(*this, start, end, key);
    const auto pool = m_strpool.data();
	const auto data = m_offsets.data();
	const auto bits = m_offsets.uintbits();
	const auto mask = m_offsets.uintmask();
	size_t lo = start, hi = end;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_beg = UintVecMin0::fast_get(data, bits, mask, mid_idx);
		size_t mid_end = UintVecMin0::fast_get(data, bits, mask, mid_idx+1);
		size_t mid_len = mid_end - mid_beg;
		assert(mid_beg <= mid_end);
		if (str_less(pool + mid_beg, mid_len, key.p, key.n))
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
size_t SortedStrVec::upper_bound(size_t start, size_t end, fstring key) const {
    assert(start <= end);
    assert(end <= m_offsets.size()-1);
//  return upper_bound_n<const SortedStrVec&>(*this, start, end, key);
	const auto pool = m_strpool.data();
	const auto data = m_offsets.data();
	const auto bits = m_offsets.uintbits();
	const auto mask = m_offsets.uintmask();
	size_t lo = start, hi = end;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_beg = UintVecMin0::fast_get(data, bits, mask, mid_idx);
		size_t mid_end = UintVecMin0::fast_get(data, bits, mask, mid_idx+1);
		size_t mid_len = mid_end - mid_beg;
		assert(mid_beg <= mid_end);
		if (!str_less(key.p, key.n, pool + mid_beg, mid_len)) // mid <= key
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

size_t SortedStrVec::max_strlen() const {
    const auto data = m_offsets.data();
    const auto size = m_offsets.size();
    const auto bits = m_offsets.uintbits();
    const auto mask = m_offsets.uintmask();
    size_t maxlen = 0;
    for(size_t i = 1; i < size; ++i) {
        size_t s = m_offsets.fast_get(data, bits, mask, i-1);
        size_t t = m_offsets.fast_get(data, bits, mask, i-0);
        size_t x = t - s;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen;
}

/////////////////////////////////////////////////////////////////////////////

template<class UintXX>
SortedStrVecUintTpl<UintXX>::SortedStrVecUintTpl(size_t delim_len) {
	m_offsets.push_back(0);
	m_delim_len = delim_len;
	m_offsets_mem_type = MemType::Malloc;
	m_strpool_mem_type = MemType::Malloc;
}

template<class UintXX>
SortedStrVecUintTpl<UintXX>::~SortedStrVecUintTpl() {
	m_offsets.risk_destroy(m_offsets_mem_type);
	m_strpool.risk_destroy(m_strpool_mem_type);
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::reserve(size_t strNum, size_t maxStrPool) {
    m_strpool.reserve(maxStrPool + m_delim_len*strNum);
    m_offsets.reserve(strNum+1);
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::shrink_to_fit() {
    m_strpool.shrink_to_fit();
    m_offsets.shrink_to_fit();
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::swap(SortedStrVecUintTpl& y) {
    m_offsets.swap(y.m_offsets);
    m_strpool.swap(y.m_strpool);
	std::swap(m_delim_len, y.m_delim_len);
	std::swap(m_offsets_mem_type, y.m_offsets_mem_type);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::push_back(fstring str) {
    m_strpool.append(str.data(), str.size());
    m_offsets.push_back(m_strpool.size());
	m_offsets.push_n(m_delim_len, '\0');
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::pop_back() {
	size_t osize = m_offsets.size();
    assert(osize > 2);
    m_strpool.risk_set_size(m_offsets[osize-2]);
    m_offsets.risk_set_size(osize-1);
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::back_grow_no_init(size_t nGrow) {
    assert(m_offsets.size() >= 2);
	size_t slen = m_strpool.size() + nGrow;
    m_strpool.resize_no_init(slen);
    m_offsets.back() = slen;
}
template<class UintXX>
void SortedStrVecUintTpl<UintXX>::reverse_keys() {
    byte_t* beg = m_strpool.begin();
    size_t  offset = 0;
    for(size_t i = 0, n = m_offsets.size()-1; i < n; ++i) {
        size_t endpos = m_offsets[i+1];
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg + offset;
        byte_t* hi = beg + endpos - m_delim_len;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        offset = endpos;
    }
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::sort() {
    THROW_STD(invalid_argument, "This method is not supported");
}

template<class UintXX>
void SortedStrVecUintTpl<UintXX>::clear() {
    m_offsets.risk_destroy(m_offsets_mem_type);
    m_strpool.risk_destroy(m_strpool_mem_type);
}

template<class UintXX>
terark_flatten
size_t SortedStrVecUintTpl<UintXX>::lower_bound_by_offset(size_t offset) const {
    return lower_bound_0(m_offsets.begin(), m_offsets.size()-1, offset);
}

template<class UintXX>
terark_flatten
size_t SortedStrVecUintTpl<UintXX>::upper_bound_by_offset(size_t offset) const {
    return upper_bound_0(m_offsets.begin(), m_offsets.size()-1, offset);
}

template<class UintXX>
terark_flatten
size_t SortedStrVecUintTpl<UintXX>::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    TERARK_ASSERT_GE(m_offsets.size(), 2);
    TERARK_ASSERT_LE(lo, hi);
    TERARK_ASSERT_LE(hi, m_offsets.size()-1);
    const UintXX* data = m_offsets.data();
    const byte_t* pool = m_strpool.data();
#if !defined(NDEBUG)
    const byte_t kh = pool[data[lo] + pos];
    assert(kh == ch);
    for (size_t i = lo; i < hi; ++i) {
        fstring s = (*this)[i];
        TERARK_ASSERT_LT(pos, s.size());
    }
#endif
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        TERARK_ASSERT_LT(pos, this->nth_size(mid));
		StrVec_prefetch(true, data + (lo + mid)/2);
		StrVec_prefetch(true, data + (hi + mid)/2);
        size_t offset = data[mid];
        if (pool[offset + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

template<class UintXX>
terark_flatten
size_t SortedStrVecUintTpl<UintXX>::lower_bound(size_t start, size_t end, fstring key) const {
    TERARK_ASSERT_LE(start, end);
    TERARK_ASSERT_LE(end, m_offsets.size()-1);
    return lower_bound_n<const SortedStrVecUintTpl<UintXX>&>(*this, start, end, key);
}

template<class UintXX>
terark_flatten
size_t SortedStrVecUintTpl<UintXX>::upper_bound(size_t start, size_t end, fstring key) const {
    TERARK_ASSERT_LE(start, end);
    TERARK_ASSERT_LE(end, m_offsets.size()-1);
    return upper_bound_n<const SortedStrVecUintTpl<UintXX>&>(*this, start, end, key);
}

template<class UintXX>
size_t SortedStrVecUintTpl<UintXX>::max_strlen() const {
	const auto offsets = m_offsets.data();
    const auto size = m_offsets.size();
    size_t maxlen = 0;
    for(size_t i = 1; i < size; ++i) {
        size_t s = offsets[i-1];
        size_t t = offsets[i-0];
        size_t x = t - s;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen - m_delim_len;
}

template class SortedStrVecUintTpl<uint32_t>;
template class SortedStrVecUintTpl<uint64_t>;

/////////////////////////////////////////////////////////////////////////////

} // namespace terark

