#pragma once
#include <terark/stdtypes.hpp>
#include <terark/bitmap.hpp>
#include <terark/valvec.hpp>
#include <terark/util/throw.hpp>

#if defined(__BMI__) || defined(__BMI2__)
    #include <immintrin.h> // for _bextr_u64
#endif

namespace terark {

class OutputBuffer;

// memory layout is binary compatible to UintVecMin0 on 64 bit
class TERARK_DLL_EXPORT SortedUintVec {
	valvec<byte_t> m_data; // corresponding to UintVecMin0::m_data

	// these fields are corresponding to UintVecMin0::m_bits
	byte_t         m_log2_blockUnits;
	byte_t         m_offsetWidth;
	byte_t         m_sampleWidth;
	byte_t         m_is_sorted_uint_vec    : 1;
	byte_t         m_is_overall_full_sorted : 1;
	byte_t         m_is_samples_full_sorted : 1;
#if TERARK_WORD_BITS == 64
	uint32_t       m_padding;
#else
	#error "SortedUintVec does not support 32 bit platform"
#endif

	byte_t const*  m_index; // corresponding to UintVecMin0::m_mask
	size_t         m_size;  // corresponding to UintVecMin0::m_size

	struct ObjectHeader;
public:
	SortedUintVec();
	~SortedUintVec();

    SortedUintVec(SortedUintVec&&) noexcept;
    SortedUintVec& operator=(SortedUintVec&&) noexcept;

/*
	class UintVecMin0& as_UintVecMin0() {
		assert(0 == m_is_sorted_uint_vec);
		return reinterpret_cast<UintVecMin0&>(*this); // breaks gcc strict aliasing
	}
	const class UintVecMin0& as_UintVecMin0() const {
		assert(0 == m_is_sorted_uint_vec);
		return reinterpret_cast<const UintVecMin0&>(*this);
	}
*/
	bool isSortedUintVec() const { return 0 != m_is_sorted_uint_vec; }
	bool isUintVecMin0  () const { return 0 == m_is_sorted_uint_vec; }

	bool is_overall_full_sorted() const { return m_is_overall_full_sorted; }
	bool is_samples_full_sorted() const { return m_is_samples_full_sorted; }

	size_t offset_width() const { return m_offsetWidth; }
	size_t sample_width() const { return m_sampleWidth; }
	size_t log2_block_units() const { return m_log2_blockUnits; }
	size_t block_units() const { return size_t(1) << m_log2_blockUnits; }

	size_t num_blocks() const {
		assert(m_is_sorted_uint_vec);
		assert(m_size > 0);
		byte_t log2_bu = m_log2_blockUnits;
		size_t mask = (size_t(1) << log2_bu) - 1;
		return (m_size + mask) >> log2_bu;
	}
	const byte*data() const { return m_data.data(); }
	size_t mem_size() const { return m_data.size(); }
	size_t size() const { return m_size; }
	size_t get(size_t idx) const;
	size_t operator[](size_t idx) const { return get(idx); }
	void get2(size_t idx, size_t aVal[2]) const;
	void get_block(size_t blockIdx, size_t* aVal) const;

    const void* get_index_base() const { return m_index; }
    size_t get_index_width() const { return m_offsetWidth + m_sampleWidth; }
    size_t get_sample_width() const { return m_sampleWidth; }
    size_t get_block_min_val(size_t blockIdx) const {
        return s_get_block_min_val(m_index, m_sampleWidth, m_offsetWidth + m_sampleWidth, blockIdx);
    }
    static size_t
    s_get_block_min_val(const void* indexBase, size_t sampleWidth, size_t indexWidth, size_t blockIdx) {
        size_t bitpos = indexWidth * (blockIdx + 1) - sampleWidth;
#if defined(__amd64__) || defined(__amd64) || \
    defined(__x86_64__) || defined(__x86_64) || \
    defined(_M_X64) && 0
        uint64_t val = unaligned_load<uint64_t>((const byte_t*)(indexBase) + bitpos/8);
    #if defined(__BMI__) || defined(__BMI2__)
        size_t sample0 = _bextr_u64(val, bitpos % 8, uint32_t(sampleWidth));
    #else
        size_t sample0 = (val >> (bitpos%8)) & ~(size_t(-1) << sampleWidth);
    #endif
#else
        size_t sample0 = febitvec::s_get_uint((const size_t*)indexBase, bitpos, sampleWidth);
#endif
        return sample0;
    }

    size_t lower_bound(size_t lo, size_t hi, size_t key) const;
    size_t upper_bound(size_t lo, size_t hi, size_t key) const;
    std::pair<size_t, size_t>
           equal_range(size_t lo, size_t hi, size_t key) const;

	template<class UintVec>
	void build_from(const UintVec& vec, size_t blockUnits);

	void clear();
	void swap(SortedUintVec&);
	void risk_set_data(const void* base, size_t bytes);
	void risk_release_ownership();

	class TERARK_DLL_EXPORT Builder : boost::noncopyable {
	public:
        struct BuildResult {
            size_t size;        // SortedUintVec::size()
            size_t mem_size;    // SortedUintVec::mem_size()
        };
		class Impl;
		virtual ~Builder();
		virtual void push_back(uint64_t val) = 0;
		virtual BuildResult finish(SortedUintVec* result) = 0;
	};
	friend class Builder;
	static Builder* createBuilder(size_t blockUnits);
    static Builder* createBuilder(size_t blockUnits, const char* fname);
    static Builder* createBuilder(size_t blockUnits, OutputBuffer* buffer);
    static Builder* createBuilder(bool inputSorted, size_t blockUnits);
    static Builder* createBuilder(bool inputSorted, size_t blockUnits, const char* fname);
    static Builder* createBuilder(bool inputSorted, size_t blockUnits, OutputBuffer* buffer);
};
BOOST_STATIC_ASSERT(sizeof(SortedUintVec) == sizeof(size_t)*6);

template<class UintVec>
void SortedUintVec::build_from(const UintVec& src, size_t blockUnits) {
	assert(m_is_sorted_uint_vec);
	if (src.size() == 0) {
		THROW_STD(invalid_argument, "src can not be empty");
	}
    auto builder = std::unique_ptr<SortedUintVec::Builder>(
        SortedUintVec::createBuilder(false, blockUnits));
	for(size_t i = 0; i < src.size(); ++i) {
		size_t v = src[i];
		builder->push_back(v);
	}
	builder->finish(this);
#if !defined(NDEBUG)
	for(size_t i = 0; i < src.size()-1; ++i) {
		size_t s0 = src[i];
		size_t s1 = src[i+1];
		size_t zz[2];
		get2(i, zz);
		assert(zz[0] == s0);
		assert(zz[1] == s1);
	}
#endif
}

}

