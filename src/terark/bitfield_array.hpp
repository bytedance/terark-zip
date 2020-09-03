#ifndef __terark_bitfield_array_hpp__
#define __terark_bitfield_array_hpp__

#include <assert.h>
#include <terark/stdtypes.hpp>
#include <terark/valvec.hpp>
#include <boost/preprocessor/cat.hpp>

namespace terark {

namespace detail {

template<int... ArgList> struct GetAllSum;
template<int Arg0, int... ArgList>
struct GetAllSum<Arg0, ArgList...> {
	enum { value = Arg0 + GetAllSum<ArgList...>::value };
};
template<> struct GetAllSum<> { enum { value = 0 }; };

template<int Arg0, int... ArgList>
struct LastArgValue {
	enum { value = LastArgValue<ArgList...>::value };
};
template<int Arg0>
struct LastArgValue<Arg0> { enum { value = Arg0 }; };

template<int Nth, int Arg0, int... ArgList>
struct PrefixSum {
	enum { value = Arg0 + PrefixSum<Nth-1, ArgList...>::value };
};
template<int Arg0, int... ArgList>
struct PrefixSum<0, Arg0, ArgList...> { enum { value = 0 }; };

template<bool Test, class Then, class Else> struct IF;
template<class Then, class Else> struct IF<1,Then,Else> {typedef Then type;};
template<class Then, class Else> struct IF<0,Then,Else> {typedef Else type;};

template<int Nth, int Arg0, int... ArgList>
struct GetNthArg {
	enum { value = GetNthArg<Nth-1, ArgList...>::value };
};
template<int Arg0, int... ArgList>
struct GetNthArg<0, Arg0, ArgList...> { enum { value = Arg0 }; };

template<int Arg0, int... ArgList>
struct MaxValue {
	enum { temp = MaxValue<ArgList...>::value };
	enum { value = Arg0 > temp ? Arg0 : temp };
};
template<int Arg0>
struct MaxValue<Arg0> { enum { value = Arg0 }; };

template<int Arg0, int... ArgList>
struct MinValue {
	enum { temp = MinValue<ArgList...>::value };
	enum { value = Arg0 < temp ? Arg0 : temp };
};
template<int Arg0>
struct MinValue<Arg0> { enum { value = Arg0 }; };

template<int Bits>
struct AllOne {
	static_assert(Bits >=  1, "Bits must >= 1");
	static_assert(Bits <= 64, "Bits must <= 64");
	typedef typename IF<(Bits<=8)
	  , unsigned char
	  ,	typename IF<(Bits<32), uint32_t, uint64_t>::type
	>::type type;
   	static const type value = (type(1) << Bits) - 1;
};
template<>struct AllOne<32> {
	typedef uint32_t type;
	static const type value = type(-1);
};
template<>struct AllOne<64> {
	typedef uint64_t type;
	static const type value = type(-1);
};

#if 1 && ( \
	defined(__i386__) || defined(__i386) || defined(_M_IX86) || \
	defined(__X86__) || defined(_X86_) || \
	defined(__THW_INTEL__) || defined(__I86__) || \
	defined(__amd64__) || defined(__amd64) || defined(__x86_64__) || defined(__x86_64) || defined(_M_X64) \
	)
  #define BITFIELD_ARRAY_FAST_MISALIGN
#endif

template<int... BitFieldsBits>
class bitfield_array {
	static_assert(sizeof...(BitFieldsBits) > 0, "BitFieldsBits... Must Have At Least One BitField");
	static_assert(MaxValue<BitFieldsBits...>::value <= 64, "BitField bits must <= 64");
	static_assert(MinValue<BitFieldsBits...>::value >=  1, "BitField bits must >= 1");
	template<int NthField>
	struct BestUint {
		typedef typename
		AllOne<GetNthArg<NthField, BitFieldsBits...>::value>::type type;
	};
	valvec<unsigned char> m_bytes;
	size_t m_size;

public:
	enum { TotalBits = GetAllSum<BitFieldsBits...>::value };
	bitfield_array() : m_size(0) {}
	explicit bitfield_array(size_t num_tuples) : m_size(num_tuples) {
		m_bytes.resize(compute_mem_size(num_tuples));
	}

	const unsigned char* data() const { return m_bytes.data(); }
	size_t size() const { return m_size; }

	// align memory size to 16 bytes
	static size_t compute_mem_size(size_t num_tuples) {
		size_t  required = (num_tuples * TotalBits + 64 + 63) / 64 * 8;
		return (required + 63) & ~size_t(63); // align to 64
	}

	size_t mem_size() const { return m_bytes.size(); }

	void resize(size_t newsize) {
		m_bytes.resize(compute_mem_size(newsize));
		m_size = newsize;
	}
	void resize_no_init(size_t newsize) {
		m_bytes.resize_no_init(compute_mem_size(newsize));
		m_size = newsize;
	}

	void risk_release_ownership() {
		m_bytes.risk_release_ownership();
		m_size = 0;
	}
	void risk_set_data(const void* vdata, size_t num_tuples) {
		byte_t* data = (byte_t*)vdata;
		m_bytes.risk_set_data(data, compute_mem_size(num_tuples));
		m_size = num_tuples;
	}

	void swap(bitfield_array& y) {
		m_bytes.swap(y.m_bytes);
		std::swap(m_size, y.m_size);
	}

	void clear() {
		m_bytes.clear();
		m_size = 0;
	}

	template<class... ArgList>
	void emplace_back(const ArgList... args) {
		static_assert(sizeof...(ArgList) == sizeof...(BitFieldsBits)
				, "ArgList Length Must Match");
		size_t idx = m_size;
		resize(m_size + 1);
		pset<0, ArgList...>(idx, args...);
	}

	template<class UintType>
	typename std::enable_if
		< sizeof...(BitFieldsBits)==1 && std::is_integral<UintType>::value
		, void
		>::type
	push_back(UintType val) {
		size_t idx = m_size;
		resize(m_size + 1);
		set0(idx, val);
	}

	template<class... Uints>
	void aset(size_t idx, Uints... fields) {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		aligned_pset<0>(m_bytes.data(), idx, fields...);
#else
		unaligned_pset<0>(m_bytes.data(), idx, fields...);
#endif
	}
	template<int FirstField, class... Uints>
	void pset(size_t idx, Uints... fields) {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		aligned_pset<FirstField>(m_bytes.data(), idx, fields...);
#else
		unaligned_pset<FirstField>(m_bytes.data(), idx, fields...);
#endif
	}

	template<int NthField>
	typename BestUint<NthField>::type get(size_t idx) const {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		return aligned_get<NthField>(m_bytes.data(), idx);
#else
		return unaligned_get<NthField>(m_bytes.data(), idx);
#endif
	}

	template<int NthField>
	void set(size_t idx, typename BestUint<NthField>::type val) {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		aligned_set<NthField>(m_bytes.data(), idx, val);
#else
		unaligned_set<NthField>(m_bytes.data(), idx, val);
#endif
	}

	typename BestUint<0>::type get0(size_t idx) const {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		return aligned_get<0>(m_bytes.data(), idx);
#else
		return unaligned_get<0>(m_bytes.data(), idx);
#endif
	}

	template<class IndexType>
	typename std::enable_if
		< sizeof...(BitFieldsBits)==1 && std::is_integral<IndexType>::value
		, typename BestUint<0>::type
		>::type
	operator[](IndexType idx) const {
		assert(size_t(idx) < m_size);
		return get0(idx);
	}

	void set0(size_t idx, typename BestUint<0>::type val) {
		assert(idx < m_size);
#if defined(BITFIELD_ARRAY_FAST_MISALIGN)
		aligned_set<0>(m_bytes.data(), idx, val);
#else
		unaligned_set<0>(m_bytes.data(), idx, val);
#endif
	}

#define aligned_load unaligned_load
#define aligned_save unaligned_save
#define get_func     unaligned_get
#define set_func     unaligned_set
#define pget_func    unaligned_pget
#define pset_func    unaligned_pset
#define aset_func    unaligned_aset
#include "bitfield_array_access.hpp"
#undef aligned_load
#undef aligned_save
#undef get_func
#undef set_func
#undef pget_func
#undef pset_func
#undef aset_func

#define get_func     aligned_get
#define set_func     aligned_set
#define pget_func    aligned_pget
#define pset_func    aligned_pset
#define aset_func    aligned_aset
#include "bitfield_array_access.hpp"
#undef get_func
#undef set_func
#undef pget_func
#undef pset_func
#undef aset_func

};

} // namespace detail
using detail::bitfield_array;

} // namespace terark

using terark::detail::bitfield_array;

#endif // __terark_bitfield_array_hpp__

