#pragma once
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/util/throw.hpp>
#include <random>
#include <string>
#include <utility>
#include <vector>

#if defined(__GNUC__)
#include <stdint.h> // for uint16_t
#endif

namespace terark {

// 'Offset' could be a struct which contains offset as a field
template<class Offset>
struct default_offset_op {
	size_t get(const Offset& x) const { return x; }
	void   set(Offset& x, size_t y) const { x = static_cast<Offset>(y); }
	void   inc(Offset& x, ptrdiff_t d = 1) const {
		assert(d >= 0);
		x += Offset(d);
	}
	Offset make(size_t y) const { return Offset(y); }
	static const Offset maxpool = Offset(-1);
// msvc compile error:
//	BOOST_STATIC_ASSERT(Offset(0) < Offset(-1)); // must be unsigned
};

// just allow operations at back
//
template< class Char
		, class Offset = unsigned
		, class OffsetOp = default_offset_op<Offset>
		>
class basic_fstrvec : private OffsetOp {
	template<class> struct void_ { typedef void type; };
public:
	valvec<Char>   strpool;
	valvec<Offset> offsets;
	static const size_t maxpool = OffsetOp::maxpool;

	explicit basic_fstrvec(const OffsetOp& oop = OffsetOp())
	  : OffsetOp(oop) {
		offsets.push_back(OffsetOp::make(0));
	}

	void reserve(size_t capacity) {
		offsets.reserve(capacity+1);
	}
	void reserve_strpool(size_t capacity) {
		strpool.reserve(capacity);
	}

	void erase_all() {
		strpool.erase_all();
		offsets.resize_no_init(1);
		offsets[0] = OffsetOp::make(0);
	}

	// push_back an empty string
	// offten use with back_append
	void push_back() {
		offsets.push_back(OffsetOp::make(strpool.size()));
	}

#define basic_fstrvec_check_overflow(StrLen) \
    assert(strpool.size() + StrLen < maxpool); \
    if (maxpool <= UINT32_MAX) { \
        if (strpool.size() + StrLen > maxpool) { \
            THROW_STD(length_error \
                , "strpool.size() = %zd, StrLen = %zd" \
                , strpool.size(), size_t(StrLen)); \
        } \
    }

#define basic_fstrvec_check_fstring(fstr, ExtraLen) \
	basic_fstrvec_check_overflow(size_t(fstr.size() + ExtraLen))
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	template<class String>
	typename std::enable_if<!std::is_fundamental<String>::value>::type
	push_back(const String& str) {
		basic_fstrvec_check_overflow(str.size());
		strpool.append(str.begin(), str.end());
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void push_back(basic_fstring<Char> fstr) {
		basic_fstrvec_check_fstring(fstr, 0);
		strpool.append(fstr.data(), fstr.size());
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	template<class String>
	typename std::enable_if<!std::is_fundamental<String>::value>::type
	push_back(const String& str, Char lastChar) {
		basic_fstrvec_check_overflow(str.size()+1);
		strpool.append(str.data(), str.size());
		strpool.push_back(lastChar);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void push_back(basic_fstring<Char> fstr, Char lastChar) {
		basic_fstrvec_check_fstring(fstr, 1);
		strpool.append(fstr.data(), fstr.size());
		strpool.push_back(lastChar);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void emplace_back(const Char* str, size_t len) {
		basic_fstrvec_check_overflow(len);
		strpool.append(str, len);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	template<class ForwardIterator>
	void emplace_back(ForwardIterator first, ForwardIterator last) {
		size_t len = std::distance(first, last);
		basic_fstrvec_check_overflow(len);
		strpool.append(first, len);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}

	void back_append(basic_fstring<Char> fstr) {
		basic_fstrvec_check_fstring(fstr, 0);
		assert(offsets.size() >= 2);
		strpool.append(fstr.data(), fstr.size());
		OffsetOp::inc(offsets.back(), fstr.size());
	}
	template<class ForwardIterator>
	void back_append(ForwardIterator first, ForwardIterator last) {
		assert(offsets.size() >= 2);
		size_t len = std::distance(first, last);
		basic_fstrvec_check_overflow(len);
		strpool.append(first, len);
		OffsetOp::inc(offsets.back(), last - first);
	}
	template<class String>
	typename std::enable_if<!std::is_fundamental<String>::value>::type
	back_append(const String& str) {
		assert(offsets.size() >= 2);
		basic_fstrvec_check_overflow(str.size());
		strpool.append(str.data(), str.size());
		OffsetOp::inc(offsets.back(), str.size());
	}
	void back_append(const Char* str, size_t len) {
		basic_fstrvec_check_overflow(len);
		assert(offsets.size() >= 2);
		strpool.append(str, len);
		OffsetOp::inc(offsets.back(), len);
	}
	void back_append(Char ch) {
		assert(offsets.size() >= 2);
		basic_fstrvec_check_overflow(1);
		strpool.push_back(ch);
		OffsetOp::inc(offsets.back(), 1);
	}

	void pop_back() {
		assert(offsets.size() >= 2);
		offsets.pop_back();
		strpool.resize(OffsetOp::get(offsets.back()));
	}

	void resize(size_t n) {
		assert(n < offsets.size() && "basic_fstrvec::resize just allow shrink");
		if (n >= offsets.size()) {
			throw std::logic_error("basic_fstrvec::resize just allow shrink");
		}
		offsets.resize(n+1);
		strpool.resize(OffsetOp::get(offsets.back()));
	}

	basic_fstring<Char> front() const {
		assert(offsets.size() >= 2);
		return (*this)[0];
	}
	basic_fstring<Char> back() const {
		assert(offsets.size() >= 2);
		return (*this)[offsets.size()-2];
	}

	size_t used_mem_size() const { return offsets.used_mem_size() + strpool.used_mem_size(); }
	size_t full_mem_size() const { return offsets.full_mem_size() + strpool.full_mem_size(); }
	size_t free_mem_size() const { return offsets.free_mem_size() + strpool.free_mem_size(); }

	size_t size() const { return offsets.size() - 1; }
	bool  empty() const { return offsets.size() < 2; }

	basic_fstring<Char> operator[](size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx+0]);
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 <= off1);
		return basic_fstring<Char>(base + off0, base + off1);
	}
	basic_fstring<Char> at(size_t idx) const {
		if (idx >= offsets.size()-1) {
			throw std::out_of_range("basic_fstrvec: at");
		}
		return (*this)[idx];
	}
	std::basic_string<Char> str(size_t idx) const {
		assert(idx < offsets.size()-1);
		basic_fstring<Char> x = (*this)[idx];
		return std::basic_string<Char>(x.data(), x.size());
	}

	size_t slen(size_t idx) const {
		assert(idx < offsets.size()-1);
		size_t off0 = OffsetOp::get(offsets[idx+0]);
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 <= off1);
		return off1 - off0;
	}
	int ilen(size_t idx) const { return (int)slen(idx); }

	Char* beg_of(size_t idx) {
		assert(idx < offsets.size()-1);
		Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
		assert(off0 <= strpool.size());
		return base + off0;
	}
	const Char* beg_of(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
		assert(off0 <= strpool.size());
		return base + off0;
	}
	Char* end_of(size_t idx) {
		assert(idx < offsets.size()-1);
		Char* base = strpool.data();
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off1 <= strpool.size());
		return base + off1;
	}
	const Char* end_of(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off1 <= strpool.size());
		return base + off1;
	}

	const Char* c_str(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
	#if !defined(NDEBUG)
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 < off1);
		assert(off1 <= strpool.size());
	//	assert(off1 >= 1); // off0 < off1 implies this assert
		assert('\0' == base[off1-1]);
	#endif
		return base + off0;
	}
	void shrink_to_fit() {
		strpool.shrink_to_fit();
		offsets.shrink_to_fit();
	}

	void swap(basic_fstrvec& y) {
		std::swap(static_cast<OffsetOp&>(*this), static_cast<OffsetOp&>(y));
		strpool.swap(y.strpool);
		offsets.swap(y.offsets);
	}

	void to_stdstrvec(std::vector<std::basic_string<Char> >* stdstrvec) const {
		assert(offsets.size() >= 1);
		stdstrvec->resize(offsets.size()-1);
		const Char* base = strpool.data();
		for(size_t i = 0; i < offsets.size()-1; ++i) {
			size_t off0 = OffsetOp::get(offsets[i+0]);
			size_t off1 = OffsetOp::get(offsets[i+1]);
			assert(off0 <= off1);
			assert(off1 <= strpool.size());
			(*stdstrvec)[i].assign(base + off0, base + off1);
		}
	}

    void shuffle(basic_fstrvec* result,
                 size_t seed = std::mt19937_64::default_seed) const {
        reorder(result, [seed](Offset* index_beg, size_t num) {
            std::shuffle(index_beg, index_beg + num, std::mt19937_64(seed));
        });
    }
    template<class OrderGen>
    void reorder(basic_fstrvec* result, OrderGen og) const {
        if (offsets.size() < 1) {
            return;
        }
        size_t n_item = offsets.size()-1;
        result->offsets.resize_no_init(n_item+1);
        Offset* dst_item = result->offsets.data();
        for (size_t i = 0; i < n_item; ++i)
            dst_item[i] = OffsetOp::make(i); // used for shuffle
        og(dst_item, n_item);
        result->strpool.resize_no_init(strpool.size());
        Char*   dst_pool = result->strpool.data();
        const Char*   src_pool = this->strpool.data();
        const Offset* src_item = this->offsets.data();
        size_t  offset = 0;
        for(size_t i = 0; i < n_item; ++i) {
            size_t r = OffsetOp::get(dst_item[i]);
            size_t off0 = OffsetOp::get(src_item[r+0]);
            size_t off1 = OffsetOp::get(src_item[r+1]);
            size_t len0 = off1 - off0;
            memcpy(dst_pool + offset, src_pool + off0, sizeof(Char)*len0);
            dst_item[i] = OffsetOp::make(offset);
            offset += len0;
        }
        dst_item[n_item] = OffsetOp::make(offset);
    }
    void shuffle(size_t seed = std::mt19937_64::default_seed) {
        basic_fstrvec tmp;
        shuffle(&tmp, seed);
        this->swap(tmp);
    }
    void sort(basic_fstrvec* result) {
        reorder(result, [this](Offset* index_beg, size_t num) {
            auto p_offsets = offsets.data();
            auto p_strpool = strpool.data();
            auto cmp = [p_offsets, p_strpool](Offset x, Offset y) {
			    size_t xoff0 = OffsetOp::get(p_offsets[x+0]);
			    size_t xoff1 = OffsetOp::get(p_offsets[x+1]);
			    size_t yoff0 = OffsetOp::get(p_offsets[y+0]);
			    size_t yoff1 = OffsetOp::get(p_offsets[y+1]);
                fstring xs(p_strpool + xoff0, xoff1 - xoff0);
                fstring ys(p_strpool + yoff0, yoff1 - yoff0);
                return xs < ys;
            };
            std::sort(index_beg, index_beg + num, cmp);
        });
    }
    void sort() {
        basic_fstrvec tmp;
        sort(&tmp);
        this->swap(tmp);
    }
	std::vector<std::basic_string<Char> > to_stdstrvec() const {
		std::vector<std::basic_string<Char> > res;
		to_stdstrvec(&res);
		return res;
	}

	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const basic_fstrvec& x) {
		dio << x.strpool;
		dio << x.offsets;
	}
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, basic_fstrvec& x) {
		dio >> x.strpool;
		dio >> x.offsets;
	}
};

typedef basic_fstrvec<char, unsigned int > fstrvec;
typedef basic_fstrvec<char, unsigned long> fstrvecl;
typedef basic_fstrvec<char, unsigned long long> fstrvecll;

typedef basic_fstrvec<wchar_t, unsigned int > wfstrvec;
typedef basic_fstrvec<wchar_t, unsigned long> wfstrvecl;
typedef basic_fstrvec<wchar_t, unsigned long long> wfstrvecll;

typedef basic_fstrvec<uint16_t, unsigned int > fstrvec16;
typedef basic_fstrvec<uint16_t, unsigned long> fstrvec16l;
typedef basic_fstrvec<uint16_t, unsigned long long> fstrvec16ll;

} // namespace terark

namespace std {
	template<class Char, class Offset>
	void swap(terark::basic_fstrvec<Char, Offset>& x,
			  terark::basic_fstrvec<Char, Offset>& y)
   	{ x.swap(y); }
}

