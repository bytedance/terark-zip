#ifndef __terark_util_linebuf_hpp__
#define __terark_util_linebuf_hpp__

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h> // strchr
#include <boost/noncopyable.hpp>
#include <terark/config.hpp>
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>
#include <terark/bitmap.hpp>

namespace terark {

// layout compatible to fstring & valvec
struct TERARK_DLL_EXPORT LineBuf : boost::noncopyable {
	char*  p;
	size_t n;
	size_t capacity;

	typedef char* iterator;
	typedef char* const_iterator;

	LineBuf();
	~LineBuf();

	ptrdiff_t getline(FILE* f); ///<  is thread safe on glibc
	ptrdiff_t getbson(FILE* f); ///< not thread safe

	bool  empty() const { return 0 == n; }
	size_t size() const { return n; }
	char*  data() const { return p; }
	char* begin() const { return p; }
	char*   end() const { return p + n; }

	void swap(LineBuf& y) {
		std::swap(capacity, y.capacity);
		std::swap(n, y.n);
		std::swap(p, y.p);
	}

	void clear();
	void erase_all() { n = 0; }

	void risk_release_ownership() { p = NULL; n = 0; capacity = 0; }
	void risk_swap_valvec(valvec<char>& vv) {
		char*  p1 = p; p = vv.data(); vv.risk_set_data(p1);
		size_t n1 = n; n = vv.size(); vv.risk_set_size(n1);
		size_t c1 = capacity; capacity = vv.capacity(); vv.risk_set_capacity(c1);
	}
	void risk_swap_valvec(valvec<byte_t>& vv) {
		auto   p1 = (byte_t*)p; p = (char*)vv.data(); vv.risk_set_data(p1);
		size_t n1 = n; n = vv.size(); vv.risk_set_size(n1);
		size_t c1 = capacity; capacity = vv.capacity(); vv.risk_set_capacity(c1);
	}

	///@{
	///@return removed bytes
	size_t trim();  // remove all trailing spaces, including '\r' and '\n'
	size_t chomp(); // remove all trailing '\r' and '\n', just as chomp in perl
	///@}

	void push_back(char ch) {
		if (n + 1 < capacity) {
			p[n++] = ch;
			p[n] = '\0';
		}
		else {
			push_back_slow_path(ch);
		}
	}
private:
	void push_back_slow_path(char ch);
public:

	operator char*() const { return p; }
	operator fstring() const { return fstring(p, n); }

	/// split into fields
	template<class Vec>
	size_t split(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		F->resize(0);
		return split_append(delims, F, max_fields);
	}
	template<class Vec>
	size_t split_append(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		return split_f(delims, [F](char* col, char* next) {
			F->push_back(typename Vec::value_type(col, next));
		}, max_fields);
	}
	template<class PushFunc>
	size_t split_f(fstring delims, PushFunc push, size_t max_fields = ~size_t(0)) {
		size_t dlen = delims.size();
		if (0 == dlen) // empty delims redirect to blank delim
			return split_f<PushFunc>(' ', push, max_fields);
		if (1 == dlen)
			return split_f<PushFunc>(delims[0], push, max_fields);
		size_t fields = 0;
		char *col = p, *end = p + n;
		while (col < end && fields+1 < max_fields) {
			char* next = (char*)memmem(col, end-col, delims.data(), dlen);
			if (NULL == next) next = end;
			push(col, next), fields++;
			*next = 0;
			col = next + dlen;
		}
		if (col <= end)
			push(col, end), fields++;
		return fields;
	}

	template<class Vec>
	size_t split_by_any(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		F->resize(0);
		return split_by_any_append(delims, F, max_fields);
	}
	template<class Vec>
	size_t split_by_any_append(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		return split_by_any_f(delims, [F](char* col, char* next) {
			F->push_back(typename Vec::value_type(col, next));
		}, max_fields);
	}
	template<class PushFunc>
	size_t split_by_any_f(fstring delims, PushFunc push, size_t max_fields = ~size_t(0)) {
		size_t dlen = delims.size();
		if (0 == dlen) // empty delims redirect to blank delim
			return split_f<PushFunc>(' ', push, max_fields);
		if (1 == dlen)
			return split_f<PushFunc>(delims[0], push, max_fields);
		static_bitmap<256> bits;
		size_t fields = 0;
		for (size_t i = 0; i < delims.size(); ++i) bits.set1(delims[i]);
		char *col = p, *end = p + n;
		while (col < end && fields+1 < max_fields) {
			char* next = col;
			while (next < end && bits.is0((byte_t)*next)) ++next;
			push(col, next), fields++;
			*next = 0;
			col = next + 1;
		}
		if (col <= end)
			push(col, end), fields++;
		return fields;
	}
	template<class Vec>
	size_t split(const char delim, Vec* F, size_t max_fields = ~size_t(0)) {
		F->resize(0);
		return split_append(delim, F, max_fields);
	}
	template<class Vec>
	size_t split_append(const char delim, Vec* F, size_t max_fields = ~size_t(0)) {
		return split_f(delim, [F](char* col, char* next) {
			F->push_back(typename Vec::value_type(col, next));
		}, max_fields);
	}
	template<class PushFunc>
	size_t split_f(const char delim, PushFunc push, size_t max_fields = ~size_t(0)) {
		size_t fields = 0;
		if (' ' == delim) {
		   	// same as awk, skip first blank field, and skip dup blanks
			char *col = p, *end = p + n;
			while (col < end && isspace(*col)) ++col; // skip first blank field
			while (col < end && fields+1 < max_fields) {
				char* next = col;
				while (next < end && !isspace(*next)) ++next;
				push(col, next), fields++;
				while (next < end &&  isspace(*next)) *next++ = 0; // skip blanks
				col = next;
			}
			if (col < end)
				push(col, end), fields++;
		}
		else {
			char *col = p, *end = p + n;
			while (col < end && fields+1 < max_fields) {
				char* next = col;
				while (next < end && delim != *next) ++next;
				push(col, next), fields++;
				*next = 0;
				col = next + 1;
			}
			if (col <= end)
				push(col, end), fields++;
		}
		return fields;
	}

	/// @params[out] offsets: length of offsets must >= arity+1
	///  on return, offsets[arity] saves tuple data length
	bool read_binary_tuple(int32_t* offsets, size_t arity, FILE* f);

	LineBuf& read_all(FILE*, size_t align = 0);
	LineBuf& read_all(fstring fname, size_t align = 0);
};

} // namespace terark

#endif // __terark_util_linebuf_hpp__

