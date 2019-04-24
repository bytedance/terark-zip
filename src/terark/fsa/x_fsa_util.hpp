#pragma once
#include <terark/config.hpp>
#include <stddef.h>

#if defined(__GNUC__)
	#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 7
	#elif defined(__clang__)
	#elif defined(__INTEL_COMPILER)
	#else
		#error "Requires GCC-4.7+"
	#endif
#endif

#ifdef _MSC_VER
	#define strcasecmp _stricmp
#endif

namespace terark {

template<class Uint>
terark_warn_unused_result
inline Uint align_to_64(Uint x) { return (x + 63) & size_t(-64); }

struct CompareBy_pos {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.pos < y.pos;
	}
	template<class T>
	bool operator()(const T& x, size_t y) const { return x.pos < y; }
	template<class T>
	bool operator()(size_t x, const T& y) const { return x < y.pos; }
};

struct CharTarget_By_ch {
	template<class CT>
	bool operator()(const CT& x, const CT& y) const { return x.ch < y.ch; }
	template<class CT>
    unsigned short
    operator()(const CT& x) const { return (unsigned short)(x.ch); }
};

struct IdentityTR {
	unsigned char operator()(unsigned char c) const { return c; }
};
struct TableTranslator {
	const unsigned char* tr_tab;
	unsigned char operator()(unsigned char c) const { return tr_tab[c]; }
	TableTranslator(const unsigned char* tr_tab1) : tr_tab(tr_tab1) {}
};

}
