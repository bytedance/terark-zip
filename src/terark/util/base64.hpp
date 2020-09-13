#pragma once
#include "libbase64.h"
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>

namespace terark {

static const int BASE64_FLAG =
	//0;
	//BASE64_FORCE_AVX2;
	//BASE64_FORCE_SSE42;
	BASE64_FORCE_PLAIN;

template<class ByteType, class ByteType2>
typename boost::enable_if_c<sizeof(ByteType)==1&&sizeof(ByteType2)==1, void>::type
base64_encode(const ByteType2* src, size_t srclen, valvec<ByteType>* out) {
	size_t outlen = srclen * 4 / 3 + 32;
	out->resize_no_init(outlen);
	trk_base64_encode(reinterpret_cast<const char*>(src), srclen,
			reinterpret_cast<char*>(out->data()), &outlen, BASE64_FLAG);
	assert(outlen <= out->size());
	out->risk_set_size(outlen);
	out->grow_capacity(1)[0] = '\0';
}

template<class ByteType>
typename boost::enable_if_c<sizeof(ByteType)==1, void>::type
base64_encode(fstring src, valvec<ByteType>* out) {
	base64_encode(src.data(), src.size(), out);
}

valvec<char> base64_encode(fstring src) {
    valvec<char> out;
    base64_encode(src.data(), src.size(), &out);
    return out;
}

template<class ByteType, class ByteType2>
typename boost::enable_if_c<sizeof(ByteType)==1&&sizeof(ByteType2)==1, void>::type
base64_decode(const ByteType2* src, size_t srclen, valvec<ByteType>* out) {
	size_t outlen = srclen * 3 / 4 + 32;
	out->resize_no_init(outlen);
	trk_base64_decode(reinterpret_cast<const char*>(src), srclen,
			reinterpret_cast<char*>(out->data()), &outlen, BASE64_FLAG);
	assert(outlen <= out->size());
	out->risk_set_size(outlen);
	out->grow_capacity(1)[0] = '\0';
}

template<class ByteType>
typename boost::enable_if_c<sizeof(ByteType)==1, void>::type
base64_decode(fstring src, valvec<ByteType>* out) {
	base64_decode(src.data(), src.size(), out);
}


} // namespace terark

