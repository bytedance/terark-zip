#pragma once
#ifndef __terark_db_byte_lex_code_hpp__
#define __terark_db_byte_lex_code_hpp__

#include <terark/pass_by_value.hpp>
#include <boost/type_traits/is_signed.hpp>
#include <boost/core/enable_if.hpp>
#include <boost/static_assert.hpp>

namespace terark {

// DataIO should be LittleEndianInput/LittleEndianOutput

template<class Int>
class ByteLexCodeInput {
	Int* t;
public:
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, ByteLexCodeInput& x) {
		dio >> *x.t;
		*x.t ^= Int(1) << (sizeof(Int)*8 - 1);
	}
	explicit ByteLexCodeInput(Int& x) : t(&x) {}
};
template<>
class ByteLexCodeInput<float> {
	float* t;
public:
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, ByteLexCodeInput& x) {
		BOOST_STATIC_ASSERT(sizeof(float) == 4);
		BOOST_STATIC_ASSERT(sizeof(int) == 4);
		int i; dio >> i;
		i ^= 1 << 31;
		memcpy(x.t, &i, 4);
	}
	explicit ByteLexCodeInput(float& x) : t(&x) {}
};
template<>
class ByteLexCodeInput<double> {
	double* t;
public:
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, ByteLexCodeInput& x) {
		BOOST_STATIC_ASSERT(sizeof(double) == 8);
		BOOST_STATIC_ASSERT(sizeof(long long) == 8);
		long long i; dio >> i;
		i ^= 1 << 63;
		memcpy(x.t, &i, 8);
	}
	explicit ByteLexCodeInput(double& x) : t(&x) {}
};

template<class Int>
class ByteLexCodeOutput {
	const Int t;
public:
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const ByteLexCodeOutput& x) {
		Int v = x.t ^ (Int(1) << (sizeof(Int)*8 - 1));
		dio << v;
	}
	explicit ByteLexCodeOutput(Int x) : t(x) {}
};
template<>
class ByteLexCodeOutput<float> {
	const float t;
public:
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const ByteLexCodeOutput& x) {
		BOOST_STATIC_ASSERT(sizeof(float) == 4);
		BOOST_STATIC_ASSERT(sizeof(int) == 4);
		int i = *reinterpret_cast<const int*>(&x.t);
		int j = i ^ (1 << 31);
		dio << j;
	}
	explicit ByteLexCodeOutput(float x) : t(x) {}
};
template<>
class ByteLexCodeOutput<double> {
	const double t;
public:
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, ByteLexCodeOutput& x) {
		BOOST_STATIC_ASSERT(sizeof(double) == 8);
		BOOST_STATIC_ASSERT(sizeof(long long) == 8);
		long long i = *reinterpret_cast<const int*>(&x.t);
		long long j = i ^ (1 << 63);
		dio << j;
	}
	explicit ByteLexCodeOutput(double x) : t(x) {}
};

template<class Int>
typename
boost::enable_if<boost::is_signed<Int>,
				 pass_by_value<ByteLexCodeInput<Int> >
				>::type
ByteLexCode(Int& x) { return ByteLexCodeInput<Int>(x); }

template<class Int>
typename boost::disable_if<boost::is_signed<Int>, Int>::type&
ByteLexCode(Int& x) { return x; }

pass_by_value<ByteLexCodeInput<float> >
ByteLexCode(float& x) { return ByteLexCodeInput<float>(x); }

pass_by_value<ByteLexCodeInput<double> >
ByteLexCode(double& x) { return ByteLexCodeInput<double>(x); }

template<class Int>
typename
boost::enable_if<boost::is_signed<Int>,
				 pass_by_value<ByteLexCodeInput<Int> >
				>::type
ByteLexCode(const Int& x) { return ByteLexCodeOutput<Int>(x); }

template<class Int>
const typename boost::disable_if<boost::is_signed<Int>, Int>::type&
ByteLexCode(const Int& x) { return x; }

ByteLexCodeOutput<float>
ByteLexCode(const float& x) { return ByteLexCodeOutput<float>(x); }

ByteLexCodeOutput<double>
ByteLexCode(const double& x) { return ByteLexCodeOutput<double>(x); }

} // namespace terark

#endif // __terark_db_byte_lex_code_hpp__
