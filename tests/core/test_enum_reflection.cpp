#include <terark/util/enum.hpp>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

TERARK_ENUM_PLAIN(EPlain, Value1, Value2 = 20, Value3 = 30)
TERARK_ENUM_CLASS(EClass, Value1, Value2 = 20, Value3 = 30)

TERARK_ENUM_PLAIN_NS(ns1, EPlain, int, Value1, Value2 = 20, Value3 = 30)
TERARK_ENUM_CLASS_NS(ns2, EClass, int, Value1, Value2 = 20, Value3 = 30)

int main() {
    printf("EPlain: num_of_EPlain(Value2) = %zd\n", EnumReflection<EPlain>::num());
    printf("EPlain: name_of_EPlain(Value2) = %.*s\n", DOT_STAR_S(EnumReflection<EPlain>::name(Value2)));
    printf("EPlain: name_of_EPlain(Value3) = %.*s\n", DOT_STAR_S(EnumReflection<EPlain>::name(Value3)));

    printf("EPlain: value_of_EPlain('Value1') = %d\n", EnumReflection<EPlain>::value("Value1"));
    printf("EPlain: value_of_EPlain('Value2') = %d\n", EnumReflection<EPlain>::value("Value2"));
    printf("EPlain: value_of_EPlain('Value3') = %d\n", EnumReflection<EPlain>::value("Value3"));

    try {
        printf("EPlain: value_of_EPlain('Unknown') = %d\n", EnumReflection<EPlain>::value("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("EPlain: catched: %s\n", ex.what());
    }

    printf("EClass: num_of_EClass(Value2) = %zd\n", EnumReflection<EClass>::num());
    printf("EClass: name_of_EClass(Value2) = %.*s\n", DOT_STAR_S(EnumReflection<EClass>::name(EClass::Value2)));
    printf("EClass: name_of_EClass(Value3) = %.*s\n", DOT_STAR_S(EnumReflection<EClass>::name(EClass::Value3)));

    printf("EClass: value_of_EClass('Value1') = %d\n", EnumReflection<EClass>::value("Value1"));
    printf("EClass: value_of_EClass('Value2') = %d\n", EnumReflection<EClass>::value("Value2"));
    printf("EClass: value_of_EClass('Value3') = %d\n", EnumReflection<EClass>::value("Value3"));

    try {
        printf("EClass: value_of_EClass('Unknown') = %d\n", EnumReflection<EClass>::value("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("EClass: catched: %s\n", ex.what());
    }

    printf("str_all_name_of_enum<EClass>() = %s\n", str_all_name_of_enum<EClass>().c_str());
    printf("str_all_of_enum<EClass>() = %s\n", str_all_of_enum<EClass>().c_str());

    printf("ns1::EPlain: num_of_ns1::EPlain(Value2) = %zd\n", EnumReflection<ns1::EPlain>::num());
    printf("ns1::EPlain: name_of_ns1::EPlain(Value2) = %.*s\n", DOT_STAR_S(EnumReflection<ns1::EPlain>::name(ns1::Value2)));
    printf("ns1::EPlain: name_of_ns1::EPlain(Value3) = %.*s\n", DOT_STAR_S(EnumReflection<ns1::EPlain>::name(ns1::Value3)));

    printf("ns1::EPlain: value_of_ns1::EPlain('Value1') = %d\n", EnumReflection<ns1::EPlain>::value("Value1"));
    printf("ns1::EPlain: value_of_ns1::EPlain('Value2') = %d\n", EnumReflection<ns1::EPlain>::value("Value2"));
    printf("ns1::EPlain: value_of_ns1::EPlain('Value3') = %d\n", EnumReflection<ns1::EPlain>::value("Value3"));

    try {
        printf("ns1::EPlain: value_of_ns1::EPlain('Unknown') = %d\n", EnumReflection<ns1::EPlain>::value("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("ns1::EPlain: catched: %s\n", ex.what());
    }


    printf("str_all_name_of_enum<EClass>() = %s\n", str_all_name_of_enum<EClass>().c_str());
    printf("str_all_of_enum<EClass>() = %s\n", str_all_of_enum<EClass>().c_str());


    printf("ns2::EClass: num_of_ns2::EClass(Value2) = %zd\n", EnumReflection<ns2::EClass>::num());
    printf("ns2::EClass: name_of_ns2::EClass(Value2) = %.*s\n", DOT_STAR_S(EnumReflection<ns2::EClass>::name(ns2::EClass::Value2)));
    printf("ns2::EClass: name_of_ns2::EClass(Value3) = %.*s\n", DOT_STAR_S(EnumReflection<ns2::EClass>::name(ns2::EClass::Value3)));

    printf("ns2::EClass: value_of_ns2::EClass('Value1') = %d\n", EnumReflection<ns2::EClass>::value("Value1"));
    printf("ns2::EClass: value_of_ns2::EClass('Value2') = %d\n", EnumReflection<ns2::EClass>::value("Value2"));
    printf("ns2::EClass: value_of_ns2::EClass('Value3') = %d\n", EnumReflection<ns2::EClass>::value("Value3"));

    try {
        printf("ns2::EClass: value_of_ns2::EClass('Unknown') = %d\n", EnumReflection<ns2::EClass>::value("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("ns2::EClass: catched: %s\n", ex.what());
    }

    return 0;
}
