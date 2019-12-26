#include <terark/util/enum.hpp>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

TERARK_ENUM_PLAIN(EPlain, int, Value1, Value2 = 20, Value3 = 30, MaxVal)
TERARK_ENUM_CLASS(EClass, int, Value1, Value2 = 20, Value3 = 30, MaxVal)

struct ns1 {
  TERARK_ENUM_PLAIN_INCLASS(EPlain, int, Value1, Value2 = 20, Value3 = 30, MaxVal)
};
struct ns2 {
  TERARK_ENUM_CLASS_INCLASS(EClass, int, Value1, Value2 = 20, Value3 = 30, MaxVal)
};

int main() {
    printf("EPlain: num_of_EPlain(Value2) = %zd\n", enum_all_names((EPlain*)0).second);
    printf("EPlain: name_of_EPlain(Value2) = %.*s\n", DOT_STAR_S(enum_name(Value2)));
    printf("EPlain: name_of_EPlain(Value3) = %.*s\n", DOT_STAR_S(enum_name(Value3)));

    printf("EPlain: value_of_EPlain('Value1') = %d\n", enum_value("Value1", MaxVal));
    printf("EPlain: value_of_EPlain('Value2') = %d\n", enum_value("Value2", MaxVal));
    printf("EPlain: value_of_EPlain('Value3') = %d\n", enum_value("Value3", MaxVal));

    printf("EPlain: value_of_EPlain('Unknown') = %d\n", enum_value("Unknown", MaxVal));

    printf("EClass: num_of_EClass(Value2) = %zd\n", enum_all_names((EClass*)0).second);
    printf("EClass: name_of_EClass(Value2) = %.*s\n", DOT_STAR_S(enum_name(EClass::Value2)));
    printf("EClass: name_of_EClass(Value3) = %.*s\n", DOT_STAR_S(enum_name(EClass::Value3)));

    printf("EClass: value_of_EClass('Value1') = %d\n", enum_value("Value1", EClass::MaxVal));
    printf("EClass: value_of_EClass('Value2') = %d\n", enum_value("Value2", EClass::MaxVal));
    printf("EClass: value_of_EClass('Value3') = %d\n", enum_value("Value3", EClass::MaxVal));

    printf("EClass: value_of_EClass('Unknown') = %d\n", enum_value("Unknown", EClass::MaxVal));

    printf("enum_str_all_names<EClass>() = %s\n", enum_str_all_names<EClass>().c_str());
    printf("enum_str_all_namevalues<EClass>() = %s\n", enum_str_all_namevalues<EClass>().c_str());

    printf("ns1::EPlain: num_of_ns1::EPlain(Value2) = %zd\n", enum_all_names((ns1::EPlain*)0).second);
    printf("ns1::EPlain: name_of_ns1::EPlain(Value2) = %.*s\n", DOT_STAR_S(enum_name(ns1::Value2)));
    printf("ns1::EPlain: name_of_ns1::EPlain(Value3) = %.*s\n", DOT_STAR_S(enum_name(ns1::Value3)));

    printf("ns1::EPlain: value_of_ns1::EPlain('Value1') = %d\n", enum_value("Value1", ns1::MaxVal));
    printf("ns1::EPlain: value_of_ns1::EPlain('Value2') = %d\n", enum_value("Value2", ns1::MaxVal));
    printf("ns1::EPlain: value_of_ns1::EPlain('Value3') = %d\n", enum_value("Value3", ns1::MaxVal));

    printf("ns1::EPlain: value_of_ns1::EPlain('Unknown') = %d\n", enum_value("Unknown", ns1::MaxVal));

    printf("enum_str_all_names<EClass>() = %s\n", enum_str_all_names<EClass>().c_str());
    printf("enum_str_all_namevalues<EClass>() = %s\n", enum_str_all_namevalues<EClass>().c_str());

    printf("ns2::EClass: num_of_ns2::EClass(Value2) = %zd\n", enum_all_names((ns2::EClass*)0).second);
    printf("ns2::EClass: name_of_ns2::EClass(Value2) = %.*s\n", DOT_STAR_S(enum_name(ns2::EClass::Value2)));
    printf("ns2::EClass: name_of_ns2::EClass(Value3) = %.*s\n", DOT_STAR_S(enum_name(ns2::EClass::Value3)));

    printf("ns2::EClass: value_of_ns2::EClass('Value1') = %d\n", enum_value("Value1", ns2::EClass::MaxVal));
    printf("ns2::EClass: value_of_ns2::EClass('Value2') = %d\n", enum_value("Value2", ns2::EClass::MaxVal));
    printf("ns2::EClass: value_of_ns2::EClass('Value3') = %d\n", enum_value("Value3", ns2::EClass::MaxVal));

    printf("ns2::EClass: value_of_ns2::EClass('Unknown') = %d\n", enum_value("Unknown", ns2::EClass::MaxVal));

    printf("EPlain define = %.*s\n", DOT_STAR_S(enum_str_define((EPlain*)0)));
    printf("ns1::EPlain define = %.*s\n", DOT_STAR_S(enum_str_define((ns1::EPlain*)0)));

    return 0;
}
