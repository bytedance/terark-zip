#pragma once

#include <terark/preproc.hpp>
#include <terark/fstring.hpp>
//#include <boost/current_function.hpp>
#include <type_traits>

#if 1

namespace terark {

template<class Enum>
class EnumValueInit {
    Enum val;
public:
    operator Enum() const { return val; }

    /// set val
    EnumValueInit& operator-(Enum v) { val = v; return *this; }

    /// absorb the IntRep param
    template<class IntRep>
    EnumValueInit& operator=(IntRep) { return *this; }
};

fstring var_symbol(const char* s);

} // namespace terark

template<class Enum>
struct EnumRepType;

///@param IntRep the underlying IntRep of Enum
///@note the use of IntRep has two goals:
/// 1. the Integeral Rep of the enum
/// 2. makes the s_name and s_value can be intantiated in header file
template<class Enum, class TIntRep = typename EnumRepType<Enum>::type>
class EnumReflection;

template<class Enum>
terark::fstring
name_of_enum(const terark::fstring* names, const Enum* values, size_t num,
             Enum v) {
  for (size_t i = 0; i < num; ++i) {
      if (v == values[i])
          return names[i];
  }
  return "";
}

template<class Enum>
bool
value_of_enum(const terark::fstring* names, const Enum* values, size_t num,
              const terark::fstring& name, Enum* result) {
  for (size_t i = 0; i < num; ++i) {
      if (name == names[i]) {
          *result = values[i];
          return true;
      }
  }
  return false;
}

template<class Enum>
std::string str_all_name_of_enum() {
  std::string s;
  EnumReflection<Enum>::for_each(
    [&s](terark::fstring name, Enum) {
        s.append(name.p, name.n);
        s.append(", ");
    });
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}

template<class Enum>
std::string str_all_of_enum() {
  typedef typename EnumRepType<Enum>::type IntRep;
  std::string s;
  EnumReflection<Enum>::for_each(
    [&s](terark::fstring name, Enum v) {
        char buf[32];
        s.append(name.p, name.n);
        s.append(" = ");
        s.append(buf, snprintf(buf, sizeof(buf),
          std::is_signed<IntRep>::value ? "%zd" : "%zu",
          size_t(v)));
        s.append(", ");
    });
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}

#define TERARK_PP_SYMBOL(ctx, arg) terark::var_symbol(#arg)

///@param ... enum values
#define TERARK_ENUM_IMPL(nsKeyword, nsName, nsQualify, nsBeg, nsEnd, \
                         Class, EnumType, ColonIntRep, IntRep, EnumScope, ...) \
  nsKeyword nsName nsBeg \
  enum Class EnumType ColonIntRep { \
    __VA_ARGS__ \
  }; nsEnd \
  template<> struct EnumRepType<nsQualify EnumType> { typedef IntRep type; }; \
  template<class TIntRep> \
  class EnumReflection<nsQualify EnumType, TIntRep> { \
  public: \
    static const terark::fstring s_define; \
    static const terark::fstring s_names[]; \
    static const nsQualify EnumType s_values[]; \
    static terark::fstring name(const nsQualify EnumType v) { \
      return name_of_enum(s_names, s_values, num(), v); \
    } \
    static nsQualify EnumType value(terark::fstring name) { \
      nsQualify EnumType result;  \
      if (value(name, &result)) return result; \
      else throw std::invalid_argument( \
        std::string("enum " #nsQualify #EnumType ": invalid name = \"")+name+"\""); \
    } \
    static bool value(terark::fstring name, nsQualify EnumType* result) { \
      return value_of_enum(s_names, s_values, num(), name, result); \
    } \
    static size_t num(); \
    template<class Func> \
    static void for_each(Func fn) { \
      for (size_t i = 0; i < num(); ++i) \
        fn(s_names[i], s_values[i]); \
    } \
    static std::string str_all_name() { \
      return str_all_name_of_enum<nsQualify EnumType>(); \
    } \
    static std::string str_all() { \
      return str_all_of_enum<nsQualify EnumType>(); \
    } \
  }; \
  template<class TIntRep> \
  const terark::fstring EnumReflection<nsQualify EnumType, TIntRep>::s_define = {\
    TERARK_PP_STR(nsKeyword nsName nsBeg enum Class EnumType ColonIntRep) \
      " { " #__VA_ARGS__ " };" \
    #nsEnd \
  }; \
  template<class TIntRep> \
  const terark::fstring EnumReflection<nsQualify EnumType, TIntRep>::s_names[] = {\
    TERARK_PP_MAP(TERARK_PP_SYMBOL, ~, __VA_ARGS__) }; \
  template<class TIntRep> \
  const nsQualify EnumType EnumReflection<nsQualify EnumType, TIntRep>::s_values[] = { \
    TERARK_PP_MAP(TERARK_PP_PREPEND, \
                  terark::EnumValueInit<nsQualify EnumType>() EnumScope, \
                  __VA_ARGS__) }; \
  template<class TIntRep> \
  size_t EnumReflection<nsQualify EnumType, TIntRep>::num() { \
    return TERARK_PP_EXTENT(s_names); \
  }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

///@param ... enum values
#define TERARK_ENUM_PLAIN(EnumType, ...) \
        TERARK_ENUM_PLAIN_EX(EnumType, int, __VA_ARGS__)

#define TERARK_ENUM_PLAIN_EX(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(,,,,,, EnumType, : IntRep, IntRep, -, __VA_ARGS__)

#define TERARK_ENUM_PLAIN_NS(nsName, EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(namespace,nsName,nsName::,{,},,EnumType,: IntRep,IntRep,-nsName::,__VA_ARGS__)

///@param ... enum values
#define TERARK_ENUM_CLASS(EnumType, ...) \
        TERARK_ENUM_CLASS_EX(EnumType, int, __VA_ARGS__)

#define TERARK_ENUM_CLASS_EX(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(,,,,,class,EnumType,: IntRep,IntRep,-EnumType::,__VA_ARGS__)

#define TERARK_ENUM_CLASS_NS(nsName, EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(namespace,nsName,nsName::,{,},class,EnumType,: IntRep,IntRep,-nsName::EnumType::,__VA_ARGS__)

#else


///@param ... enum values
#define TERARK_ENUM_PLAIN(EnumType, ...) \
  enum EnumType { \
    __VA_ARGS__ \
  }; \
  static const char* s_name_of_##EnumType[] = { \
    TERARK_PP_STR(__VA_ARGS__) }; \
  static const enum EnumType s_value_of_##EnumType[] = { \
    __VA_ARGS__ }; \
  inline const char* name_of_##EnumType(const enum EnumType v) { \
    for (size_t i = 0; i < TERARK_PP_EXTENT(s_value_of_##EnumType); ++i) { \
      if (v == s_value_of_##EnumType[i]) { \
        return s_name_of_##EnumType[i]; \
      } \
    } \
    return nullptr; \
  } \
  inline enum EnumType value_of_##EnumType(const char* name) { \
    for (size_t i = 0; i < TERARK_PP_EXTENT(s_name_of_##EnumType); ++i) { \
      const char * sp = s_name_of_##EnumType[i]; \
      if (strcmp(name, sp) == 0) \
        return s_value_of_##EnumType[i]; \
    } \
    throw std::invalid_argument( \
      std::string(TERARK_PP_STR(EnumType) ": invalid enum name = \"") + name + "\""); \
  } \
  inline size_t num_of_##EnumType() { \
    return TERARK_PP_EXTENT(s_value_of_##EnumType); \
  }

///@param ... enum values
#define TERARK_ENUM_CLASS(EnumType, ...) \
  enum class EnumType { \
    __VA_ARGS__ \
  }; \
  static const char* s_name_of_##EnumType[] = { \
    TERARK_PP_STR(__VA_ARGS__) }; \
  static const EnumType s_value_of_##EnumType[] = { \
    TERARK_PP_MAP(TERARK_PP_PREPEND, EnumType::, __VA_ARGS__) }; \
  inline const char* name_of_##EnumType(const EnumType v) { \
    for (size_t i = 0; i < TERARK_PP_EXTENT(s_value_of_##EnumType); ++i) { \
      if (v == s_value_of_##EnumType[i]) { \
        return s_name_of_##EnumType[i]; \
      } \
    } \
    return nullptr; \
  } \
  inline EnumType value_of_##EnumType(const char* name) { \
    for (size_t i = 0; i < TERARK_PP_EXTENT(s_name_of_##EnumType); ++i) { \
      const char * sp = s_name_of_##EnumType[i]; \
      if (strcmp(name, sp) == 0) \
        return s_value_of_##EnumType[i]; \
    } \
    throw std::invalid_argument( \
      std::string(TERARK_PP_STR(EnumType) ": invalid enum name = \"") + name + "\""); \
  } \
  inline size_t num_of_##EnumType() { \
    return TERARK_PP_EXTENT(s_value_of_##EnumType); \
  }

#endif
