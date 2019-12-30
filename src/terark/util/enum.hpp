// created by leipeng at 2019-12-25
#pragma once

#include <terark/preproc.hpp>
#include <terark/fstring.hpp>
//#include <boost/current_function.hpp>
#include <type_traits>

#if 1

namespace terark {
fstring var_symbol(const char* s);
}

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

template<class Enum>
terark::fstring enum_name(Enum v) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    if (v == values[i])
      return names.first[i];
  }
  return "";
}

template<class Enum>
bool enum_value(const terark::fstring& name, Enum* result) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
      if (name == names.first[i]) {
          *result = values[i];
          return true;
      }
  }
  return false;
}

/// for convenient
template<class Enum>
Enum enum_value(const terark::fstring& name, Enum Default) {
  enum_value(name, &Default);
  return Default;
}

template<class Enum, class Func>
void enum_for_each(Func fn) {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    fn(names.first[i], values[i]);
  }
}

template<class Enum>
std::string enum_str_all_names() {
  auto names = enum_all_names((Enum*)0);
  std::string s;
  for (size_t i = 0; i < names.second; ++i) {
    terark::fstring name = names.first[i];
    s.append(name.p, name.n);
    s.append(", ");
  };
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}

template<class Enum>
std::string enum_str_all_namevalues() {
  typedef decltype(enum_rep_type((Enum*)0)) IntRep;
  auto names = enum_all_names((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  std::string s;
  for (size_t i = 0; i < names.second; ++i) {
    terark::fstring name = names.first[i];
    const Enum v = values[i];
    char buf[32];
    s.append(name.p, name.n);
    s.append(" = ");
    s.append(buf, snprintf(buf, sizeof(buf),
      std::is_signed<IntRep>::value ? "%zd" : "%zu",
      size_t(v)));
    s.append(", ");
  };
  if (s.size()) {
    s.resize(s.size()-2);
  }
  return s;
}


#define TERARK_PP_SYMBOL(ctx, arg) terark::var_symbol(#arg)

///@param Inline can be 'inline' or 'friend'
///@param ... enum values
#define TERARK_ENUM_IMPL(Inline, Class, EnumType, IntRep, EnumScope, ...) \
  enum Class EnumType : IntRep { \
    __VA_ARGS__ \
  }; \
  IntRep enum_rep_type(EnumType*); \
  Inline terark::fstring enum_str_define(EnumType*) { \
    return TERARK_PP_STR(enum Class EnumType : IntRep) \
      " { " #__VA_ARGS__ " }"; \
  } \
  Inline std::pair<const terark::fstring*, size_t> \
  enum_all_names(EnumType*) { \
    static const terark::fstring s_names[] = { \
      TERARK_PP_MAP(TERARK_PP_SYMBOL, ~, __VA_ARGS__) }; \
    return std::make_pair(s_names, TERARK_PP_EXTENT(s_names)); \
  } \
  Inline const EnumType* enum_all_values(EnumType*) { \
    static const EnumType s_values[] = { \
      TERARK_PP_MAP(TERARK_PP_PREPEND, \
                    EnumValueInit<EnumType>() - EnumScope, \
                    __VA_ARGS__) }; \
      return s_values; \
   }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

///@param ... enum values
#define TERARK_ENUM_PLAIN(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(inline,,EnumType,IntRep,,__VA_ARGS__)

#define TERARK_ENUM_PLAIN_INCLASS(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(friend,,EnumType,IntRep,,__VA_ARGS__)

///@param ... enum values
#define TERARK_ENUM_CLASS(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(inline,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

#define TERARK_ENUM_CLASS_INCLASS(EnumType, IntRep, ...) \
  TERARK_ENUM_IMPL(friend,class,EnumType,IntRep,EnumType::,__VA_ARGS__)


//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
/// max number of macro parameters in Visual C++ is 127, this makes
/// TERARK_PP_MAP only support max 61 __VA_ARGS__
/// so we use:
///   TERARK_BIG_ENUM_PLAIN
///   TERARK_BIG_ENUM_CLASS
///   TERARK_BIG_ENUM_PLAIN_INCLASS
///   TERARK_BIG_ENUM_CLASS_INCLASS
/// arguments are grouped by parents, this enlarges max allowed enum values.
/// example:
///   TERARK_BIG_ENUM_PLAIN(MyEnum, int, (v1, v2), (v3, v4), (v5,v6))
///@note
/// enum_str_define(EnumType) = enum MyEnum : int { v1, v2, v3, v4, v5, v6, };
/// ---------------------------------------- this is valid ---------------^
/// there is an extra ", " after value list, this is a valid enum definition.
/// it is too hard to remove the "," so let it be there.

///@param Inline can be 'inline' or 'friend'
///@param ... enum values
#define TERARK_BIG_ENUM_IMPL(Inline, Class, EnumType, IntRep, EnumScope, ...) \
  enum Class EnumType : IntRep { \
    TERARK_PP_FLATTEN(__VA_ARGS__) \
  }; \
  IntRep enum_rep_type(EnumType*); \
  Inline terark::fstring enum_str_define(EnumType*) { \
    return TERARK_PP_STR(enum Class EnumType : IntRep) \
     " { " \
         TERARK_PP_APPLY( \
           TERARK_PP_CAT2(TERARK_PP_JOIN_,TERARK_PP_ARG_N(__VA_ARGS__)), \
           TERARK_PP_APPLY( \
             TERARK_PP_CAT2(TERARK_PP_MAP_,TERARK_PP_ARG_N(__VA_ARGS__)), \
             TERARK_PP_APPEND, ", ", \
             TERARK_PP_STR_FLATTEN(__VA_ARGS__))) "}"; \
  } \
  Inline std::pair<const terark::fstring*, size_t> \
  enum_all_names(EnumType*) { \
    static const terark::fstring s_names[] = { \
      TERARK_PP_BIG_MAP(TERARK_PP_SYMBOL, ~, __VA_ARGS__) }; \
    return std::make_pair(s_names, TERARK_PP_EXTENT(s_names)); \
  } \
  Inline const EnumType* enum_all_values(EnumType*) { \
    static const EnumType s_values[] = { \
      TERARK_PP_BIG_MAP(TERARK_PP_PREPEND, \
                        EnumValueInit<EnumType>() - EnumScope, \
                        __VA_ARGS__) }; \
      return s_values; \
   }

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

///@param ... enum values
#define TERARK_BIG_ENUM_PLAIN(EnumType, IntRep, ...) \
  TERARK_BIG_ENUM_IMPL(inline,,EnumType,IntRep,,__VA_ARGS__)

#define TERARK_BIG_ENUM_PLAIN_INCLASS(EnumType, IntRep, ...) \
  TERARK_BIG_ENUM_IMPL(friend,,EnumType,IntRep,,__VA_ARGS__)

///@param ... enum values
#define TERARK_BIG_ENUM_CLASS(EnumType, IntRep, ...) \
  TERARK_BIG_ENUM_IMPL(inline,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

#define TERARK_BIG_ENUM_CLASS_INCLASS(EnumType, IntRep, ...) \
  TERARK_BIG_ENUM_IMPL(friend,class,EnumType,IntRep,EnumType::,__VA_ARGS__)

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
