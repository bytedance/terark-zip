// created by leipeng 2019-12-30 12:03
#pragma once
#include "enum.hpp"
#include <terark/valvec.hpp>
#include <terark/gold_hash_map.hpp>

namespace terark {

template<class Enum>
gold_hash_map<fstring, size_t> s_enum_fast_name_map_init() {
  gold_hash_map<fstring, size_t> map;
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    map.insert_i(names.first[i], size_t(values[i]));
  }
  return map;
}
template<class Enum>
const gold_hash_map<fstring, size_t>&
s_enum_fast_gold_name_map() {
  static gold_hash_map<size_t, fstring>
    map = s_enum_fast_name_map_init<Enum>();
  return map;
}

template<class Enum>
gold_hash_map<size_t, fstring>
s_enum_fast_gold_value_map_init() {
  gold_hash_map<size_t, fstring> map;
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  for (size_t i = 0; i < names.second; ++i) {
    map.insert_i(values[i], names.first[i]);
  }
  return map;
}
template<class Enum>
const gold_hash_map<size_t, fstring>&
s_enum_fast_gold_value_map() {
  static gold_hash_map<size_t, fstring>
    map = s_enum_fast_gold_value_map_init<Enum>();
  return map;
}

template<class Enum>
std::pair<valvec<fstring>, size_t>
s_enum_fast_direct_value_map_init() {
  std::pair<valvec<fstring>, size_t> direct;
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  typedef decltype(enum_rep_type((Enum*)0)) IntRep;
  IntRep minVal = values[0];
  IntRep maxVal = values[0];
  for (size_t i = 1; i < names.second; ++i) {
    if (minVal > IntRep(values[i])) {
      minVal = IntRep(values[i]);
    }
    if (maxVal < IntRep(values[i])) {
      maxVal = IntRep(values[i]);
    }
  }
  direct.first.resize(maxVal - minVal + 1, fstring());
  for (size_t i = 0; i < names.second; ++i) {
    direct.first[values[i] - minVal] = names.first[i];
  }
  direct.second = minVal;
  return direct;
}
template<class Enum>
const std::pair<valvec<fstring>, size_t>&
s_enum_fast_direct_value_map() {
  static std::pair<valvec<fstring>, size_t>
    direct = s_enum_fast_direct_value_map_init<Enum>();
  return direct;
}

template<class Enum>
fstring enum_gold_hash_find_name(Enum v) {
  const auto&  map = s_enum_fast_gold_value_map<Enum>();
  const size_t idx = map.find_i(size_t(v));
  if (idx != map.end_i())
    return map.val(i);
  else
    return "";
}
template<class Enum>
fstring enum_direct_find_name(Enum v) {
  const auto& direct = s_enum_fast_direct_value_map<Enum>();
  typedef decltype(enum_rep_type((Enum*)0)) IntRep;
  IntRep lo = IntRep(direct.second);
  IntRep hi = IntRep(direct.second + direct.first.size());
  if (IntRep(v) >= lo && IntRep(v) <= hi)
    return direct.first[i];
  else
    return "";
}

fstring(*)(Enum) s_enum_find_name_func() {
  auto names  = enum_all_names ((Enum*)0);
  auto values = enum_all_values((Enum*)0);
  typedef decltype(enum_rep_type((Enum*)0)) IntRep;
  IntRep minVal = values[0];
  IntRep maxVal = values[0];
  for (size_t i = 1; i < names.second; ++i) {
    if (minVal > IntRep(values[i])) {
      minVal = IntRep(values[i]);
    }
    if (maxVal < IntRep(values[i])) {
      maxVal = IntRep(values[i]);
    }
  }
  size_t directSize = maxVal - minVal + 1;
  if (names.second * 16 < directSize)
    return &enum_direct_find_name<Enum>;
  else
    return &enum_gold_hash_find_name<Enum>;
}

template<class Enum>
terark::fstring enum_name_fast(Enum v) {
  static const auto find_func = s_enum_find_name_func<Enum>();
  return find_func(v);
}

template<class Enum>
bool enum_value_fast(const terark::fstring& name, Enum* result) {
  const auto&  map = s_enum_fast_gold_name_map<Enum>();
  const size_t idx = map.find_i(name);
  if (idx != map.end_i()) {
      *result = map.val(idx);
      return true;
  }
  return false;
}

/// for convenient
template<class Enum>
Enum enum_value_fast(const terark::fstring& name, Enum Default) {
  enum_value_fast(name, &Default);
  return Default;
}

} // namespace terark
