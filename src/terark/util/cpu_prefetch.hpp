//
// Created by leipeng on 2019-07-16.
//
#pragma once

#include <xmmintrin.h>

#define TERARK_CPU_PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
