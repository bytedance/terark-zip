//
// Created by leipeng on 2019-06-18.
//

#pragma once

#include <stdio.h>
#include <terark/config.hpp>

namespace terark {

    TERARK_DLL_EXPORT int system_vfork(const char*);

#if !defined(_MSC_VER) && 0 // TODO
    TERARK_DLL_EXPORT FILE* popen_vfork(const char* fname, const char* mode);
#endif

}