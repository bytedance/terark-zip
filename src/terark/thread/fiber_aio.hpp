//
// Created by leipeng on 2019-08-22.
//
#pragma once

#include <terark/config.hpp>
#include <stdio.h> // for size_t, ssize_t

namespace terark {

TERARK_DLL_EXPORT
ssize_t fiber_aio_read(int fd, void* buf, size_t len, off_t offset);

TERARK_DLL_EXPORT
void fiber_aio_need(const void* buf, size_t len);


} // namespace terark
