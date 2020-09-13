//
// Created by leipeng on 2019-08-22.
//
#pragma once

#include <terark/config.hpp>
#include <stdio.h> // for size_t, ssize_t
#include <stdint.h>
#include <sys/types.h>

namespace terark {

TERARK_DLL_EXPORT
intptr_t fiber_aio_read(int fd, void* buf, size_t len, off_t offset);

TERARK_DLL_EXPORT
void fiber_aio_need(const void* buf, size_t len);

TERARK_DLL_EXPORT
intptr_t fiber_aio_write(int fd, const void* buf, size_t len, off_t offset);

/// put the write to a dedicated thread to execute the write by aio
TERARK_DLL_EXPORT
intptr_t fiber_put_write(int fd, const void* buf, size_t len, off_t offset);


} // namespace terark
