//
// Created by leipeng on 2019-06-18.
//

#pragma once

#include <stdio.h>
#include <terark/config.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/util/function.hpp>
#include <thread>
#include <memory>
#include <utility>
#include <future>

namespace terark {

TERARK_DLL_EXPORT int system_vfork(const char*);

TERARK_DLL_EXPORT
void vfork_cmd(fstring cmd, fstring stdinData,
                function<void(std::string&& stdoutData, std::exception*)>,
                fstring tmpFilePrefix = "");

TERARK_DLL_EXPORT
std::future<std::string>
vfork_cmd(fstring cmd, fstring stdinData, fstring tmpFilePrefix = "");

/// Notes:
///   1. If mode = "r", then stdout redirect  in @param cmd is not allowed
///   2. If mode = "w", then stdin  redirect  in @param cmd is not allowed
///   3. stderr redirect such as 2>&1 or 1>&2 in @param cmd is not allowed
///   4. If redirect rules are violated, the behavior is undefined
///   5. If you needs redirect, write a wrapping shell script
class TERARK_DLL_EXPORT ProcPipeStream : public FileStream {
    using FileStream::dopen;
    using FileStream::size;
    using FileStream::attach;
    using FileStream::detach;
    using FileStream::rewind;
    using FileStream::seek;
    using FileStream::chsize;
    using FileStream::fsize;
    using FileStream::pread;
    using FileStream::pwrite;
    using FileStream::tell;

    int m_pipe[2];
    int m_err;
    bool m_mode_is_read;
    volatile int m_child_step;
    intptr_t m_childpid;
    std::string m_cmd;
    std::unique_ptr<std::thread> m_thr;

    void vfork_exec_wait() noexcept;
    void close_pipe() noexcept;
    void wait_proc() noexcept;

public:
    ProcPipeStream() noexcept;
    ProcPipeStream(fstring cmd, fstring mode);
    ~ProcPipeStream();
    void open(fstring cmd, fstring mode);
    bool xopen(fstring cmd, fstring mode) noexcept;

    ///@{
    ///@param onFinish called after sub proc finished
    ///@note close/xclose must be called before onFinish
    ///@note close/xclose can not be called in onFinish
    ProcPipeStream(fstring cmd, fstring mode, function<void(ProcPipeStream*)> onFinish);
    void open(fstring cmd, fstring mode, function<void(ProcPipeStream*)> onFinish);
    bool xopen(fstring cmd, fstring mode, function<void(ProcPipeStream*)> onFinish) noexcept;
    void wait_finish() noexcept;
    ///@}

    void close();
    int xclose() noexcept;
    int err_code() const noexcept { return m_err; }
};

} // namespace terark
