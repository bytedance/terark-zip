//
// Created by leipeng on 2019-06-18.
//

#include "process.hpp"
#include <terark/num_to_str.hpp>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#if defined(_MSC_VER)
    #define NOMINMAX
    #define WIN32_LEAN_AND_MEAN
    #include <windows.h>
    #include <io.h>
#else
    #include <sys/types.h>
    #include <sys/wait.h>
    #include <unistd.h>
#endif
#include "stat.hpp"
#include <fcntl.h>

#include <terark/num_to_str.hpp>
#include <terark/util/function.hpp>

namespace terark {

    // system(cmd) on Linux calling fork which do copy page table
    // we should use vfork
    TERARK_DLL_EXPORT int system_vfork(const char* cmd) {
    #if defined(_MSC_VER)
        return ::system(cmd); // windows has no fork performance issue
    #else
        pid_t childpid = vfork();
        if (0 == childpid) { // child process
            execl("/bin/sh", "/bin/sh", "-c", cmd, NULL);
            int err = errno;
            fprintf(stderr, "ERROR: execl /bin/sh -c \"%s\" = %s\n", cmd, strerror(err));
            _exit(err);
        }
        else if (childpid < 0) {
            int err = errno;
            fprintf(stderr, "ERROR: vfork() = %s\n", strerror(err));
            return err;
        }
        int childstatus = 0;
        pid_t pid = waitpid(childpid, &childstatus, 0);
        if (pid != childpid) {
            int err = errno;
            fprintf(stderr, "ERROR: wait /bin/sh -c \"%s\" = %s\n", cmd, strerror(err));
            return err;
        }
        return childstatus;
    #endif
    }

/////////////////////////////////////////////////////////////////////////////

ProcPipeStream::ProcPipeStream() noexcept {
    m_pipe[0] = m_pipe[1] = -1;
    m_err = 0;
    m_childstatus = 0;
    m_child_step = 0;
    m_childpid = -1;
}

ProcPipeStream::ProcPipeStream(fstring cmd, fstring mode)
  : ProcPipeStream(cmd, mode, function<void(ProcPipeStream*)>()) {
}

ProcPipeStream::ProcPipeStream(fstring cmd, fstring mode,
                               function<void(ProcPipeStream*)> onFinish) {
    m_pipe[0] = m_pipe[1] = -1;
    m_err = 0;
    m_childstatus = 0;
    m_child_step = 0;
    m_childpid = -1;

    open(cmd, mode, std::move(onFinish));
}

ProcPipeStream::~ProcPipeStream() {
    if (m_fp) {
        try { close(); } catch (const std::exception&) {}
    }
}

void ProcPipeStream::open(fstring cmd, fstring mode) {
    open(cmd, mode, function<void(ProcPipeStream*)>());
}

void ProcPipeStream::open(fstring cmd, fstring mode,
                          function<void(ProcPipeStream*)> onFinish) {
    if (m_fp) {
        THROW_STD(invalid_argument, "File is already open");
    }
    if (!xopen(cmd, mode, std::move(onFinish))) {
        THROW_STD(logic_error,
               "cmd = %s, mode = %s, m_err = %d(%s), errno = %d(%s)",
                cmd.c_str(), mode.c_str(),
                m_err, strerror(m_err),
                errno, strerror(errno)
                );
    }
}

bool ProcPipeStream::xopen(fstring cmd, fstring mode) noexcept {
    return xopen(cmd, mode, function<void(ProcPipeStream*)>());
}

bool ProcPipeStream::xopen(fstring cmd, fstring mode,
                           function<void(ProcPipeStream*)> onFinish)
noexcept {
#if defined(_MSC_VER)
    THROW_STD(invalid_argument, "Not implemented");
#else
    if (m_fp) {
        fprintf(stderr, "ERROR: %s:%d:%s: File is already open\n",
                __FILE__, __LINE__, BOOST_CURRENT_FUNCTION);
        errno = EINVAL;
        return false;
    }
    if (mode.strchr('r'))
        m_mode_is_read = true;
    else if (mode.strchr('w') || mode.strchr('a'))
        m_mode_is_read = false;
    else {
        fprintf(stderr, "ERROR: %s:%d:%s: mode = \"%s\" is invalid\n",
                __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, mode.c_str());
        errno = EINVAL; // invalid argument
        return false;
    }

    int err = ::pipe(m_pipe);
    if (err < 0) {
        fprintf(stderr, "ERROR: pipe() = %s\n", strerror(errno));
        return false;
    }
    set_close_on_exec(m_pipe[m_mode_is_read?0:1]);

    {
        // cmd2 = cmd + " > /dev/fd/m_pipe[0|1]"
        string_appender<std::string> cmd2;
        cmd2.reserve(cmd.size() + 32);
        cmd2.append(cmd.c_str(), cmd.size());
        cmd2.append(m_mode_is_read ? " >" : " <");
        cmd2.append(" /dev/fd/");
        cmd2 << (m_mode_is_read ? m_pipe[1] : m_pipe[0]);
        cmd2.swap(m_cmd);
    }

//  fprintf(stderr, "INFO: mode = %s, m_pipe = [%d, %d], m_cmd = %s\n", mode.c_str(), m_pipe[0], m_pipe[1], m_cmd.c_str());

    m_fp = fdopen(m_pipe[m_mode_is_read?0:1] , mode.c_str());
    if (NULL == m_fp) {
        fprintf(stderr, "ERROR: fdopen(\"%s\", \"%s\") = %s\n", m_cmd.c_str(), mode.c_str(), strerror(m_err));
        ::close(m_pipe[0]); m_pipe[0] = -1;
        ::close(m_pipe[1]); m_pipe[1] = -1;
        return false;
    }
    //this->disbuf();
    fprintf(stderr, "INFO: fdopen(\"%s\") done\n", m_cmd.c_str());

 // fprintf(stderr, "INFO: onFinish is defined = %d\n", bool(onFinish));
    if (onFinish) {
        // onFinish can own the lifetime of this
        std::thread([this,onFinish=std::move(onFinish)]() {
            this->vfork_exec_wait();
            try { onFinish(this); }
            catch (const std::exception& ex) {
                fprintf(stderr, "ERROR: user onFinish thrown: %s\n", ex.what());
                m_err = -1;
            }
            this->m_child_step = 3;
        }).detach();
    } else {
        m_thr.reset(new std::thread(&ProcPipeStream::vfork_exec_wait, this));
    }
    while (m_child_step < 1) {
        usleep(1000); // 1 ms
        //std::this_thread::yield();
    }
//  fprintf(stderr, "INFO: waited m_child_step = %d\n", m_child_step);
    return true;
#endif
}

void ProcPipeStream::vfork_exec_wait() noexcept {
#if !defined(_MSC_VER)
    fprintf(stderr, "INFO: calling vfork, m_cmd = \"%s\"\n", m_cmd.c_str());
    m_childpid = vfork();
    if (0 == m_childpid) { // child process
    //  fprintf(stderr, "INFO: calling execl /bin/sh -c \"%s\" = %s\n", m_cmd.c_str(), strerror(errno));
        execl("/bin/sh", "/bin/sh", "-c", m_cmd.c_str(), NULL);
        int err = errno;
        fprintf(stderr, "ERROR: execl /bin/sh -c \"%s\" = %s\n", m_cmd.c_str(), strerror(err));
        _exit(err);
    } else if (m_childpid < 0) {
        m_err = errno;
        fprintf(stderr, "ERROR: vfork() = %s\n", strerror(m_err));
        return;
    }
//  fprintf(stderr, "INFO: vfork done, childpid = %zd\n", m_childpid);
    m_child_step = 1;
    m_childstatus = 0;
    pid_t pid = waitpid(m_childpid, &m_childstatus, 0);
    if (pid != m_childpid) {
        m_err = errno;
        fprintf(stderr, "ERROR: wait /bin/sh -c \"%s\" = %s\n", m_cmd.c_str(), strerror(m_err));
        return;
    }
//  fprintf(stderr, "INFO: child proc done, m_childstatus = %d ++++\n", m_childstatus);
    // const int fnofp = fileno(m_fp); // hang in fileno(m_fp) on Mac
    fprintf(stderr, "INFO: childstatus = %d\n", m_childstatus);
    m_err = 0;
    // close peer ...
    if (m_mode_is_read) {
    //  fprintf(stderr, "INFO: parent is read(fd=%d), close write peer(fd=%d)\n", m_pipe[0], m_pipe[1]);
        ::close(m_pipe[1]); m_pipe[1] = -1;
    } else {
    //  fprintf(stderr, "INFO: parent is write(fd=%d), close read peer(fd=%d)\n", m_pipe[1], m_pipe[0]);
        ::close(m_pipe[0]); m_pipe[0] = -1;
    }
    m_child_step = 2;
#endif
}

void ProcPipeStream::close() {
    xclose();
}

int ProcPipeStream::xclose() noexcept {
//  fprintf(stderr, "INFO: xclose(): m_pipe = [%d, %d]\n", m_pipe[0], m_pipe[1]);
    if (m_pipe[0] >= 0 || m_pipe[1] >= 0) {
        close_pipe();
        wait_proc();
    }
    return m_err;
}

void ProcPipeStream::close_pipe() noexcept {
//  fprintf(stderr, "INFO: close_pipe(): m_pipe = [%d, %d]\n", m_pipe[0], m_pipe[1]);
    assert(m_pipe[0] >= 0 || m_pipe[1] >= 0);
//  fprintf(stderr, "INFO: doing fclose(fd=%d)\n", m_pipe[m_mode_is_read?0:1]);
    assert(NULL != m_fp);
    fclose(m_fp);
    m_fp = NULL;
//  fprintf(stderr, "INFO: done  fclose(fd=%d)\n", m_pipe[m_mode_is_read?0:1]);
    m_pipe[m_mode_is_read?0:1] = -1;
}

void ProcPipeStream::wait_proc() noexcept {
    intptr_t waited_ms = 0;
    while (m_child_step < 2) {
        TERARK_IF_MSVC(Sleep(10), usleep(10000)); // 10 ms
        waited_ms += 10;
        if (waited_ms % 5000 == 0) {
            fprintf(stderr, "INFO: waited_ms = %zd\n", waited_ms);
        }
    }
    if (m_thr) {
        assert(m_thr->joinable());
        m_thr->join();
    }
    else {
        while (m_child_step < 3) {
            TERARK_IF_MSVC(Sleep(10), usleep(10000)); // 10 ms
            waited_ms += 10;
            if (waited_ms % 5000 == 0) {
                fprintf(stderr, "INFO: wait onFinish = %zd\n", waited_ms);
            }
        }
    }
//  fprintf(stderr, "INFO: m_thr joined\n");
}

#define ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET 1

struct VforkCmdPromise {
    std::shared_ptr<std::promise<std::string> > promise
        = std::make_shared<std::promise<std::string>>();

    void operator()(std::string&& stdoutData, std::exception* ex) {
    //  fprintf(stderr, "INFO: VforkCmdPromise.set(%s)\n", stdoutData.c_str());
        promise->set_value(std::move(stdoutData));
        if (ex) {
            try {
                throw *ex;
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        }
    }
};

std::string
vfork_cmd(fstring cmd, fstring stdinData, fstring tmpFilePrefix) {
    auto future = vfork_cmd_future(cmd, stdinData, tmpFilePrefix);
    return future.get(); // blocking on this line
}

struct VforkCmdImpl {
    std::string tmp_file;
    string_appender<> cmdw;
    int fd;
    ProcPipeStream proc;

    VforkCmdImpl(fstring cmd, fstring tmpFilePrefix) {
        if (tmpFilePrefix.empty()) {
            tmpFilePrefix = "/tmp/ProcPipeStream-";
        }
        tmp_file = tmpFilePrefix + "XXXXXX";
        fd = mkstemp(&tmp_file[0]);
        if (fd < 0) {
            THROW_STD(runtime_error, "mkstemp(%s) = %s", tmp_file.c_str(),
                      strerror(errno));
        }
        cmdw.reserve(cmd.size() + 32);
#if ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET
        // use  " > /dev/fd/xxx" will prevent from tmp_file being
        // deleted unexpected
        cmdw << cmd << " > /dev/fd/" << fd;
#else
        cmdw << cmd << " > " << tmp_file;
        ::close(fd); fd = -1;
#endif
    }

    ~VforkCmdImpl() {
        if (fd >= 0) {
            ::close(fd);
        }
        if (!tmp_file.empty()) {
            ::remove(tmp_file.c_str());
        }
    }

    std::string read_stdout() {
        //
        // now cmd sub process must have finished
        //
#if ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET
        ::lseek(fd, 0, SEEK_SET); // must lseek to begin
#else
        fd = ::open(tmp_file.c_str(), O_RDONLY);
        if (fd < 0) {
          THROW_STD(runtime_error, "::open(fname=%s, O_RDONLY) = %s",
                    tmp_file.c_str(), strerror(errno));
        }
#endif
        struct ll_stat st;
        if (::ll_fstat(fd, &st) < 0) {
            THROW_STD(runtime_error, "::fstat(fname=%s) = %s",
                      tmp_file.c_str(), strerror(errno));
        }
        std::string result;
        if (st.st_size) {
            result.resize(st.st_size);
            intptr_t rdlen = ::read(fd, &*result.begin(), st.st_size);
            if (intptr_t(st.st_size) != rdlen) {
                THROW_STD(runtime_error,
                          "::read(fname=%s, len=%zd) = %zd : %s",
                          tmp_file.c_str(),
                          size_t(st.st_size), size_t(rdlen),
                          strerror(errno));
            }
        }
        return result;
    }
};

void
vfork_cmd(fstring cmd, fstring stdinData,
          function<void(std::string&&, std::exception*)> onFinish,
          fstring tmpFilePrefix)
{
    auto share = std::make_shared<VforkCmdImpl>(cmd, tmpFilePrefix);

    share->proc.open(share->cmdw, "w",
   [share, onFinish=std::move(onFinish)](ProcPipeStream* proc) {
        try {
            if (proc->err_code() == 0) {
                onFinish(share->read_stdout(), nullptr);
            }
            else {
                string_appender<> msg;
                msg << "vfork_cmd error: ";
                msg << "realcmd = " << share->cmdw << ", ";
                msg << "err_code = " << proc->err_code() << ", ";
                msg << "childstatus = " << proc->childstatus();
                std::runtime_error ex(msg);
                onFinish(share->read_stdout(), &ex);
            }
        }
        catch (std::exception ex) {
            onFinish(std::string(""), &ex);
        }
    });
    share->proc.ensureWrite(stdinData.data(), stdinData.size());
    share->proc.close();
}

std::future<std::string>
vfork_cmd_future(fstring cmd, fstring stdinData, fstring tmpFilePrefix) {
    VforkCmdPromise prom;
    std::future<std::string> future = prom.promise->get_future();
    vfork_cmd(cmd, stdinData, std::move(prom));
    return future;
}

} // namespace terark
