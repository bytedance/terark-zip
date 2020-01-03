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
#else
    #include <sys/types.h>
    #include <sys/wait.h>
    #include <unistd.h>
#endif
#include <terark/num_to_str.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>

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
    m_child_step = 0;
    m_childpid = -1;
}

ProcPipeStream::ProcPipeStream(fstring cmd, fstring mode) {
    m_pipe[0] = m_pipe[1] = -1;
    m_err = 0;
    m_child_step = 0;
    m_childpid = -1;

    open(cmd, mode);
}

ProcPipeStream::~ProcPipeStream() {
    if (m_pipe[0] >= 0) {
        try { close(); } catch (const std::exception&) {}
    }
}

void ProcPipeStream::open(fstring cmd, fstring mode) {
    if (m_fp) {
        THROW_STD(invalid_argument, "File is already open");
    }
    if (!xopen(cmd, mode)) {
        THROW_STD(logic_error,
               "cmd = %s, mode = %s, m_err = %d(%s), errno = %d(%s)",
                cmd.c_str(), mode.c_str(),
                m_err, strerror(m_err),
                errno, strerror(errno)
                );
    }
}

bool ProcPipeStream::xopen(fstring cmd, fstring mode) noexcept {
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

    m_thr.reset(new std::thread([this]() {
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
        int childstatus = 0;
        pid_t pid = waitpid(m_childpid, &childstatus, 0);
        if (pid != m_childpid) {
            m_err = errno;
            fprintf(stderr, "ERROR: wait /bin/sh -c \"%s\" = %s\n", m_cmd.c_str(), strerror(m_err));
            return;
        }
    //  fprintf(stderr, "INFO: child proc done, childstatus = %d ++++\n", childstatus);
        // const int fnofp = fileno(m_fp); // hang in fileno(m_fp) on Mac
        m_err = childstatus;
        // close peer ...
        if (m_mode_is_read) {
        //  fprintf(stderr, "INFO: parent is read(fd=%d), close write peer(fd=%d)\n", m_pipe[0], m_pipe[1]);
            ::close(m_pipe[1]); m_pipe[1] = -1;
        } else {
        //  fprintf(stderr, "INFO: parent is write(fd=%d), close read peer(fd=%d)\n", m_pipe[1], m_pipe[0]);
            ::close(m_pipe[0]); m_pipe[0] = -1;
        }
        m_child_step = 2;
    }));
    while (m_child_step < 1) {
        usleep(1000); // 1 ms
        //std::this_thread::yield();
    }
//  fprintf(stderr, "INFO: waited m_child_step = %d\n", m_child_step);
    return true;
#endif
}

void ProcPipeStream::close() {
    xclose();
}

int ProcPipeStream::xclose() noexcept {
//  fprintf(stderr, "INFO: xclose(): m_pipe = [%d, %d]\n", m_pipe[0], m_pipe[1]);
    if (m_pipe[0] >= 0 || m_pipe[1] >= 0) {
    //  fprintf(stderr, "INFO: doing fclose(fd=%d)\n", m_pipe[m_mode_is_read?0:1]);
        assert(NULL != m_fp);
        fclose(m_fp);
        m_fp = NULL;
    //  fprintf(stderr, "INFO: done  fclose(fd=%d)\n", m_pipe[m_mode_is_read?0:1]);
        m_pipe[m_mode_is_read?0:1] = -1;
        intptr_t waited_ms = 0;
        while (m_child_step < 2) {
            TERARK_IF_MSVC(Sleep(10), usleep(10000)); // 10 ms
            waited_ms += 10;
            if (waited_ms % 5000 == 0) {
                fprintf(stderr, "INFO: waited_ms = %zd\n", waited_ms);
            }
        }
        m_thr->join();
    //  assert(NULL != m_fp);
    //  fprintf(stderr, "INFO: m_thr joined\n");
        return m_err;
    }
    return 0;
}

#define ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET 1

std::string
ProcPipeStream::run_cmd(fstring cmd, fstring stdinData, fstring tmpFilePrefix) {
    bool tmp_file_created = false;
    if (tmpFilePrefix.empty()) {
      tmpFilePrefix = "/tmp/ProcPipeStream-";
    }
    std::string tmp_file = tmpFilePrefix + "XXXXXX";
    int fd = mkstemp(&tmp_file[0]);
    if (fd < 0) {
      THROW_STD(runtime_error, "mkstemp(%s) = %s", tmp_file.c_str(),
                strerror(errno));
    }
    tmp_file_created = true;
    {
      string_appender<> cmdw;
      cmdw.reserve(cmd.size() + 32);
#if ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET
      // use  " > /dev/fd/xxx" will prevent from tmp_file being
      // deleted unexpected
      cmdw << cmd << " > /dev/fd/" << fd;
#else
      cmdw << cmd << " > " << tmp_file;
      ::close(fd);
#endif
      ProcPipeStream proc(cmdw, "w");
      proc.ensureWrite(stdinData.data(), stdinData.size());
    }
    //
    // now cmd sub process must have finished
    //
    terark::LineBuf result;
    {
#if ProcPipeStream_PREVENT_UNEXPECTED_FILE_DELET
        ::lseek(fd, 0, SEEK_SET); // must lseek to begin
        Auto_fclose tmp_result_file(fdopen(fd, "rb"));
#else
        Auto_fclose tmp_result_file(fopen(tmp_file.c_str(), "rb"));
#endif
        if (!tmp_result_file) {
          THROW_STD(runtime_error, "fdopen(fd=%d(fname=%s), rb) = %s", fd,
                    tmp_file.c_str(), strerror(errno));
        }
        result.read_all(tmp_result_file);
    }
    if (tmp_file_created) {
      ::remove(tmp_file.c_str());
    }
    return std::string(result.p, result.n);
}

} // namespace terark
