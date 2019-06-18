//
// Created by leipeng on 2019-06-18.
//

#include "process.hpp"
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#if !defined(_MSC_VER)
    #include <sys/types.h>
    #include <sys/wait.h>
    #include <unistd.h>
#endif

namespace terark {

    // system(cmd) on Linux calling fork which do copy page table
    // we should use vfork
    TERARK_DLL_EXPORT int system_vfork(const char* cmd) {
    #if defined(_MSC_VER)
        return ::system(cmd); // windows has no fork performance issue
    #else
        pid_t childpid = vfork();
        if (0 == childpid) { // child process
            execl("/bin/sh", "-c", cmd, NULL);
            int err = errno;
            fprintf(stderr, "execlp /bin/sh -c \"%s\" = %s\n", cmd, strerror(err));
            return err;
        }
        int childstatus = 0;
        int err = waitpid(childpid, &childstatus, 0);
        if (err) {
            fprintf(stderr, "wait /bin/sh -c \"%s\" = %s\n", cmd, strerror(err));
            return err;
        }
        return childstatus;
    #endif
    }

#if !defined(_MSC_VER) && 0 // TODO
    TERARK_DLL_EXPORT FILE* popen_vfork(const char* fname, const char* mode) {
        // TODO:
    }
#endif

} // namespace terark