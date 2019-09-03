#include "mutex.hpp"

#if defined(TERARK_WITH_TBB) && TERARK_WITH_TBB+1 >= 2+1
    #include <tbb/tbb_machine.h>
#endif

namespace terark {

#if defined(TERARK_WITH_TBB) && TERARK_WITH_TBB+1 >= 2+1

void spin_mutex::lock() {
    __TBB_LockByte(m_is_locked);
}

void spin_mutex::unlock() {
    __TBB_UnlockByte(m_is_locked);
}
#endif

} // namespace terark
