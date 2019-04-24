#pragma once
#include <terark/config.hpp>
#include <boost/noncopyable.hpp>
#include <mutex>
#if defined(TERARK_WITH_TBB)
  #include <tbb/spin_mutex.h>
#endif

namespace terark {

#if defined(TERARK_WITH_TBB)
  #if TERARK_WITH_TBB+1 >= 2+1
    class TERARK_DLL_EXPORT spin_mutex : boost::noncopyable {
        unsigned char  m_is_locked;
    public:
        spin_mutex() : m_is_locked(0) {}
        void lock();
        void unlock();
    };
  #else
    using tbb::spin_mutex;
  #endif
#else
    typedef std::mutex spin_mutex;
#endif

} // namespace terark
