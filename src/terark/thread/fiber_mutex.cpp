//
// Created by leipeng on 2019-08-16.
//

#include "fiber_mutex.hpp"
#include <boost/fiber/operations.hpp>

namespace terark {

void FiberMutex::lock() {
    while (terark_unlikely(!m_mutex.try_lock())) {
        if (std::this_thread::get_id() == m_lock_owner_thread_id) {
            boost::this_fiber::yield();
        } else {
            break;
        }
    }
    m_mutex.lock();
    m_lock_owner_thread_id = std::this_thread::get_id();
}

}
