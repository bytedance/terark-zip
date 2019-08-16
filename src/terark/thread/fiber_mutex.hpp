//
// Created by leipeng on 2019-08-16.
//
#pragma once

#include <mutex>
#include <thread>
#include <terark/config.hpp>

namespace terark {

class TERARK_DLL_EXPORT FiberMutex {
    std::mutex m_mutex;
    std::thread::id m_lock_owner_thread_id;
public:
    void lock();
    inline void unlock() {
        m_lock_owner_thread_id = std::thread::id(); // null id
        m_mutex.unlock();
    }
};

}
