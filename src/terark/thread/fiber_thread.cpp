//
// Created by leipeng on 2019-08-16.
//

#include "fiber_thread.hpp"
#include <thread>
#include <boost/fiber/fiber.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>

namespace terark {

class FiberThread::Impl {
public:
    valvec<boost::fibers::fiber> m_fiber_vec;
    function<void()>             m_fiber_fun;
    std::thread                  m_thread;
    std::atomic<int>             m_refcnt;

    void thread_fn() {
        for (size_t i = 1; i < m_fiber_vec.capacity(); ++i) {
            m_fiber_vec.unchecked_emplace_back(m_fiber_fun);
        }
        m_fiber_fun(); // run in thread main func

        for (size_t i = 0; i < m_fiber_vec.size(); ++i) {
            m_fiber_vec[i].join();
        }
        assert(m_refcnt >= 1);
        unref();
    }

    Impl(function<void()>&& fn, size_t num_fibers)
     : m_fiber_vec(num_fibers-1, valvec_reserve())
     , m_fiber_fun(std::move(fn))
     , m_thread([this](){thread_fn();})
     , m_refcnt(2)
    {
    }

    void unref() {
        if (0 == --m_refcnt) {
            m_thread.detach();
            delete this;
        }
    }
};

FiberThread::FiberThread(function<void()> fn, size_t num_fibers) {
    maximize(num_fibers, 1u);
    m_impl = new Impl(std::move(fn), num_fibers);
}

FiberThread::~FiberThread() {
    assert(nullptr == m_impl);
}

std::thread::id FiberThread::get_id() const noexcept {
    return m_impl->m_thread.get_id();
}

void FiberThread::dettach() noexcept {
    assert(m_impl);
    assert(m_impl->m_refcnt == 2);
    m_impl->unref();
    m_impl = nullptr;
}

void FiberThread::join() {
    assert(m_impl);
    assert(m_impl->m_refcnt == 2);
    m_impl->m_thread.join();
    assert(m_impl->m_refcnt == 1);
    delete m_impl;
    m_impl = nullptr;
}

}
