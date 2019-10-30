//
// Created by leipeng on 2019-08-26.
//

#include "fiber_yield.hpp"

namespace terark {

    void FiberYield::yield_slow() noexcept {
        init_in_fiber_thread();
        //m_sched->yield(*m_active_context_pp); // official boost
        m_sched->yield(m_active_context_pp);
    }

///////////////////////////////////////////////////////////////////////////

    void FiberYield::wait_slow(boost::fibers::context** wc) noexcept {
        init_in_fiber_thread();
        unchecked_wait(wc);
    }
    void FiberYield::notify_slow(boost::fibers::context** wc) noexcept {
        init_in_fiber_thread();
        unchecked_notify(wc);
    }

    void FiberYield::wait_slow(wait_queue_t& wq) noexcept {
        init_in_fiber_thread();
        unchecked_wait(wq);
    }

    void FiberYield::notify_one_slow(wait_queue_t& wq) noexcept {
        init_in_fiber_thread();
        unchecked_notify_one(wq);
    }

    void FiberYield::notify_max_one_slow(wait_queue_t& wq) noexcept {
        init_in_fiber_thread();
        unchecked_notify_max_one(wq);
    }

    size_t FiberYield::notify_all(wait_queue_t& wq) noexcept {
        if (terark_likely(NULL != m_active_context_pp)) {
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
        } else {
            init_in_fiber_thread();
        }
        return unchecked_notify_all(wq);
    }

    size_t FiberYield::unchecked_notify_all(wait_queue_t& wq) noexcept {
        assert(NULL != m_active_context_pp);
        assert(NULL != m_sched);
        assert(boost::fibers::context::active_pp() == m_active_context_pp);
        assert((*m_active_context_pp)->get_scheduler() == m_sched);
        size_t cnt = 0;
        while (!wq.empty()) {
            boost::fibers::context* ctx = &wq.front();
            wq.pop_front();
            m_sched->schedule(ctx);
            cnt++;
        }
        return cnt;
    }

}
