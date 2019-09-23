//
// Created by leipeng on 2019-08-26.
//
#pragma once

#include <terark/config.hpp>
#include <boost/fiber/scheduler.hpp>

namespace terark {

    //
    // boost::this_fiber::yield() will access thread_local, but
    // accessing thread_local is pretty slow, use this helper to speedup
    //
    class TERARK_DLL_EXPORT FiberYield {
        boost::fibers::context**  m_active_context_pp;
        boost::fibers::scheduler* m_sched;
        void yield_slow();
    public:
        FiberYield() {
            m_active_context_pp = NULL;
            m_sched = NULL;
        }
        explicit FiberYield(int /*init_tag*/) {
            init_in_fiber_thread();
        }
        void init_in_fiber_thread() {
            m_active_context_pp = boost::fibers::context::active_pp();
            m_sched = (*m_active_context_pp)->get_scheduler();
        }
        inline void yield() {
            if (terark_likely(NULL != m_active_context_pp)) {
                assert(boost::fibers::context::active_pp() == m_active_context_pp);
                assert((*m_active_context_pp)->get_scheduler() == m_sched);
                m_sched->yield(*m_active_context_pp);
            } else {
                yield_slow();
            }
        }
        inline void unchecked_yield() {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            m_sched->yield(*m_active_context_pp);
        }
    };

}
