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
    public:
        typedef boost::fibers::context::wait_queue_t wait_queue_t;
        FiberYield() noexcept {
            m_active_context_pp = NULL;
            m_sched = NULL;
        }
        explicit FiberYield(int /*init_tag*/) {
            init_in_fiber_thread();
        }
        // ensure initialization order:
        //    boost::fibers::context::active_pp() must be called first
        explicit FiberYield(boost::fibers::context** active_pp) noexcept {
            assert(boost::fibers::context::active_pp() == active_pp);
            m_active_context_pp = active_pp;
            m_sched = (*active_pp)->get_scheduler();
        }
        void init_in_fiber_thread() noexcept {
            m_active_context_pp = boost::fibers::context::active_pp();
            m_sched = (*m_active_context_pp)->get_scheduler();
        }
        inline void yield() noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                assert(boost::fibers::context::active_pp() == m_active_context_pp);
                assert((*m_active_context_pp)->get_scheduler() == m_sched);
                //m_sched->yield(*m_active_context_pp); // official boost
                m_sched->yield(m_active_context_pp);
            } else {
                yield_slow();
            }
        }
        inline void unchecked_yield() noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            //m_sched->yield(*m_active_context_pp); // official boost
            m_sched->yield(m_active_context_pp);
        }

        inline void wait(boost::fibers::context** wc) noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                unchecked_wait(wc);
            } else {
                wait_slow(wc);
            }
        }
        inline void unchecked_wait(boost::fibers::context** wc) noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            assert((*wc)->get_scheduler() == m_sched);
            assert(wc != m_active_context_pp);
            *wc = *m_active_context_pp;
            m_sched->suspend(m_active_context_pp);
        }
        inline void notify(boost::fibers::context** wc) noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                unchecked_notify(wc);
            } else {
                notify_slow(wc);
            }
        }
        inline void unchecked_notify(boost::fibers::context** wc) noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            assert((*wc)->get_scheduler() == m_sched);
            assert((*wc) != (*m_active_context_pp));
            assert((*wc) != nullptr);
            m_sched->schedule(*wc);
        }

        inline void wait(wait_queue_t& wq) noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                unchecked_wait(wq);
            } else {
                wait_slow(wq);
            }
        }
        inline void unchecked_wait(wait_queue_t& wq) noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            (*m_active_context_pp)->wait_link(wq);
            m_sched->suspend();
        }

        inline void notify_one(wait_queue_t& wq) noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                unchecked_notify_one(wq);
            } else {
                notify_one_slow(wq);
            }
        }
        inline void unchecked_notify_one(wait_queue_t& wq) noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            assert(!wq.empty());
            boost::fibers::context* ctx = &wq.front();
            wq.pop_front();
            m_sched->schedule(ctx);
        }

        inline void notify_max_one(wait_queue_t& wq) noexcept {
            if (terark_likely(NULL != m_active_context_pp)) {
                unchecked_notify_max_one(wq);
            } else {
                notify_max_one_slow(wq);
            }
        }
        inline void unchecked_notify_max_one(wait_queue_t& wq) noexcept {
            assert(NULL != m_active_context_pp);
            assert(NULL != m_sched);
            assert(boost::fibers::context::active_pp() == m_active_context_pp);
            assert((*m_active_context_pp)->get_scheduler() == m_sched);
            if (!wq.empty()) {
                boost::fibers::context* ctx = &wq.front();
                wq.pop_front();
                m_sched->schedule(ctx);
            }
        }

        size_t notify_all(wait_queue_t&) noexcept;
        size_t unchecked_notify_all(wait_queue_t&) noexcept;

    private:
        boost::fibers::context**  m_active_context_pp;
        boost::fibers::scheduler* m_sched;
        void yield_slow() noexcept;
        void wait_slow(boost::fibers::context** wc) noexcept;
        void notify_slow(boost::fibers::context** wc) noexcept;
        void wait_slow(wait_queue_t&) noexcept;
        void notify_one_slow(wait_queue_t&) noexcept;
        void notify_max_one_slow(wait_queue_t&) noexcept;
    };

    class TERARK_DLL_EXPORT FiberWait : public FiberYield {
        boost::fibers::context::wait_queue_t m_wait_queue;
    public:
        FiberWait() noexcept {}
        explicit FiberWait(boost::fibers::context** pp) noexcept : FiberYield(pp) {}

        ~FiberWait() { assert(m_wait_queue.empty()); }

        inline void wait() noexcept {
            FiberYield::wait(m_wait_queue);
        }
        inline void unchecked_wait() noexcept {
            FiberYield::unchecked_wait(m_wait_queue);
        }

        inline void notify_one() noexcept {
            FiberYield::notify_one(m_wait_queue);
        }
        inline void unchecked_notify_one() noexcept {
            FiberYield::unchecked_notify_one(m_wait_queue);
        }

        inline void notify_max_one() noexcept {
            FiberYield::notify_max_one(m_wait_queue);
        }
        inline void unchecked_notify_max_one() noexcept {
            FiberYield::unchecked_notify_max_one(m_wait_queue);
        }

        inline size_t notify_all() noexcept {
            return FiberYield::notify_all(m_wait_queue);
        }
        inline size_t unchecked_notify_all() noexcept {
            return FiberYield::unchecked_notify_all(m_wait_queue);
        }
    };
}
