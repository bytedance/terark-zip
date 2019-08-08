//
// Created by leipeng on 2019-08-08.
//
#pragma once

#include <boost/fiber/context.hpp>
#include <boost/fiber/scheduler.hpp>
#include <boost/noncopyable.hpp>

namespace terark {

class ReuseStack {
private:
    std::size_t     size_;
    void*           base_;
public:
    typedef boost::context::stack_traits  traits_type;
    typedef boost::context::stack_context stack_context;

    ReuseStack(void* base, std::size_t size) noexcept :
        size_(size), base_(base) {
    }

    stack_context allocate() noexcept {
        assert(NULL != base_);
        void * vp = base_;
        stack_context sctx;
        sctx.size = size_;
        sctx.sp = static_cast< char * >( vp) + sctx.size;
#if defined(BOOST_USE_VALGRIND)
        sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER( sctx.sp, vp);
#endif
        base_ = NULL;
        return sctx;
    }

    void deallocate( stack_context & sctx) noexcept {
        BOOST_ASSERT( sctx.sp);

#if defined(BOOST_USE_VALGRIND)
        VALGRIND_STACK_DEREGISTER( sctx.valgrind_stack_id);
#endif
     //   void * vp = static_cast< char * >( sctx.sp) - sctx.size;
     //   std::free( vp);
    }
};

// allowing multiple fibers running submit/reap pair
class RunOnceFiberPool {
    class Worker : boost::noncopyable {
    public:
        void* m_stack_base;
        size_t m_stack_size;
        boost::intrusive_ptr<boost::fibers::context> m_fiber;
        int m_next = -1;

        template<class Fn, class ... Arg>
        boost::fibers::context* setup(Fn&& fn, Arg... arg) {
            using namespace boost::fibers;
            assert(!m_fiber);
            m_fiber = make_worker_context(launch::post,
                          ReuseStack(m_stack_base, m_stack_size),
                          std::forward<Fn>(fn), std::forward<Arg>(arg)...);
            return m_fiber.get();
        }

        Worker() {
            m_stack_size = ReuseStack::traits_type::default_size();
            m_stack_base = malloc(m_stack_size);
            if (!m_stack_base) {
                std::terminate();
            }
        }

        ~Worker() {
            assert(!m_fiber);
            free(m_stack_base);
        }
    };

    std::vector<Worker> m_workers;
    boost::fibers::context** m_active_pp;
    boost::fibers::scheduler* m_sched;
    int m_freehead = -1;
public:
    explicit RunOnceFiberPool(size_t num) : m_workers(num) {
        using namespace boost::fibers;
        m_active_pp = context::active_pp();
        m_sched = (*m_active_pp)->get_scheduler();
        for (size_t i = 0; i < num; ++i) {
            auto& w = m_workers[i];
            w.m_next = int(i + 1);
        }
        m_workers.back().m_next = -1;
        m_freehead = 0;
    }
    ~RunOnceFiberPool() {
        for (auto& w : m_workers) {
            assert(!w.m_fiber);
        }
    }

    ///@param myhead must be initialized to -1
    /// can be called multiple times with same 'myhead'
    template<class Fn, class... Arg>
    void submit(int& myhead, Fn&& fn, Arg... arg) {
        assert(-1 == myhead || size_t(myhead) < m_workers.size());
        if (-1 != m_freehead) {
            int oldfree_idx = m_freehead;
            auto& oldfree = m_workers[oldfree_idx];
            m_freehead = oldfree.m_next;
            oldfree.m_next = myhead;
            myhead = oldfree_idx;
            auto ctx = oldfree.setup(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
            m_sched->attach_worker_context(ctx);
            m_sched->schedule(ctx);
        }
        else {
            // run in active fiber
            fn(std::forward<Arg>(arg)...);
        }
    }

    void yield() {
        m_sched->yield(*m_active_pp);
    }

    void reap(int myhead) {
        for (int p = myhead; -1 != p;) {
            auto& w = m_workers[p];
            w.m_fiber->join();
            w.m_fiber.reset();
            // remove from myhead list and put to m_freehead list
            int next = w.m_next;
            w.m_next = m_freehead;
            m_freehead = p;
            p = next;
        }
    }
};

}
