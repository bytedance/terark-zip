//
// Created by leipeng on 2019-08-08.
//
#pragma once

#include <boost/fiber/context.hpp>
#include <boost/fiber/scheduler.hpp>
#include <terark/valvec.hpp>

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
    class Worker {
    public:
        void*  m_stack_base;
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

        Worker(size_t stack_size) {
            m_stack_size = stack_size;
            m_stack_base = malloc(stack_size);
            if (!m_stack_base) {
                std::terminate();
            }
        }

        Worker(const Worker&) = delete;
        Worker(Worker&&) = delete;
        Worker&operator=(const Worker&) = delete;
        Worker&operator=(Worker&&) = delete;

        ~Worker() {
            assert(!m_fiber);
            free(m_stack_base);
        }
    };

    valvec<Worker> m_workers;
    boost::fibers::context** m_active_pp;
    boost::fibers::scheduler* m_sched;
    int m_freehead = -1;
public:
    explicit
    RunOnceFiberPool(size_t num,
                     size_t stack_size = ReuseStack::traits_type::default_size())
    {
        using namespace boost::fibers;
        m_active_pp = context::active_pp();
        m_sched = (*m_active_pp)->get_scheduler();
        m_workers.reserve(num);
        for (size_t i = 0; i < num; ++i) {
            m_workers.unchecked_emplace_back(stack_size);
            auto& w = m_workers[i];
            w.m_next = int(i + 1);
        }
        m_workers.back().m_next = -1;
        m_freehead = 0;
    }
    ~RunOnceFiberPool() {
#if !defined(NDEBUG)
        for (auto& w : m_workers) {
            assert(!w.m_fiber);
        }
#endif
    }

    size_t capacity() const { return m_workers.size(); }

    ///@param myhead must be initialized to -1
    /// can be called multiple times with same 'myhead'
    template<class Fn, class... Arg>
    void submit(int& myhead, Fn&& fn, Arg... arg) {
        using namespace boost::fibers;
        assert(-1 == myhead || size_t(myhead) < m_workers.size());
        if (-1 != m_freehead) {
            int oldfree_idx = m_freehead;
            auto& oldfree = m_workers[oldfree_idx];
            m_freehead = oldfree.m_next;
            oldfree.m_next = myhead;
            myhead = oldfree_idx;

            //auto ctx = oldfree.setup(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
            //manually inline as below is faster

            assert(!oldfree.m_fiber);
            oldfree.m_fiber = make_worker_context(launch::post,
                          ReuseStack(oldfree.m_stack_base, oldfree.m_stack_size),
                          std::forward<Fn>(fn), std::forward<Arg>(arg)...);
            auto ctx = oldfree.m_fiber.get();

            m_sched->attach_worker_context(ctx);
            m_sched->schedule(ctx);
        }
        else {
            // run in active fiber
            fn(std::forward<Arg>(arg)...);
        }
    }

    // never run fn in calling fiber
    template<class Fn, class... Arg>
    void async(int& myhead, Fn&& fn, Arg... arg) {
        using namespace boost::fibers;
        assert(-1 == myhead || size_t(myhead) < m_workers.size());
        if (BOOST_UNLIKELY(-1 == m_freehead)) {
            yield();
            reap(myhead);
            myhead = -1;
        }
        while (BOOST_UNLIKELY(-1 == m_freehead)) {
            yield();
        }
        int oldfree_idx = m_freehead;
        auto& oldfree = m_workers[oldfree_idx];
        m_freehead = oldfree.m_next;
        oldfree.m_next = myhead;
        myhead = oldfree_idx;

        //auto ctx = oldfree.setup(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
        //manually inline as below is faster

        assert(!oldfree.m_fiber);
        oldfree.m_fiber = make_worker_context(launch::post,
                      ReuseStack(oldfree.m_stack_base, oldfree.m_stack_size),
                      std::forward<Fn>(fn), std::forward<Arg>(arg)...);
        auto ctx = oldfree.m_fiber.get();

        m_sched->attach_worker_context(ctx);
        m_sched->schedule(ctx);
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
