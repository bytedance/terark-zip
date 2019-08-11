//
// Created by leipeng on 2019-08-08.
//
#pragma once

#include <boost/fiber/context.hpp>
#include <boost/fiber/scheduler.hpp>
//#include <boost/fiber/operations.hpp>
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
class TERARK_DLL_EXPORT RunOnceFiberPool {
public:
    class TERARK_DLL_EXPORT Worker {
        friend class RunOnceFiberPool;
        Worker* m_next = NULL;
        Worker* m_prev = NULL;
        void*  m_stack_base;
        size_t m_stack_size;
        boost::intrusive_ptr<boost::fibers::context> m_fiber;
        inline void list_append(Worker* node) {
            assert(NULL == this->m_stack_base); //  is a head
            assert(NULL != node->m_stack_base); // not a head
            auto tail = this->m_prev;
            node->m_prev = tail;
            node->m_next = this;
            this->m_prev = node;
            tail->m_next = node;
        }
        inline void list_delete() {
            assert(NULL != m_stack_base); // not a head
            auto prev = m_prev;
            auto next = m_next;
            prev->m_next = next;
            next->m_prev = prev;
        }
        inline bool is_empty_list() const {
            if (this == m_next) {
                assert(this == m_prev);
                return true;
            }
            return false;
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
    public:
        // serve as list header
        inline Worker() {
            m_next = m_prev = this;
            m_stack_base = NULL;
            m_stack_size = 0;
        }
        ~Worker();
    };
    friend class Worker;

private:
    RunOnceFiberPool(const RunOnceFiberPool&) = delete;
    RunOnceFiberPool(RunOnceFiberPool&&) = delete;
    RunOnceFiberPool&operator=(const RunOnceFiberPool&) = delete;
    RunOnceFiberPool&operator=(RunOnceFiberPool&&) = delete;

    valvec<Worker> m_workers;
    boost::fibers::context** m_active_pp;
    boost::fibers::scheduler* m_sched;
    Worker m_freehead;
    size_t m_freesize;
public:
    explicit
    RunOnceFiberPool(size_t num);
    RunOnceFiberPool(size_t num, size_t stack_size);
    ~RunOnceFiberPool();

    size_t capacity() const { return m_workers.size(); }
    size_t freesize() const { return m_freesize; }
    size_t usedsize() const { return m_workers.size() - m_freesize; }

    ///@param myhead must be initialized to -1
    /// can be called multiple times with same 'myhead'
    template<class Fn, class... Arg>
    void submit(Worker& myhead, Fn&& fn, Arg... arg) {
        assert(NULL == myhead.m_stack_base);
        using namespace boost::fibers;
        if (!m_freehead.is_empty_list()) {
            m_freesize--;
            auto w = m_freehead.m_prev; // tail
            w->list_delete();
            myhead.list_append(w);

            auto ffn = [this,fn,w](Arg... a) {
                fn(std::forward<Arg>(a)...);
                w->list_delete();
                m_freehead.list_append(w);
                m_freesize++;
            };
            //assert(!w->m_fiber || w->m_fiber->is_terminated());
            w->m_fiber = make_worker_context(launch::post,
                          ReuseStack(w->m_stack_base, w->m_stack_size),
                          std::move(ffn), std::forward<Arg>(arg)...);
            auto ctx = w->m_fiber.get();

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
    void async(Worker& myhead, Fn&& fn, Arg... arg) {
        assert(NULL == myhead.m_stack_base);
        using namespace boost::fibers;
        if (BOOST_UNLIKELY(m_freehead.is_empty_list())) {
            yield();
        }
        m_freesize--;
        auto w = m_freehead.m_prev; // tail
        w->list_delete();
        myhead.list_append(w);

        auto ffn = [this,fn,w](Arg... a) {
            fn(std::forward<Arg>(a)...);
            w->list_delete();
            m_freehead.list_append(w);
            m_freesize++;
        };
        //assert(!w->m_fiber || w->m_fiber->is_terminated());
        w->m_fiber = make_worker_context(launch::post,
                      ReuseStack(w->m_stack_base, w->m_stack_size),
                      std::move(ffn), std::forward<Arg>(arg)...);
        auto ctx = w->m_fiber.get();

        m_sched->attach_worker_context(ctx);
        m_sched->schedule(ctx);
    }

    inline void yield() {
        m_sched->yield(*m_active_pp);
        //boost::this_fiber::yield();
    }

    void reap(Worker& myhead);
};

}
