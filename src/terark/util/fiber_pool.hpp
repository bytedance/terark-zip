//
// Created by leipeng on 2019-08-08.
//
#pragma once

#include <boost/context/fiber.hpp>
#include <terark/stdtypes.hpp>
#include <terark/valvec.hpp>
#include <map>

namespace terark {

template<class T>
class FiberLocalPtr;

// allowing multiple fibers running submit/reap pair
class TERARK_DLL_EXPORT RunOnceFiberPool {
    DECLARE_NONE_MOVEABLE_CLASS(RunOnceFiberPool);
    template<class> friend class FiberLocalPtr;
    class ReuseStack;
    class Schedualer;
    typedef void (*del_t)(void*);
public:
    class TERARK_DLL_EXPORT Worker {
        DECLARE_NONE_MOVEABLE_CLASS(Worker);
        template<class> friend class FiberLocalPtr;
        friend class RunOnceFiberPool;
        friend class ReuseStack;
        friend class Schedualer;
        Worker* m_next_ready;
        Worker* m_prev_ready;
        Worker* m_next;
        Worker* m_prev;
        std::map<const del_t*, void*> m_fss;
        void*   m_stack_base;
        RunOnceFiberPool* m_owner;
        boost::context::fiber m_fiber;

        size_t index() const {
            if (m_owner)
                return this - m_owner->m_workers.data();
            else
                return size_t(-1);
        }

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
        inline bool list_is_empty() const {
            if (this == m_next) {
                assert(this == m_prev);
                return true;
            }
            return false;
        }
        inline void ready_append(Worker* node) {
            assert(NULL == this->m_stack_base); //  is a head
            assert(NULL != node->m_stack_base); // not a head
            auto tail = this->m_prev_ready;
            node->m_prev_ready = tail;
            node->m_next_ready = this;
            this->m_prev_ready = node;
            tail->m_next_ready = node;
        }
        inline void ready_insert_as_next(Worker* node) {
            //assert(NULL == this->m_stack_base); //  is a head
            assert(NULL != node->m_stack_base); // not a head
            assert(NULL == node->m_next_ready);
            assert(NULL == node->m_prev_ready);
            auto next = this->m_next_ready;
            node->m_prev_ready = this;
            node->m_next_ready = next;
            next->m_prev_ready = node;
            this->m_next_ready = node;
        }
        inline void ready_delete() {
            assert(NULL != m_stack_base); // not a head
            auto prev = m_prev_ready;
            auto next = m_next_ready;
            prev->m_next_ready = next;
            next->m_prev_ready = prev;
            m_prev_ready = m_next_ready = NULL;
        }
        inline bool ready_is_empty() const {
            if (this == m_next_ready) {
                assert(this == m_prev_ready);
                return true;
            }
            return false;
        }
        inline size_t ready_calc_size() const {
            size_t n = 0;
            auto p = m_next_ready;
            while (p != this) {
                n++;
                p = p->m_next_ready;
            }
            return n;
        }
        Worker(RunOnceFiberPool*);
    public:
        // serve as list header
        inline Worker() {
            m_next_ready = m_prev_ready = NULL;
            m_next = m_prev = this;
            m_stack_base = NULL;
            m_owner = NULL;
        }
        ~Worker();
    };
    friend class Worker;

private:
    class ReuseStack {
        Worker* m_worker;
    public:
        typedef boost::context::stack_traits  traits_type;
        typedef boost::context::stack_context stack_context;
        ReuseStack(Worker* w) noexcept : m_worker(w) {}
        terark_forceinline stack_context allocate() noexcept {
            assert(NULL != m_worker);
            void * vp = m_worker->m_stack_base;
            stack_context sctx;
            sctx.size = m_worker->m_owner->m_stack_size;
            sctx.sp = static_cast<char*>(vp) + sctx.size;
    #if defined(BOOST_USE_VALGRIND)
            sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER(sctx.sp, vp);
    #endif
            return sctx;
        }
        terark_forceinline void deallocate(stack_context& sctx) noexcept {
            BOOST_ASSERT( sctx.sp);

    #if defined(BOOST_USE_VALGRIND)
            VALGRIND_STACK_DEREGISTER(sctx.valgrind_stack_id);
    #endif
#if !defined(NDEBUG)
            void * vp = static_cast<char*>(sctx.sp) - sctx.size;
            assert(vp == m_worker->m_stack_base);
#endif
        }
    };

    valvec<Worker> m_workers;
    //boost::fibers::context** m_active_pp;
    //boost::fibers::scheduler* m_sched;
    Schedualer* m_sched;
    Worker m_freehead;
    size_t m_freesize;
    size_t m_stack_size;

    class Schedualer {
        DECLARE_NONE_MOVEABLE_CLASS(Schedualer);
    public:
        Worker m_head;
        size_t m_ready_size;
        size_t m_yield_cnt;
        Schedualer();
        ~Schedualer();
        static Schedualer* get_sched();
        static void kill_fss(const del_t* fss);
        static void** get_fss(const del_t*);
        void run_one_round();
        void join();
        void set_active(Worker* w) { m_head.m_next = w; }
        Worker* get_active() const { return m_head.m_next; }
    };
public:
    explicit
    RunOnceFiberPool(size_t num);
    RunOnceFiberPool(size_t num, size_t stack_size);
    ~RunOnceFiberPool();

    size_t capacity() const noexcept { return m_workers.size(); }
    size_t freesize() const noexcept { return m_freesize; }
    size_t usedsize() const noexcept { return m_workers.size() - m_freesize; }
    size_t yield_cnt() const noexcept { return m_sched->m_yield_cnt; }

    ///@param myhead must be initialized to -1
    /// can be called multiple times with same 'myhead'
    // always run fn in callee  fiber
    // never  run fn in calling fiber
    template<class Fn, class... Arg>
    void submit(Worker& myhead, Fn&& fn, Arg... arg) {
        assert(NULL == myhead.m_stack_base);
        while (BOOST_UNLIKELY(m_freehead.list_is_empty())) {
            m_sched->run_one_round();
        }
        using boost::context::fiber;
        const auto w = m_freehead.m_prev; // tail is more recent, cache hotter
        assert(!w->m_fiber);
        m_freesize--;
        w->list_delete();
        myhead.list_append(w);
        const auto old_active = m_sched->get_active();
        assert(old_active != w);
        assert(!old_active->m_fiber);
        m_sched->m_ready_size++;
        old_active->ready_insert_as_next(w);
        m_sched->set_active(w);
        auto fib = fiber(std::allocator_arg, ReuseStack(w),
        [&myhead,this,w,old_active,fn,arg...](boost::context::fiber&& c) {
            assert(!w->m_fiber);
            old_active->m_fiber = std::move(c);
            assert(m_sched->get_active() == w);
            assert(!w->m_fiber);
            assert(!w->ready_is_empty());
            assert(w->m_prev_ready == old_active);
            printf("fiber_enter: workers = %zd, freesize = %zd, w.idx = %zd, o.idx = %zd, n.idx = %zd, ready_size = %zd, yield_cnt = %zd, fiber-living: ",
                    m_workers.size(), m_freesize, w->index(), old_active->index(), w->m_next_ready->index(), m_sched->m_ready_size, yield_cnt());
            fn(arg...);
            assert(m_sched->get_active() == w);
            assert(!w->m_fiber);
            const auto wnext = w->m_next_ready;
            printf("fiber_leave: workers = %zd, freesize = %zd, w.idx = %zd, o.idx = %zd, n.idx = %zd, ready_size = %zd, yield_cnt = %zd, fiber-living: ",
                    m_workers.size(), m_freesize, w->index(), old_active->index(), wnext->index(), m_sched->m_ready_size, yield_cnt());
            for (size_t i = 0; i < m_workers.size(); ++i) {
                bool ismy_worker = false;
                for (Worker* p = myhead.m_next; p != &myhead; p = p->m_next) {
                    if (m_workers.ptr(i) == p) {
                        ismy_worker = true;
                        break;
                    }
                }
                if (ismy_worker)
                    printf("[%zd] = %d, ", i, bool(m_workers[i].m_fiber));
            }
            printf("\n");
            assert(wnext->m_fiber);
            assert(w->m_prev_ready->m_fiber);
            assert(m_sched->m_ready_size > 0);
            assert(!m_sched->m_head.ready_is_empty());
            assert(!w->ready_is_empty());
            assert(w != &m_sched->m_head); // not main context

            auto wprev = w->m_prev_ready;
            m_sched->set_active(wprev);
            assert(wprev->m_fiber);

            w->list_delete();
            w->ready_delete();
            m_freehead.list_append(w);
            m_freesize++;
            m_sched->m_ready_size--;
            return std::move(wprev->m_fiber);
        });
        std::move(fib).resume();
        assert(m_sched->get_active() == old_active);
        m_sched->m_yield_cnt++;
    }

    terark_forceinline void yield() { m_sched->run_one_round(); }

    void reap(Worker& myhead);
};

template<class T>
class FiberLocalPtr {
    DECLARE_NONE_MOVEABLE_CLASS(FiberLocalPtr);
    void (*m_del)(T*);
public:
    typedef T   element_type;
    explicit
    FiberLocalPtr(void(*fn)(T*)) : m_del(fn) {}
    FiberLocalPtr() : m_del([](T* x){delete x;}) {}
    ~FiberLocalPtr() {
        auto pdel = reinterpret_cast<RunOnceFiberPool::del_t*>(&m_del);
        RunOnceFiberPool::Schedualer::kill_fss(pdel);
    }
    T** get_pp() const noexcept {
        auto pdel = reinterpret_cast<const RunOnceFiberPool::del_t*>(&m_del);
        return reinterpret_cast<T**>(RunOnceFiberPool::Schedualer::get_fss(pdel));
    }
    T* get() const noexcept { return *get_pp(); }
    T* operator->() const noexcept { T* p = get(); assert(p); return  p;}
    T& operator*() const noexcept  { T* p = get(); assert(p); return *p;}
    T* release() {
        T** pp = get_pp();
        T* ret = *pp;
        *pp = NULL;
        return ret;
    }
    void reset(T* p) {
        T** pp = get_pp();
        if (*pp) {
            m_del(*pp);
        }
        *pp = p;
    }
};


}
