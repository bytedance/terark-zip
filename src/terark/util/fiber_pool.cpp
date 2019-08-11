//
// Created by leipeng on 2019-08-11.
//

#include "fiber_pool.hpp"

namespace terark {

RunOnceFiberPool::Worker::~Worker() {
    if (m_stack_base) {
        //assert(!m_fiber || m_fiber->is_terminated());
    } else {
        assert(is_empty_list()); // user land list head
    }
    free(m_stack_base);
}

RunOnceFiberPool::RunOnceFiberPool(size_t num)
 : RunOnceFiberPool(num, ReuseStack::traits_type::default_size())
{}

RunOnceFiberPool::RunOnceFiberPool(size_t num, size_t stack_size) {
    using namespace boost::fibers;
    m_active_pp = context::active_pp();
    m_sched = (*m_active_pp)->get_scheduler();
    m_workers.resize_no_init(num);
    for (size_t i = 0; i < num; ++i) {
        auto w = m_workers.ptr(i);
        new(w)Worker(stack_size);
        w->m_next = w + 1;
        w->m_prev = w - 1;
    }
    size_t t = num-1;
    m_workers[t].m_next = &m_freehead;
    m_workers[0].m_prev = &m_freehead;
    m_freehead.m_next = m_workers.ptr(0);
    m_freehead.m_prev = m_workers.ptr(t);
    m_freesize = num;
}

RunOnceFiberPool::~RunOnceFiberPool() {
#if !defined(NDEBUG)
    assert(m_workers.size() == m_freesize);
    assert(!m_freehead.m_fiber);
    for (size_t i = 0; i < m_workers.size(); ++i) {
        auto& w = m_workers[i];
        //assert(!w.m_fiber || w.m_fiber->is_terminated());
        assert(NULL != w.m_stack_base);
    }
    m_workers.clear();
    m_freehead.m_prev = m_freehead.m_next = &m_freehead;
#endif
}

void RunOnceFiberPool::reap(Worker& myhead) {
    while (!myhead.is_empty_list()) {
        yield();
    }
}

} // namespace terark
