//
// Created by leipeng on 2019-08-11.
//

#include "fiber_pool.hpp"
#include <boost/fiber/operations.hpp>

namespace terark {

RunOnceFiberPool::Worker::Worker(RunOnceFiberPool* o) {
    m_owner = o;
    m_stack_base = malloc(o->m_stack_size);
    if (!m_stack_base) {
        std::terminate();
    }
    m_next_ready = m_prev_ready = NULL;
}

RunOnceFiberPool::Worker::~Worker() {
    if (NULL == m_stack_base) {
        assert(list_is_empty()); // user land list head
    }
    free(m_stack_base);
}

RunOnceFiberPool::RunOnceFiberPool(size_t num)
 : RunOnceFiberPool(num, ReuseStack::traits_type::default_size())
{}

RunOnceFiberPool::RunOnceFiberPool(size_t num, size_t stack_size) {
    using namespace boost::fibers;
    m_sched = Schedualer::get_sched();
    m_workers.resize_no_init(num);
    for (size_t i = 0; i < num; ++i) {
        auto w = m_workers.ptr(i);
        new(w)Worker(this);
        w->m_next = w + 1;
        w->m_prev = w - 1;
    }
    size_t t = num-1;
    m_workers[t].m_next = &m_freehead;
    m_workers[0].m_prev = &m_freehead;
    m_freehead.m_next = m_workers.ptr(0);
    m_freehead.m_prev = m_workers.ptr(t);
    m_freesize = num;
    m_stack_size = stack_size;
}

RunOnceFiberPool::~RunOnceFiberPool() {
    assert(m_workers.size() == m_freesize);
    assert(!m_freehead.m_fiber);
    assert(!m_freehead.m_stack_base);
    for (size_t i = 0; i < m_workers.size(); ++i) {
        auto& w = m_workers[i];
        assert(!w.m_fiber);
        assert(NULL != w.m_stack_base);
    }
    m_workers.clear();
    m_freehead.m_prev = m_freehead.m_next = &m_freehead;
}

void RunOnceFiberPool::reap(Worker& myhead) {
    while (!myhead.list_is_empty()) {
        yield();
    }
    size_t my_ready = 0;
    for (Worker* p = m_sched->m_head.m_next_ready; p != &m_sched->m_head; ) {
        if (p >= m_workers.begin() && p < m_workers.end()) {
            my_ready++;
        }
        p = p->m_next_ready;
    }
    printf("fiber_reap: workers = %zd, freesize = %zd, my_ready = %zd, ready_size = %zd, ready_list = %zd\n",
            m_workers.size(), m_freesize, my_ready, m_sched->m_ready_size, m_sched->m_head.ready_calc_size());
}

RunOnceFiberPool::Schedualer::Schedualer() {
    //m_head.m_fiber will be main fiber
    m_head.m_next_ready = m_head.m_prev_ready = &m_head;
    m_ready_size = 0; // not include main fiber
    m_yield_cnt = 0;
//  set_active(&m_head);
    assert(get_active() == &m_head);
}

RunOnceFiberPool::Schedualer::~Schedualer() {
    assert(!m_head.m_fiber);
    assert(!m_ready_size);
    assert(m_head.ready_is_empty());
}

RunOnceFiberPool::Schedualer*
RunOnceFiberPool::Schedualer::get_sched() {
    static thread_local Schedualer singleton;
    return &singleton;
}

void RunOnceFiberPool::Schedualer::run_one_round() {
    using namespace boost::context;
    assert(m_ready_size > 0);
    assert(!m_head.ready_is_empty());
    m_yield_cnt++;
    auto old_active = get_active();
    auto new_active = old_active->m_next_ready;
    assert(!old_active->m_fiber);
    set_active(new_active);
    assert(new_active->m_fiber);
    printf("run_one_round: o.idx = %zd, n.idx = %zd, yield_cnt = %zd\n", old_active->index(), new_active->index(), m_yield_cnt);
//  auto target = std::move(new_active->m_fiber).resume();
/*
    auto target = std::move(new_active->m_fiber).resume_with(
            [old_active](fiber&& c) {
       old_active->m_fiber = std::move(c);
       return fiber{}; //std::move(new_active->m_next_ready->m_fiber);
    });
*/
    assert(!old_active->m_next_ready->m_fiber);
    assert(get_active() == old_active);
    old_active->m_next_ready->m_fiber = std::move(target);
}

void RunOnceFiberPool::Schedualer::join() {
    while (&m_head != m_head.m_next_ready) {
        run_one_round();
    }
}

void RunOnceFiberPool::Schedualer::kill_fss(const del_t* pdel) {
    auto schd = get_sched();
    Worker* curr = schd->m_head.m_next_ready;
    del_t del = *pdel;
    while (&schd->m_head != curr) {
        auto iter = curr->m_fss.find(pdel);
        if (curr->m_fss.end() != iter) {
            if (NULL != iter->second) {
                del(iter->second);
            }
            curr->m_fss.erase(iter);
        }
        curr = curr->m_next_ready;
    }
}

void** RunOnceFiberPool::Schedualer::get_fss(const del_t* del) {
    auto schd = get_sched();
    auto active = schd->get_active();
    auto ib = active->m_fss.emplace(del, (void*)NULL);
    return &ib.first->second;
}

} // namespace terark
