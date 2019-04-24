/* vim: set tabstop=4 : */
#include "concurrent_cow.inl"

namespace terark { namespace cow_mman {

static const size_t BULK_FREE_NUM = 32;

TokenList::TokenList(ConcurrentLevel conLevel) {
    if (conLevel >= MultiWriteMultiRead) {
        new(&m_lazy_free_list_tls)instance_tls<LazyFreeList>();
    }
    else {
        new(&m_lazy_free_list_sgl)LazyFreeList();
    }
    m_main = NULL;
}

TokenList::~TokenList() {
    auto conLevel = m_main->m_mempool_concurrent_level;
    if (conLevel >= MultiWriteMultiRead) {
        m_lazy_free_list_tls.~instance_tls<LazyFreeList>();
    }
    else {
        m_lazy_free_list_sgl.~LazyFreeList();
    }
}

void TokenList::mem_lazy_free(size_t loc, size_t size) {
    m_main->mem_lazy_free(loc, size, this);
}

CowMemPool::~CowMemPool() {

}

void CowMemPool::mem_lazy_free(size_t loc, size_t size, TokenList* sub) {
    auto conLevel = m_writing_concurrent_level;
    if (conLevel >= SingleThreadShared) {
        uint64_t age = sub->m_token_head.m_age++;
        sub->lazy_free_list(conLevel).push_back(
                { age, uint32_t(loc), uint32_t(size) });
    }
    else {
        mem_free(loc, size);
    }
}

size_t CowMemPool::mem_alloc(size_t size) {
    switch (m_writing_concurrent_level) {
    default:   assert(false); return mem_alloc_fail;
    case MultiWriteMultiRead: return alloc_node<MultiWriteMultiRead>(size);
    case   OneWriteMultiRead: return alloc_node<  OneWriteMultiRead>(size);
    case  SingleThreadStrict:
    case  SingleThreadShared: return alloc_node< SingleThreadShared>(size);
    case     NoWriteReadOnly: assert(false); return mem_alloc_fail;
    }
}

void CowMemPool::mem_free(size_t loc, size_t size) {
    switch (m_writing_concurrent_level) {
    default:   assert(false);                                            break;
    case MultiWriteMultiRead: free_node<MultiWriteMultiRead>(loc, size); break;
    case   OneWriteMultiRead: free_node<  OneWriteMultiRead>(loc, size); break;
    case  SingleThreadStrict:
    case  SingleThreadShared: free_node< SingleThreadShared>(loc, size); break;
    case     NoWriteReadOnly:                                            break;
    }
}

///////////////////////////////////////////////////////////////////////
TokenBase::~TokenBase() {
    // do nothing
}

ReaderToken::ReaderToken() {
    m_next  = NULL;
    m_prev  = NULL;
    m_age   = 0;
    m_main  = NULL;
    m_sub   = NULL;
    m_value = NULL;
}

ReaderToken::ReaderToken(TokenList* sub1) {
    m_main  = NULL;
    m_sub   = NULL;
    m_value = NULL;
    attach(sub1);
}

void ReaderToken::attach(TokenList* sub1) {
    assert(NULL == m_main);
    assert(NULL == m_sub);
//  assert(NULL == m_value); // dont touch m_value
    auto sub = static_cast<TokenList*>(sub1);
    CowMemPool* main = sub->m_main;
    TokenLink* head = &sub->m_token_head;
    m_main = main;
    m_sub = sub;
    if (sub->m_writing_concurrent_level < SingleThreadShared) {
        m_age  = 0;
        m_prev = NULL;
        m_next = NULL;
    }
    else {
        m_next = head;
        sub->m_token_mutex.lock();
        assert(head->m_prev->m_next == head);
        assert(head->m_next->m_prev == head);
        m_age = head->m_age;
        TokenLink* prev = m_prev = head->m_prev;
        prev->m_next = this;
        head->m_prev = this;
        sub->m_token_mutex.unlock();
    }
}

ReaderToken::~ReaderToken() {
    detach();
}

void ReaderToken::detach() {
    auto main = m_main;
    if (m_next) { // I'm in list, remove me from list
        assert(NULL != m_sub);
        auto sub = m_sub;
        auto conLevel = main->m_writing_concurrent_level;
        assert(conLevel != SingleThreadStrict);
        if (conLevel != SingleThreadShared) sub->m_token_mutex.lock();
        assert(&sub->m_token_head == sub->m_token_head.m_next->m_prev);
        assert(&sub->m_token_head == sub->m_token_head.m_prev->m_next);
        auto  prev = m_prev; assert(this == prev->m_next);
        auto  next = m_next; assert(this == next->m_prev);
        prev->m_next = next;
        next->m_prev = prev;
        if (conLevel != SingleThreadShared) sub->m_token_mutex.unlock();
        m_next  = NULL;
        m_prev  = NULL;
    }
    else {
        assert(NULL == m_prev);
    }
    m_age   = 0;
    m_sub   = NULL;
    m_main  = NULL;
    m_value = NULL;
}

void ReaderToken::update_list(ConcurrentLevel conLevel, TokenList* sub) {

//  assert(main->m_writing_concurrent_level >= OneWriteMultiRead);
//  assert may fail when another thread just called main->set_readonly()
//  but is ok to insert this token into a readonly trie
    TokenLink* head = &sub->m_token_head;
    m_value = NULL;
    // conLevel maybe NoWriteReadOnly at this time point
    if (conLevel != SingleThreadShared) sub->m_token_mutex.lock();

    assert(head == head->m_next->m_prev);
    assert(head == head->m_prev->m_next);
    auto  prev = m_prev; assert(this == prev->m_next);
    auto  next = m_next; assert(this == next->m_prev);
    prev->m_next = next;
    next->m_prev = prev;

    // insert to tail
    m_next = head;
    m_age  = head->m_age;
    prev= m_prev = head->m_prev;
    prev->m_next = this;
    head->m_prev = this;

    if (conLevel != SingleThreadShared) sub->m_token_mutex.unlock();
}

/// @returns true really updated
///         false not updated
bool ReaderToken::update(TokenUpdatePolicy updatePolicy) {
    assert(NULL != m_sub);
    assert(NULL != m_main);
    auto conLevel = m_main->m_writing_concurrent_level;
    assert(this->m_age <= m_sub->m_token_head.m_age);
    if (conLevel == NoWriteReadOnly) {
        if (m_next) { // I'm in list, remove me from list
            assert(NULL != m_prev);
            auto sub = m_sub;
            conLevel = m_main->m_mempool_concurrent_level;
            if (conLevel != SingleThreadShared) sub->m_token_mutex.lock();
            assert(&sub->m_token_head == sub->m_token_head.m_next->m_prev);
            assert(&sub->m_token_head == sub->m_token_head.m_prev->m_next);
            auto  prev = m_prev; assert(this == prev->m_next);
            auto  next = m_next; assert(this == next->m_prev);
            prev->m_next = next;
            next->m_prev = prev;
            if (conLevel != SingleThreadShared) sub->m_token_mutex.unlock();
            m_value = NULL;
            m_age  = 0;
            m_prev = NULL;
            m_next = NULL;
            return true;
        }
    }
    else if (conLevel >= SingleThreadShared) {
        auto sub = m_sub;
        if (m_age == sub->m_token_head.m_age)
            return false;
        if (UpdateLazy == updatePolicy) {
            if (m_age + BULK_FREE_NUM > sub->m_token_head.m_age)
                return false;
            if (sub->lazy_free_list(conLevel).size() < 2*BULK_FREE_NUM)
                return false;
        }
        if (m_next == &sub->m_token_head) {
            // return true may cause caller to update iterator status, thus
            // inducing too many overhead, so this should after lazy check
            // I'm tail, risky, but is ok, can be tolerated
            m_age = sub->m_token_head.m_age;
        }
        else {
            update_list(conLevel, sub);
        }
        return true;
    }
    else {
        assert(conLevel == SingleThreadStrict);
        assert(NULL == m_next);
        assert(NULL == m_prev);
    }
    return false;
}

///////////////////////////////////////////////////////////////////////

// for derived class, if concurrent level >= OneWriteMultiRead,
// init_value can return false to notify init fail, in fail case,
// CowMemPool::insert() will return true and set token.value to NULL.
bool
WriterToken::init_value(void* value, size_t valsize) {
    assert(valsize % AlignSize == 0);
    return true;
}

WriterToken::WriterToken(TokenList* sub1) {
    auto sub = static_cast<TokenList*>(sub1);
    CowMemPool* main = sub->m_main;
    assert(main->m_writing_concurrent_level != NoWriteReadOnly);
    TokenLink* head = &sub->m_token_head;
    auto conLevel = main->m_writing_concurrent_level;
    m_value = NULL;
    m_main = main;
    m_sub = sub;
    if (conLevel <= OneWriteMultiRead) {
        m_age  = 0;
        m_prev = NULL;
        m_next = NULL;
    }
    else {
        assert(MultiWriteMultiRead == conLevel);
        m_age = head->m_age;
        m_next = head;
        sub->m_token_mutex.lock();
        assert(head->m_prev->m_next == head);
        assert(head->m_next->m_prev == head);
        TokenLink* prev = m_prev = head->m_prev;
        prev->m_next = this;
        head->m_prev = this;
        sub->m_token_mutex.unlock();
    }
}

WriterToken::~WriterToken() {
    auto sub = m_sub;
    if (m_next) {
        assert(MultiWriteMultiRead == m_main->m_mempool_concurrent_level);
        sub->m_token_mutex.lock();
        assert(&sub->m_token_head == sub->m_token_head.m_next->m_prev);
        assert(&sub->m_token_head == sub->m_token_head.m_prev->m_next);
        auto  prev = m_prev; assert(this == prev->m_next);
        auto  next = m_next; assert(this == next->m_prev);
        prev->m_next = next;
        next->m_prev = prev;
        sub->m_token_mutex.unlock();
    }
    else {
        assert(NULL == m_next);
        assert(NULL == m_prev);
    }
}

void WriterToken::update_list(TokenList* sub) {
    TokenLink* head = &sub->m_token_head;
    m_value = NULL;
    sub->m_token_mutex.lock();

    assert(head == head->m_next->m_prev);
    assert(head == head->m_prev->m_next);

    auto  prev = m_prev; assert(this == prev->m_next);
    auto  next = m_next; assert(this == next->m_prev);
    prev->m_next = next;
    next->m_prev = prev;

    // insert to tail
    m_next = head;
    m_age  = head->m_age;
    prev= m_prev = head->m_prev;
    prev->m_next = this;
    head->m_prev = this;

    sub->m_token_mutex.unlock();
}

/// @returns true really updated
///         false not updated
bool WriterToken::update(TokenUpdatePolicy updatePolicy) {
    assert(m_main->m_writing_concurrent_level != NoWriteReadOnly);
    assert(this->m_age <= m_sub->m_token_head.m_age);
    if (m_main->m_writing_concurrent_level >= MultiWriteMultiRead) {
        auto sub = m_sub;
        if (m_age == sub->m_token_head.m_age)
            return false;
        if (UpdateLazy == updatePolicy) {
            if (m_age + BULK_FREE_NUM > sub->m_token_head.m_age)
                return false;
            if (sub->lazy_free_list(MultiWriteMultiRead).size() < 2*BULK_FREE_NUM)
                return false;
        }
        if (m_next == &sub->m_token_head) {
            // return true may cause caller to update iterator status, thus
            // inducing too many overhead, so this should after lazy check
            // I'm tail, risky, but is ok, can be tolerated
            m_age = sub->m_token_head.m_age;
        }
        else {
            update_list(sub);
        }
        return true;
    }
    return false;
}

}} // namespace terark::cow_mman
