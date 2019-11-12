#pragma once
#include "instance_tls.hpp"

namespace terark {

///! TlsMember is a instance_tls member of Owner
///! requires TlsMember*    TlsMember::m_next_free
///! requires const Owner0* TlsMember::tls_owner()
///!                Owner0 should be Owner or some base class of Owner
template<class Owner, class TlsMember>
class instance_tls_owner {
protected:
    struct TlsPtr : boost::noncopyable {
        TlsMember* ptr;
        TlsPtr() : ptr(NULL) {}
        ~TlsPtr() {
            TlsMember* t = as_atomic(ptr).load(std::memory_order_relaxed);
            if (nullptr == t)
                return;
            if (!as_atomic(ptr).compare_exchange_strong(t, nullptr))
                return;
            auto owner0 = t->tls_owner();
            auto owner = static_cast<Owner*>(owner0);
            assert(NULL != owner0);
            assert(dynamic_cast<Owner*>(owner0) != NULL);
            if (!owner->m_is_dying) { // is called by thread die
                // put into free list, to be reused by another thread
                if (owner->reuse(t)) {
                    push_head(owner, t);
                }
                else {
                    delete t;
                }
            }
        }
        void push_head(Owner* owner, TlsMember* t) {
        #if 1
            owner->m_free_cnt++;
            auto old_head = as_atomic(owner->m_first_free).load(std::memory_order_relaxed);
            do {
                t->m_next_free = old_head;
            } while (!as_atomic(owner->m_first_free).compare_exchange_weak(
                         old_head, t,
                         std::memory_order_release,
                         std::memory_order_relaxed));
        #else
            owner->m_tls_mtx.lock();
            t->m_next_free = owner->m_first_free;
            owner->m_first_free = t;
            owner->m_free_cnt++;
            owner->m_tls_mtx.unlock();
        #endif
        }
    };
    friend struct TlsPtr;
    friend TlsMember;
    mutable std::mutex m_tls_mtx;
    mutable TlsMember* m_first_free = NULL;
    mutable size_t     m_free_cnt;
    mutable valvec<std::unique_ptr<TlsMember> > m_tls_vec;
    union {
        // to manually control the life time, because:
        // m_is_dying and ~TlsPtr() has strict time sequence dependency
        instance_tls<TlsPtr> m_tls_ptr;
    };
    bool m_is_dying = false;
    bool m_is_fixed_cap = false;

    TlsMember* fill_tls(TlsPtr& tls) const {
        return fill_tls(tls, [this]() {
            return static_cast<const Owner*>(this)->create_tls_obj();
        });
    }
    template<class NewTLS>
    TlsMember* fill_tls(TlsPtr& tls, NewTLS New) const {
        if (m_first_free) {
            std::lock_guard<std::mutex> lock(m_tls_mtx);
            if (m_first_free) { // reuse from free list
                TlsMember* pto = m_first_free;
                m_first_free = pto->m_next_free;
                assert(m_free_cnt > 0);
                m_free_cnt--;
                tls.ptr = pto;
                return pto;
            }
        }
        // safe to do not use unique_ptr
        TlsMember* pto = New();
        if (nullptr == pto) {
            return nullptr;
        }
        if (m_is_fixed_cap) {
            m_tls_mtx.lock(); // safe to do not use RAII lock_guard
            if (terark_likely(m_tls_vec.size() < m_tls_vec.capacity())) {
                m_tls_vec.unchecked_emplace_back(pto); // should not fail
            } else {
                m_tls_mtx.unlock();
                delete pto;
                return NULL;
            }
            m_tls_mtx.unlock();
        }
        else {
            m_tls_mtx.lock(); // safe to do not use RAII lock_guard
            m_tls_vec.emplace_back(pto); // should not fail
            m_tls_mtx.unlock();
        }
        tls.ptr = pto;
        return pto;
    }

private:
    template<class Func>
    bool for_each_tls_aux(Func fun, bool*) const {
        if (m_is_fixed_cap) { // do not need lock
            auto vec = m_tls_vec.data();
            auto len = m_tls_vec.size();
            for (size_t i = 0; i < len; ++i) {
                if (fun(vec[i].get()))
                    return true;
            }
            return false;
        }
        else {
            const static size_t dim = 32;
            TlsMember* vec[dim];
            size_t i = 0;
            for (;;) {
                m_tls_mtx.lock(); // safe to do not use RAII lock_guard
                size_t n = m_tls_vec.size(), m = std::min(n-i, dim);
                auto src = m_tls_vec.data() + i;
                for (size_t j = 0; j < m; ++j) {
                    vec[j] = src[j].get(); assert(NULL != vec[i]);
                }
                m_tls_mtx.unlock();
                for (size_t j = 0; j < m; ++j) {
                    if (fun(vec[j]))
                        return true;
                }
                i += m;
                if (i >= n)
                    return false;
            }
        }
    }
    template<class Func>
    void for_each_tls_aux(Func fun, void*) const {
        if (m_is_fixed_cap) { // do not need lock
            auto vec = m_tls_vec.data();
            auto len = m_tls_vec.size();
            for (size_t i = 0; i < len; ++i) {
                fun(vec[i].get());
            }
        }
        else {
            const static size_t dim = 32;
            TlsMember* vec[dim];
            size_t i = 0;
            for (;;) {
                m_tls_mtx.lock(); // safe to do not use RAII lock_guard
                size_t n = m_tls_vec.size(), m = std::min(n-i, dim);
                auto src = m_tls_vec.data() + i;
                for (size_t j = 0; j < m; ++j) {
                    vec[j] = src[j].get(); assert(NULL != vec[i]);
                }
                m_tls_mtx.unlock();
                for (size_t j = 0; j < m; ++j) {
                    fun(vec[j]);
                }
                i += m;
                if (i >= n)
                    return;
            }
        }
    }

public:
    instance_tls_owner() {
        new(&m_tls_ptr)instance_tls<TlsPtr>(); // explicit construct
        // This class must be inherited as last base class
        assert(size_t(static_cast<Owner*>(this)) <= size_t(this));
        m_free_cnt = 0;
    }
    ~instance_tls_owner() {
        m_is_dying = true; // must before m_tls_ptr.~instance_tls()
        m_tls_ptr.~instance_tls(); // explicit destruct
    }
    inline bool reuse(TlsMember* /*tls*/) { return true; }
    void init_fixed_cap(size_t cap) {
        assert(cap > 0);
        assert(m_tls_vec.size() == 0);
        m_is_fixed_cap = true;
        m_tls_vec.reserve(cap);
    }
    void view_tls_vec(valvec<TlsMember*>* tlsVec) const {
        tlsVec->ensure_capacity(m_tls_vec.capacity());
        m_tls_mtx.lock(); // safe to do not use RAII lock_guard
        size_t n = m_tls_vec.size();
        tlsVec->resize_no_init(n); // should not fail
        auto dst = tlsVec->data();
        auto src = m_tls_vec.data();
        for (size_t i = 0; i < n; ++i) {
            dst[i] = src[i].get();
        }
        m_tls_mtx.unlock();
    }
    template<class Func>
    auto for_each_tls(Func fun) const -> decltype(fun((TlsMember*)NULL)) {
        decltype(fun((TlsMember*)NULL))* dispatch = NULL;
        return for_each_tls_aux(fun, dispatch);
    }
    TlsMember* get_tls() const {
        TlsPtr& tls = m_tls_ptr.get();
        if (tls.ptr)
            return tls.ptr;
        else
            return fill_tls(tls);
    }
    template<class NewTLS>
    TlsMember* get_tls(NewTLS New) const {
        TlsPtr& tls = m_tls_ptr.get();
        if (tls.ptr)
            return tls.ptr;
        else
            return fill_tls(tls, New);
    }
    template<class NewTLS>
    TlsMember* get_tls(NewTLS* New) const {
        return get_tls<NewTLS&>(*New);
    }
};

template<class TlsMember>
struct instance_tls_add_next_free {
    using TlsMember::TlsMember;
    instance_tls_add_next_free* m_next_free = NULL;
};
template<class Owner, class TlsMember>
struct instance_tls_add_links {
    using TlsMember::TlsMember;
    instance_tls_add_links* m_next_free = NULL;
    Owner* m_tls_owner = NULL;
    Owner* tls_owner() const { return m_tls_owner; }
    friend void
    instance_tls_set_owner(instance_tls_add_links* t, Owner* o) {
        t->m_tls_owner = o;
    }
};
inline void instance_tls_set_owner(...) {}

///! this template class is a short cut for:
///!   if there is a constructor TlsMember::TlsMember(Owner*);
template<class Owner, class TlsMember>
class easy_instance_tls_owner : public instance_tls_owner<Owner, TlsMember> {
public:
    TlsMember* create_tls_obj() const {
        auto t = new TlsMember(this);
        instance_tls_set_owner(t, this);
        return t;
    }
};

/*
// always be linear dependency, sample usage:
 struct MyReaderToken : Patricia::ReaderToken {
    MainPatricia* tls_owner() const { return main(); }
    MyReaderToken* m_next_free = NULL;
    using Patricia::ReaderToken::ReaderToken;
    //...
 };
 class PatriciaMemTableRep; // forward declare
 class MyPatricia :
    public MainPatricia,
    public instance_tls_owner<MyPatricia, MyReaderToken>
 {
    MemTableRep* m_memtab;
 public:
    MemTableRep* tls_owner() const { return m_memtab; }
    MyPatricia* m_next_free = NULL;
    MyReaderToken* create_tls_obj() const { return new MyReaderToken(this); }
    //...
 }; //
 class PatriciaMemTableRep :
    public MemTableRep,
    public instance_tls_owner<PatriciaMemTableRep, MyPatricia>
 {
 public:
    MyPatricia* create_tls_obj() const { return new MyPatricia(this); }
 }; //

 // or more simpler:
 class MyPatricia :
    public MainPatricia,
    public easy_instance_tls_owner<MyPatricia, 
                instance_tls_add_links<Patricia::ReaderToken> >
 {
 public:
    //...
 }; //
 class PatriciaMemTableRep :
    public MemTableRep,
    public easy_instance_tls_owner<PatriciaMemTableRep,
                instance_tls_add_links<MyPatricia> >
 {
 }; //

 */

} // namespace terark
