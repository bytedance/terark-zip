#pragma once
#include "valvec.hpp"
#include <terark/util/atomic.hpp>
#include <terark/util/function.hpp>
#include <terark/thread/instance_tls_owner.hpp>
#include <stdexcept>
#include <boost/integer/static_log2.hpp>
#include <boost/mpl/if.hpp>
#include <mutex>

namespace terark {

#define TERARK_MPTC_USE_SKIPLIST

template<int AlignSize> class TCMemPoolTlsHolder; // forward declare
template<int AlignSize> class ThreadCacheMemPool; // forward declare
template<int AlignSize>
class TCMemPoolOneThread : boost::noncopyable {
public:
    BOOST_STATIC_ASSERT((AlignSize & (AlignSize-1)) == 0);
    BOOST_STATIC_ASSERT(AlignSize >= 4);
    typedef typename boost::mpl::if_c<AlignSize == 4, uint32_t, uint64_t>::type link_size_t;

    static const size_t skip_list_level_max = 8;    // data io depend on this, don't modify this value
    static const size_t list_tail = ~link_size_t(0);
    static const size_t offset_shift = boost::static_log2<AlignSize>::value;

    typedef link_size_t link_t;
    struct huge_link_t {
        link_size_t size;
        link_size_t next[skip_list_level_max];
    };
    struct head_t {
        head_t() : head(list_tail), cnt(0) {}
        link_size_t head;
        link_size_t cnt;
    };
    size_t         fragment_size;
    intptr_t       m_frag_inc;
    huge_link_t    huge_list; // huge_list.size is max height of skiplist
    valvec<head_t> m_freelist_head;
    size_t  huge_size_sum;
    size_t  huge_node_cnt;
    ThreadCacheMemPool<AlignSize>* m_mempool;
    TCMemPoolOneThread* m_next_free;
    size_t  m_hot_end; // only be accessed by tls
    size_t  m_hot_pos; // only be accessed by tls, frequently changing

    explicit TCMemPoolOneThread(ThreadCacheMemPool<AlignSize>* mp) {
        m_mempool = mp;
        m_next_free = nullptr;
        m_freelist_head.resize(mp->m_fastbin_max_size / AlignSize);
        fragment_size = 0;
        m_frag_inc = 0;
        huge_size_sum = 0;
        huge_node_cnt = 0;
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
        m_hot_pos = 0;
        m_hot_end = 0;
    }
    virtual ~TCMemPoolOneThread() {
    }

    TCMemPoolTlsHolder<AlignSize>* tls_owner() const;

#if defined(__GNUC__)
    unsigned int m_rand_seed = 1;
    unsigned int rand() { return rand_r(&m_rand_seed); }
#endif
    size_t random_level() {
        size_t level = 1;
        while (rand() % 4 == 0 && level < skip_list_level_max)
            ++level;
        return level - 1;
    }
#if defined(NDEBUG)
    #define mptc1t_debug_fill_alloc(mem, len)
    #define mptc1t_debug_fill_free(mem, len)
#else
    void mptc1t_debug_fill_alloc(void* /*mem*/, size_t /*len*/) {
        //memset(mem, 0xCC, len);
    }
    void mptc1t_debug_fill_free(void* /*mem*/, size_t /*len*/) {
        //memset(mem, 0xDD, len);
    }
#endif

    terark_no_inline
    size_t alloc(byte_t* base, size_t request) {
        assert(request % AlignSize == 0);
        if (request <= m_freelist_head.size() * AlignSize) {
            size_t idx = request / AlignSize - 1;
            auto& list = m_freelist_head[idx];
            size_t next = list.head;
            if (list_tail != next) {
                size_t pos = size_t(next) * AlignSize;
                fragment_size -= request;
                m_frag_inc -= request;
                if (m_frag_inc < -256 * 1024) {
                    as_atomic(m_mempool->fragment_size).
                        fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                    m_frag_inc = 0;
                }
                list.cnt--;
                list.head = *(link_size_t*)(base + pos);
                mptc1t_debug_fill_alloc(base + pos, request);
                return pos;
            }
            else {
                // try 2x size in freelist
                size_t idx2 = idx * 2 + 1;
                if (idx2 < m_freelist_head.size()) {
                    auto& list2 = m_freelist_head[idx2];
                    next = list2.head;
                    if (list_tail != next) {
                        size_t pos = size_t(next) * AlignSize;
                        fragment_size -= request;
                        m_frag_inc -= request;
                        if (m_frag_inc < -256 * 1024) {
                            as_atomic(m_mempool->fragment_size).
                                fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                            m_frag_inc = 0;
                        }
                        list2.cnt--;
                        list2.head = *(link_size_t*)(base + pos);
                        // put remain half to 'list'...
                        // list.head must be list_tail
                    //  *(link_size_t*)(base + pos + request) = list.head;
                        *(link_size_t*)(base + pos + request) = list_tail;
                        list.cnt++;
                        list.head = (pos + request) / AlignSize;
                        mptc1t_debug_fill_alloc(base + pos, request);
                        return pos;
                    }
                }
            }
            {
                assert(m_hot_pos <= m_hot_end);
                assert(m_hot_end <= m_mempool->size());
                size_t pos = m_hot_pos;
                size_t End = pos + request;
                if (End <= m_hot_end) {
                    m_hot_pos = End;
                    mptc1t_debug_fill_alloc(base + pos, request);
                    return pos;
                }
            }
#if defined(TERARK_MPTC_USE_SKIPLIST)
            // set huge_list largest block as {m_hot_pos,m_hot_end} if exists
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n2 = &huge_list;
            if (huge_list.size) {
                size_t k = huge_list.size - 1;
                huge_link_t* n1 = nullptr;
                while (true) {
                    while (n2->next[k] != list_tail) {
                      n1 = n2;
                      n2 = (huge_link_t*)(base + (size_t(n2->next[k]) << offset_shift));
                    }
                    update[k] = n1;
                    if (k-- > 0)
                        n2 = n1;
                    else
                        break;
                }
            }
            if (n2->size >= request) {
                size_t rlen = n2->size;
                size_t res = size_t((byte*)n2 - base);
                size_t res_shift = res >> offset_shift;
                for (size_t k = 0; k < huge_list.size; ++k) {
                    if (update[k]->next[k] == res_shift)
                        update[k]->next[k] = n2->next[k];
                }
                while (huge_list.next[huge_list.size - 1] == list_tail && --huge_list.size > 0)
                    ;
                if (m_hot_pos < m_hot_end) {
                    sfree(base, m_hot_pos, m_hot_end - m_hot_pos);
                }
                m_hot_pos = res + request;
                m_hot_end = res + rlen;
                huge_size_sum -= rlen;
                huge_node_cnt -= 1;
                fragment_size -= rlen;
                m_frag_inc -= rlen;
                if (m_frag_inc < -256 * 1024) {
                    as_atomic(m_mempool->fragment_size).
                        fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                    m_frag_inc = 0;
                }
                mptc1t_debug_fill_alloc(base + res, request);
                return res;
            }
#else
            if (list_tail != size_t(huge_list.next[0])) {
                size_t res = size_t(huge_list.next[0]) << offset_shift;
                size_t rlen = ((huge_link_t*)(base + res))->size;
                if (rlen >= request) {
                    huge_list.next[0] = ((huge_link_t*)(base + res))->next[0];
                    if (m_hot_pos < m_hot_end) {
                        sfree(base, m_hot_pos, m_hot_end - m_hot_pos);
                    }
                    m_hot_pos = res + request;
                    m_hot_end = res + rlen;
                    huge_size_sum -= rlen;
                    huge_node_cnt -= 1;
                    fragment_size -= rlen;
                    m_frag_inc -= rlen;
                    if (m_frag_inc < -256 * 1024) {
                        as_atomic(m_mempool->fragment_size).
                            fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                        m_frag_inc = 0;
                    }
                    mptc1t_debug_fill_alloc(base + res, request);
                    return res;
                }
            }
#endif
        }
        else {
            assert(request >= sizeof(huge_link_t));
#if defined(TERARK_MPTC_USE_SKIPLIST)
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2 = nullptr;
            for (size_t k = huge_list.size; k > 0; ) {
                k--;
                while (n1->next[k] != list_tail && (n2 = ((huge_link_t*)(base + (size_t(n1->next[k]) << offset_shift))))->size < request)
                    n1 = n2;
                update[k] = n1;
            }
            if (n2 != nullptr && n2->size >= request) {
                assert((byte*)n2 >= base);
                size_t remain = n2->size - request;
                size_t res = size_t((byte*)n2 - base);
                size_t res_shift = res >> offset_shift;
                for (size_t k = 0; k < huge_list.size; ++k)
                    if ((n1 = update[k])->next[k] == res_shift)
                        n1->next[k] = n2->next[k];
                while (huge_list.next[huge_list.size - 1] == list_tail && --huge_list.size > 0)
                    ;
                if (remain)
                    sfree(base, res + request, remain);
                huge_size_sum -= request;
                huge_node_cnt--;
                fragment_size -= request;
                m_frag_inc -= request;
                if (m_frag_inc < -256 * 1024) {
                    as_atomic(m_mempool->fragment_size).
                        fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                    m_frag_inc = 0;
                }
                mptc1t_debug_fill_alloc(base + res, request);
                return res;
            }
#else
            if (list_tail != size_t(huge_list.next[0])) {
                size_t res = size_t(huge_list.next[0]) << offset_shift;
                size_t rlen = ((huge_link_t*)(base + res))->size;
                if (rlen >= request) {
                    huge_list.next[0] = ((huge_link_t*)(base + res))->next[0];
                    huge_size_sum -= rlen;
                    huge_node_cnt -= 1;
                    fragment_size -= rlen;
                    m_frag_inc -= rlen;
                    if (m_frag_inc < -256 * 1024) {
                        as_atomic(m_mempool->fragment_size).
                            fetch_sub(size_t(-m_frag_inc), std::memory_order_relaxed);
                        m_frag_inc = 0;
                    }
                    if (rlen > request) {
                        sfree(base, res + request, rlen - request);
                    }
                    mptc1t_debug_fill_alloc(base + res, request);
                    return res;
                }
            }
#endif
            assert(m_hot_pos <= m_hot_end);
            assert(m_hot_end <= m_mempool->size());
            size_t pos = m_hot_pos;
            size_t End = pos + request;
            if (End <= m_hot_end) {
                m_hot_pos = End;
                mptc1t_debug_fill_alloc(base + pos, request);
                return pos;
            }
        }
        return size_t(-1); // fail
    }

    size_t alloc3(byte_t* base, size_t oldpos, size_t oldlen, size_t newlen) {
        assert(oldpos % AlignSize == 0);
        assert(oldlen % AlignSize == 0);
        assert(newlen % AlignSize == 0);
        if (oldpos + oldlen == m_hot_pos) {
            size_t newend = oldpos + newlen;
            if (newend <= m_hot_end) {
                m_hot_pos = newend;
                return oldpos;
            }
        }
        if (newlen < oldlen) {
            assert(oldlen - newlen >= sizeof(link_t));
            assert(oldlen - newlen >= AlignSize);
            sfree(base, oldpos + newlen, oldlen - newlen);
            return oldpos;
        }
        else if (newlen == oldlen) {
            // do nothing
            return oldpos;
        }
        else {
            size_t newpos = alloc(base, newlen);
            if (size_t(-1) != newpos) {
                memcpy(base + newpos, base + oldpos, std::min(oldlen, newlen));
                sfree(base, oldpos, oldlen);
            }
            return newpos;
        }
    }

    terark_no_inline
    void sfree(byte_t* base, size_t pos, size_t len) {
        assert(pos % AlignSize == 0);
        assert(len % AlignSize == 0);
        assert(len > 0);
        if (pos + len == m_hot_pos) {
            m_hot_pos = pos;
            return;
        }
        if (len <= m_freelist_head.size() * AlignSize) {
            size_t idx = len / AlignSize - 1;
            auto& list = m_freelist_head[idx];
            mptc1t_debug_fill_free((link_t*)(base + pos) + 1, len-sizeof(link_t));
            *(link_size_t*)(base + pos) = list.head;
            list.head = link_size_t(pos / AlignSize);
            list.cnt++;
        }
        else {
            assert(len >= sizeof(huge_link_t));
#if defined(TERARK_MPTC_USE_SKIPLIST)
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2;
            size_t rand_lev = random_level();
            size_t k = huge_list.size;
            while (k-- > 0) {
                while (n1->next[k] != list_tail && (n2 = ((huge_link_t*)(base + (size_t(n1->next[k]) << offset_shift))))->size < len)
                    n1 = n2;
                update[k] = n1;
            }
            if (rand_lev >= huge_list.size) {
                k = huge_list.size++;
                update[k] = &huge_list;
            }
            else {
                k = rand_lev;
            }
            n2 = (huge_link_t*)(base + pos);
            size_t pos_shift = pos >> offset_shift;
            do {
                n1 = update[k];
                n2->next[k] = n1->next[k];
                n1->next[k] = pos_shift;
            } while(k-- > 0);
            n2->size = len;
#else
            huge_link_t* n2 = (huge_link_t*)(base + pos);
            n2->size = link_size_t(len);
            n2->next[0] = huge_list.next[0];
            huge_list.next[0] = link_size_t(pos >> offset_shift);
#endif
            mptc1t_debug_fill_free(n2 + 1, len - sizeof(*n2));
            huge_size_sum += len;
            huge_node_cnt++;
        }
        fragment_size += len;
        m_frag_inc += len;
        if (m_frag_inc > 256 * 1024) {
            as_atomic(m_mempool->fragment_size).
                fetch_add(size_t(m_frag_inc), std::memory_order_relaxed);
            m_frag_inc = 0;
        }
    }

    void set_hot_area(byte_t* base, size_t pos, size_t len) {
        if (m_hot_end == pos) {
            // do not need to change m_hot_pos
            m_hot_end = pos + len;
        }
        else {
            if (m_hot_pos < m_hot_end) {
                size_t large_len = m_hot_end - m_hot_pos;
                sfree(base, m_hot_pos, large_len);
            }
            else {
                // do nothing
                TERARK_IF_MSVC(TERARK_IF_DEBUG(int debug1 = 0,),);
            }
            m_hot_pos = pos;
            m_hot_end = pos + len;
        }
    }

    void populate_hot_area(byte_t* base, size_t pageSize) {
        for (size_t pos = m_hot_pos; pos < m_hot_end; pos += pageSize) {
            base[pos] = 0;
        }
    }

    virtual bool reuse() { return true; }
};

template<int AlignSize>
class TCMemPoolTlsHolder :
    public instance_tls_owner<TCMemPoolTlsHolder<AlignSize>,
                              TCMemPoolOneThread<AlignSize> > {
public:
    bool reuse(TCMemPoolOneThread<AlignSize>* t) { return t->reuse(); }
};

/// mempool which alloc mem block identified by
/// integer offset(relative address), not pointers(absolute address)
/// integer offset could be 32bit even in 64bit hardware.
///
/// the returned offset is aligned to AlignSize, this allows 32bit
/// integer could address up to 4G*AlignSize memory
///
/// when memory exhausted, valvec can realloc memory without memcpy
/// @see valvec
template<int AlignSize>
class ThreadCacheMemPool : public valvec<byte_t>, boost::noncopyable {
    friend class TCMemPoolOneThread<AlignSize>;
    static const size_t ArenaSize = 2 * 1024 * 1024;

    typedef typename TCMemPoolOneThread<AlignSize>::link_t link_t;

protected:
    size_t  fragment_size; // for compatible with MemPool_Lock(Free|None|Mutex)

    typedef valvec<unsigned char> mem;
    TCMemPoolTlsHolder<AlignSize> m_tls;
    size_t        m_fastbin_max_size;

public:
    using mem::data;
    using mem::size; // bring to public...
    using mem::capacity;
    using mem::risk_set_data;
    using mem::risk_set_capacity;
    using mem::risk_release_ownership;

    size_t frag_size() const { return fragment_size; }

    valvec<unsigned char>* get_valvec() { return this; }

    std::function<TCMemPoolOneThread<AlignSize>*(ThreadCacheMemPool*)> m_new_tc;

          mem& get_data_byte_vec()       { return *this; }
    const mem& get_data_byte_vec() const { return *this; }

    enum { align_size = AlignSize };

    explicit ThreadCacheMemPool(size_t fastbin_max_size) {
        assert(fastbin_max_size >= AlignSize);
        assert(fastbin_max_size >= sizeof(typename TCMemPoolOneThread<AlignSize>::huge_link_t));
        fragment_size = 0;
        m_fastbin_max_size = pow2_align_up(fastbin_max_size, AlignSize);
        m_new_tc = &default_new_tc;
    }

    ~ThreadCacheMemPool() {
        // do nothing
    }

    void sync_frag_size() {
        m_tls.for_each_tls([this](TCMemPoolOneThread<AlignSize>* tc) {
            this->fragment_size += tc->m_frag_inc;
            tc->m_frag_inc = 0;
        });
    }

    void destroy_and_clean() {
        mem::clear();
    }

    void get_fastbin(valvec<size_t>* fast) const {
        fast->resize_fill(m_fastbin_max_size/AlignSize, 0);
        m_tls.for_each_tls([fast](TCMemPoolOneThread<AlignSize>* tc) {
            auto _p = tc->m_freelist_head.data();
            auto _n = tc->m_freelist_head.size();
            size_t* fastptr = fast->data();
            for (size_t i = 0; i < _n; ++i) {
                fastptr[i] += _p[i].cnt;
            }
        });
    }

    size_t get_huge_stat(size_t* huge_memsize) const {
        size_t huge_size_sum = 0;
        size_t huge_node_cnt = 0;
        m_tls.for_each_tls([&](TCMemPoolOneThread<AlignSize>* tc) {
            huge_size_sum += tc->huge_size_sum;
            huge_node_cnt += tc->huge_node_cnt;
        });
        *huge_memsize = huge_size_sum;
        return huge_node_cnt;
    }

    void risk_set_data(const void* data, size_t len) {
        assert(NULL == mem::p);
        assert(0 == mem::n);
        assert(0 == mem::c);
        mem::risk_set_data((unsigned char*)data, len);
    }

    unsigned char byte_at(size_t pos) const {
        assert(pos < mem::n);
        return mem::p[pos];
    }

    void clear() {
    }

    void erase_all() {
    }

    terark_no_inline
    void reserve(size_t cap) {
        cap = pow2_align_up(cap, ArenaSize);
        size_t oldsize = mem::n;
        use_hugepage_resize_no_init(this, cap);
        mem::n = oldsize;
    }

    void shrink_to_fit() {}

    template<class U> const U& at(size_t pos) const {
        assert(pos < mem::n);
    //  assert(pos + sizeof(U) < mem::n);
        return *(U*)(mem::p + pos);
    }
    template<class U> U& at(size_t pos) {
        assert(pos < mem::n);
    //  assert(pos + sizeof(U) < mem::n);
        return *(U*)(mem::p + pos);
    }

    // should not throw
    terark_no_inline
    bool chunk_alloc(TCMemPoolOneThread<AlignSize>* tc, size_t request) {
        size_t  chunk_len; // = pow2_align_up(request, ArenaSize);
        size_t  cap  = mem::c;
        size_t  oldn; // = mem::n;
        byte_t* base = mem::p;
        do {
            chunk_len = pow2_align_up(request, ArenaSize);
            oldn = mem::n;
            size_t endpos = size_t(base + oldn);
            if (terark_unlikely(endpos % ArenaSize != 0)) {
                chunk_len += ArenaSize - endpos % ArenaSize;
            }
            if (terark_unlikely(oldn + chunk_len > cap)) {
                if (oldn + request > cap) {
                    // cap is fixed, so fail
                    return false;
                }
                chunk_len = cap - oldn;
            }
            assert(oldn + chunk_len <= cap);
        } while (!cas_weak(mem::n, oldn, oldn + chunk_len));

        tc->set_hot_area(base, oldn, chunk_len);
        return true;
    }

    static TCMemPoolOneThread<AlignSize>*
    default_new_tc(ThreadCacheMemPool* mp) {
        return new TCMemPoolOneThread<AlignSize>(mp);
    }

    TCMemPoolOneThread<AlignSize>* tls() {
        return m_tls.get_tls(bind(&m_new_tc, this));
    }

    TCMemPoolTlsHolder<AlignSize>& alltls() { return m_tls; }

    // param request must be aligned by AlignSize
    size_t alloc(size_t request) {
        assert(request > 0);
        auto tc = m_tls.get_tls(bind(&m_new_tc, this));
        if (terark_unlikely(nullptr == tc)) {
            return size_t(-1); // fail
        }
        return alloc(request, tc);
    }
    terark_forceinline
    size_t alloc(size_t request, TCMemPoolOneThread<AlignSize>* tc) {
        assert(request > 0);
        assert(nullptr != tc);
        if (AlignSize < sizeof(link_t)) { // const expression
            request = std::max(sizeof(link_t), request);
        }
        request = pow2_align_up(request, AlignSize);
        size_t res = tc->alloc(mem::p, request);
        if (terark_likely(size_t(-1) != res))
            return res;
        else
            return alloc_slow_path(request, tc);
    }
    terark_no_inline
    size_t alloc_slow_path(size_t request, TCMemPoolOneThread<AlignSize>* tc) {
        if (chunk_alloc(tc, request))
            return tc->alloc(mem::p, request);
        else
            return size_t(-1);
    }

    size_t alloc3(size_t oldpos, size_t oldlen, size_t newlen) {
        assert(newlen > 0);
        assert(oldlen > 0);
        auto tc = m_tls.get_tls(bind(&m_new_tc, this));
        return alloc3(oldpos, oldlen, newlen, tc);
    }
    size_t alloc3(size_t oldpos, size_t oldlen, size_t newlen,
                  TCMemPoolOneThread<AlignSize>* tc) {
        assert(newlen > 0); newlen = pow2_align_up(newlen, AlignSize);
        assert(oldlen > 0); oldlen = pow2_align_up(oldlen, AlignSize);
        if (AlignSize < sizeof(link_t)) { // const expression
            newlen = std::max(sizeof(link_t), newlen);
        }
        size_t res = tc->alloc3(mem::p, oldpos, oldlen, newlen);
        if (terark_unlikely(size_t(-1) == res)) {
            assert(oldlen < newlen);
            if (terark_likely(chunk_alloc(tc, newlen))) {
                res = tc->alloc(mem::p, newlen);
                if (terark_likely(size_t(-1) != res)) {
                    memcpy(mem::p + res, mem::p + oldpos, oldlen);
                    tc->sfree(mem::p, oldpos, oldlen);
                }
            }
        }
        return res;
    }

    void sfree(size_t pos, size_t len) {
        assert(len > 0);
        assert(pos < mem::n);
        assert(pos % AlignSize == 0);
        auto tc = m_tls.get_tls(bind(&m_new_tc, this));
        sfree(pos, len, tc);
    }
    terark_forceinline
    void sfree(size_t pos, size_t len, TCMemPoolOneThread<AlignSize>* tc) {
        assert(len > 0);
        assert(pos < mem::n);
        assert(pos % AlignSize == 0);
        if (AlignSize < sizeof(link_t)) { // const expression
            len = std::max(sizeof(link_t), len);
        }
        len = pow2_align_up(len, AlignSize);
        assert(pos + len <= mem::n);
        tc->sfree(mem::p, pos, len);
    }

    void tc_populate(size_t sz) {
        size_t  chunk_len; // = pow2_align_down(sz, ArenaSize);
        size_t  cap  = mem::c;
        size_t  oldn; // = mem::n;
        byte_t* base = mem::p;
        do {
            chunk_len = pow2_align_down(sz, ArenaSize);
            oldn = mem::n;
            size_t endpos = size_t(base + oldn);
            if (terark_unlikely(endpos % ArenaSize != 0)) {
                chunk_len += ArenaSize - endpos % ArenaSize;
            }
            if (terark_unlikely(oldn + chunk_len > cap)) {
                chunk_len = cap - oldn;
            }
            assert(oldn + chunk_len <= cap);
        } while (!cas_weak(mem::n, oldn, oldn + chunk_len));

        auto tc = m_tls.get_tls(bind(&m_new_tc, this));
        tc->set_hot_area(base, oldn, chunk_len);
        //tc->populate_hot_area(base, ArenaSize);
        tc->populate_hot_area(base, 4*1024);
    }
};

template<int AlignSize>
inline
TCMemPoolTlsHolder<AlignSize>*
TCMemPoolOneThread<AlignSize>::tls_owner() const { return &m_mempool->m_tls; }

} // namespace terark

