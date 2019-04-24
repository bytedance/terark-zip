/* vim: set tabstop=4 : */
#pragma once

#include "concurrent_cow.hpp"
#include <terark/util/auto_grow_circular_queue.hpp>
#include <terark/mempool.hpp>
#include <terark/mempool_fixed_cap.hpp>
#include <terark/mempool_lock_none.hpp>
#include <terark/mempool_lock_free.hpp>

namespace terark { namespace cow_mman {

class TERARK_DLL_EXPORT CowMemPool;

class TERARK_DLL_EXPORT TokenList : boost::noncopyable {
    friend class CowMemPool;
    friend class ReaderToken;
    friend class WriterToken;
public:
    void mem_lazy_free(size_t loc, size_t size);
protected:
    void init_token_head();
    CowMemPool*      m_main;
    ConcurrentLevel  m_writing_concurrent_level;
    ConcurrentLevel  m_mempool_concurrent_level;
    bool             m_is_virtual_alloc;
    TokenLink        m_token_head;
    std::mutex       m_token_mutex;

    struct LazyFreeItem {
        uint64_t age;
        uint32_t node;
        uint32_t size;
    };
    typedef AutoGrowCircularQueue<LazyFreeItem> LazyFreeList;
    union {
                     LazyFreeList  m_lazy_free_list_sgl; // single
        instance_tls<LazyFreeList> m_lazy_free_list_tls;
    };

    LazyFreeList& lazy_free_list(ConcurrentLevel conLevel);

    TokenList(ConcurrentLevel conLevel);
    ~TokenList();
};

class TERARK_DLL_EXPORT CowMemPool : boost::noncopyable {
    friend class TokenList;
    friend class ReaderToken;
    friend class WriterToken;
public:
    static const size_t mem_alloc_fail = size_t(-1) / AlignSize;

    MemStat mem_get_stat() const;
    size_t mem_frag_size() const;

    template<ConcurrentLevel ConLevel>
    void free_node(size_t nodeId, size_t nodeSize) {
        size_t nodePos = AlignSize * nodeId;
        if (ConLevel >= MultiWriteMultiRead)
            m_mempool_lock_free.sfree(nodePos, nodeSize);
        else if (ConLevel == OneWriteMultiRead)
            m_mempool_fixed_cap.sfree(nodePos, nodeSize);
        else
            m_mempool_lock_none.sfree(nodePos, nodeSize);
    }

    template<ConcurrentLevel ConLevel>
    size_t alloc_node(size_t nodeSize) {
        size_t nodePos;
        if (ConLevel >= MultiWriteMultiRead) {
            nodePos = m_mempool_lock_free.alloc(nodeSize);
        }
        else if (ConLevel == OneWriteMultiRead) {
            nodePos = m_mempool_fixed_cap.alloc(nodeSize);
        }
        else {
            nodePos = m_mempool_lock_none.alloc(nodeSize);
        }
        size_t nodeId = nodePos / AlignSize;
        return nodeId;
    }

    size_t mem_capacity() const { return m_mempool.capacity(); }
    size_t mem_size_inline() const { return m_mempool.size(); }
    size_t mem_size() const { return  m_mempool.size(); }
    size_t frag_size() const { return m_mempool.frag_size(); }
    void shrink_to_fit();

    size_t mem_alloc(size_t size);
    void mem_free(size_t loc, size_t size);
    void* mem_get(size_t loc) {
        assert(loc < UINT32_MAX-1);
        auto a = reinterpret_cast<byte_t*>(m_mempool.data());
        return a + loc * AlignSize;
    }
    void mem_lazy_free(size_t loc, size_t size, TokenList*);

protected:
    ConcurrentLevel m_writing_concurrent_level;
    ConcurrentLevel m_mempool_concurrent_level;
    bool            m_is_virtual_alloc;
    union {
        MemPool_CompileX<AlignSize> m_mempool;
        MemPool_LockNone<AlignSize> m_mempool_lock_none;
        MemPool_FixedCap<AlignSize> m_mempool_fixed_cap;
    //  MemPool_LockFree<AlignSize> m_mempool_lock_free;
    //  ThreadCacheMemPool<AlignSize> m_mempool_thread_cache;
        ThreadCacheMemPool<AlignSize> m_mempool_lock_free;
    };
    virtual ~CowMemPool();
};

inline
TokenList::LazyFreeList&
TokenList::lazy_free_list(ConcurrentLevel conLevel) {
    assert(conLevel == m_main->m_mempool_concurrent_level);
    assert(conLevel ==   this->m_mempool_concurrent_level);
    if (MultiWriteMultiRead == conLevel)
        return m_lazy_free_list_tls.get();
    else
        return m_lazy_free_list_sgl;
}

}} // namespace terark::cow_mman
