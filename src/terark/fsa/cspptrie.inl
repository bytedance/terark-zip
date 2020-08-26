#pragma once
#if __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Warray-bounds"
#endif
#if defined(TERARK_WITH_TBB)
  // to prevent incompatible object layout:
  #include <boost/preprocessor/cat.hpp>
  #define MainPatricia BOOST_PP_CAT(MainPatricia_TBB_, TERARK_WITH_TBB)
#endif

#include "cspptrie.hpp"
#include <limits.h>
#include <boost/noncopyable.hpp>
#include <boost/lockfree/queue.hpp>
#include <terark/bitmap.hpp>
#include <terark/mempool.hpp>
#include <terark/mempool_fixed_cap.hpp>
#include <terark/mempool_lock_none.hpp>
#include <terark/mempool_lock_free.hpp>
#include <terark/mempool_thread_cache.hpp>
#include <terark/thread/mutex.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/auto_grow_circular_queue.hpp>
#include <deque>
#include <mutex>
#if defined(TerarkFSA_HighPrivate)
#include "dfa_algo.hpp"
#endif // TerarkFSA_HighPrivate
#include "dfa_mmap_header.hpp"
#include "fast_search_byte.hpp"
#include "graph_walker.hpp"
#include "x_fsa_util.hpp"

#define TERARK_PATRICIA_LINEAR_SEARCH_SMALL

namespace terark {

union PatriciaNode {
    // 1. b_lazy_free can only be set to 1 once, it is permanent: once set,
    //    never clear. b_lazy_free is just for non fast node.
    // 2. b_set_final is the lock for set fast node's final bit, it's permanent.
    //    permanent makes a little simple and performance gain.
    // 3. b_set_final is a optimization, we use it, just because it is an
    //    unused bit if we don't use it. if we don't use it, b_lock should be
    //    used for this purpose(in this case, b_lock need to be unlocked thus
    //    is not permanent).
    struct MetaInfo {
        uint08_t  n_cnt_type  : 4;
        uint08_t  b_is_final  : 1;
        uint08_t  b_lazy_free : 1;
        uint08_t  b_set_final : 1; // only for fast node set final
        uint08_t  b_lock      : 1;
        uint08_t  n_zpath_len;
        uint08_t  c_label[2];
    };
    struct BigCount {
        uint16_t  unused;
        uint16_t  n_children;
    };
    MetaInfo  meta;
    uint08_t  flags; // for meta flags
    BigCount  big;
    uint32_t  child;
    uint08_t  bytes[4];
    char      chars[4];
};
BOOST_STATIC_ASSERT(sizeof(PatriciaNode) == 4);

#define PatriciaNode_IsValid(x) (x.meta.n_cnt_type <= 8 || x.meta.n_cnt_type == 15)

template<size_t Align>
class TERARK_DLL_EXPORT PatriciaMem : public Patricia {
public:
    static_assert(Align==4 || Align==8, "Align must be 4 or 8");
    typedef typename std::conditional<Align==4,uint32_t,uint64_t>::type pos_type;
    typedef size_t   state_id_t;
    typedef pos_type transition_t;
    friend class TokenBase;
    friend class ReaderToken;
    friend class WriterToken;
    friend class Iterator;
    static const size_t AlignSize = Align;
    static const size_t max_state = pos_type(-1) - 1;
    static const size_t nil_state = pos_type(-1);
    static const size_t sigma = 256;

    size_t v_gnode_states() const override final { return m_n_nodes; }
    bool has_freelist() const override final { return false; }
    bool is_free(size_t) const { assert(false); return false; }

    void mem_lazy_free(size_t loc, size_t size);

    WriterTokenPtr& tls_writer_token() final;
    ReaderToken* tls_reader_token() final;

protected:
    struct LazyFreeItem {
        uint64_t age;
        pos_type node;
        pos_type size;
    };
    //  using  LazyFreeListBase = AutoGrowCircularQueue<LazyFreeItem>;
    using  LazyFreeListBase = std::deque<LazyFreeItem>;
    struct LazyFreeList : LazyFreeListBase {
        size_t m_mem_size = 0;
    };
    struct LazyFreeListTLS;
    union {
        LazyFreeList    m_lazy_free_list_sgl; // single
    };
    WriterTokenPtr m_writer_token_sgl;
    struct ReaderTokenTLS_Holder;
    struct ReaderTokenTLS_Object {
        ReaderTokenTLS_Object() { m_token.reset(new ReaderToken()); }
        ReaderTokenPtr m_token;
        ReaderTokenTLS_Object* m_next_free = nullptr;
        ReaderTokenTLS_Holder* tls_owner() const;
    };
    struct ReaderTokenTLS_Holder
        : instance_tls_owner<ReaderTokenTLS_Holder, ReaderTokenTLS_Object> {
        void reuse(ReaderTokenTLS_Object* token);
    };
    union {
        ReaderTokenTLS_Holder m_reader_token_sgl_tls;
    };
    LazyFreeList& lazy_free_list(ConcurrentLevel conLevel);

    void init(ConcurrentLevel conLevel);
    void mempool_lock_free_cons(size_t valsize);

    intptr_t  m_fd;
    size_t    m_appdata_offset;
    size_t    m_appdata_length;

    bool      m_head_is_dead;

    union {
        MemPool_CompileX<AlignSize> m_mempool;
        MemPool_LockNone<AlignSize> m_mempool_lock_none;
        MemPool_FixedCap<AlignSize> m_mempool_fixed_cap;
        //  MemPool_LockFree<AlignSize> m_mempool_lock_free;
        //  ThreadCacheMemPool<AlignSize> m_mempool_thread_cache;
        ThreadCacheMemPool<AlignSize> m_mempool_lock_free;
    };

    char padding2[32];
    // ---------------------------------------------------------
    // following fields are frequently updating
    TokenBase  m_dummy; // m_dummy.m_next is real head
    LinkType   m_tail;
    uint32_t   m_token_qlen;
    bool       m_head_lock;
    bool       m_head_is_idle;

//  std::mutex m_token_mutex;
    std::mutex m_counter_mutex;

    size_t     m_max_word_len;
    size_t     m_n_nodes;
    size_t     m_n_words;
    Stat       m_stat;

    void reclaim_head();

    void alloc_mempool_space(intptr_t maxMem);

    template<ConcurrentLevel>
    void revoke_expired_nodes();
    template<ConcurrentLevel, class LazyList>
    void revoke_expired_nodes(LazyList&, TokenBase*);
    void check_valsize(size_t valsize) const;
    void SingleThreadShared_sync_token_list(byte_t* oldmembase);

    void finish_load_mmap(const DFA_MmapHeader*) override final;
    long prepare_save_mmap(DFA_MmapHeader*, const void**) const override final;

    void destroy();

    template<ConcurrentLevel>
    void free_node(size_t nodeId, size_t nodeSize, LazyFreeListTLS*);

    template<ConcurrentLevel>
    size_t alloc_node(size_t nodeSize, LazyFreeListTLS*);

    template<ConcurrentLevel>
    void free_raw(size_t pos, size_t len, LazyFreeListTLS*);

    template<ConcurrentLevel>
    size_t alloc_raw(size_t len, LazyFreeListTLS*);

    void free_aux(size_t pos, size_t len);
    size_t alloc_aux(size_t len);

public:
    PatriciaMem();

    explicit
    PatriciaMem(size_t valsize,
                intptr_t maxMem = 512<<10,
                ConcurrentLevel = OneWriteMultiRead,
                fstring fpath = "");
    ~PatriciaMem();
    void set_readonly() override final;
    bool  is_readonly() const final {
        return NoWriteReadOnly == m_writing_concurrent_level;
    }

    ConcurrentLevel concurrent_level() const { return m_writing_concurrent_level; }

    size_t total_states() const { return m_mempool.size() / AlignSize; }

    size_t total_transitions() const { return m_n_nodes - 1; }

    size_t mem_capacity() const { return m_mempool.capacity(); }
    size_t mem_size_inline() const { return m_mempool.size(); }
    size_t mem_size() const override final { return m_mempool.size(); }
    void shrink_to_fit();

    static const size_t mem_alloc_fail = size_t(-1) / AlignSize;

    size_t new_root(size_t valsize);
    size_t mem_alloc(size_t size);
    void mem_free(size_t loc, size_t size);
    void* mem_get(size_t loc) {
        assert(loc < total_states());
        auto a = reinterpret_cast<byte_t*>(m_mempool.data());
        return a + loc * AlignSize;
    }
    const void* mem_get(size_t loc) const {
        assert(loc < total_states());
        auto a = reinterpret_cast<const byte_t*>(m_mempool.data());
        return a + loc * AlignSize;
    }

    size_t mem_align_size() const final { return AlignSize; }
    size_t mem_frag_size() const final { return m_mempool.frag_size(); }
    using Patricia::mem_get_stat;
    void mem_get_stat(MemStat*) const final;

    const Stat& trie_stat() const final { return m_stat; }
    const Stat& sync_stat() final;
    size_t num_words() const final { return m_n_words; }

    void* alloc_appdata(size_t len);
    void* appdata_ptr() const {
        assert(size_t(-1) != m_appdata_offset);
        assert(size_t(00) != m_appdata_length);
        return (byte_t*)m_mempool.data() + m_appdata_offset;
    }
    size_t appdata_len() const {
        assert(size_t(-1) != m_appdata_offset);
        assert(size_t(00) != m_appdata_length);
        return m_appdata_length;
    }

    void mempool_tc_populate(size_t) override;
    void mempool_set_readonly();
};

// Patricia is an interface
class TERARK_DLL_EXPORT MainPatricia : public PatriciaMem<4> {
public:
    typedef PatriciaNode State, state_t;
    static const uint32_t s_skip_slots[16];

    class IterImpl; friend class IterImpl;
    MainPatricia();

    explicit
    MainPatricia(size_t valsize,
                 intptr_t maxMem = 512<<10,
                 ConcurrentLevel = OneWriteMultiRead,
                 fstring fpath = "");

    bool is_pzip(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        return 0 != a[s].meta.n_zpath_len;
    }
    bool is_term(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        return a[s].meta.b_is_final;
    }
    void set_term_bit(size_t s) {
        auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        a[s].meta.b_is_final = 1;
    }
    void clear_term_bit(size_t s) {
        auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        a[s].meta.b_is_final = 0;
    }
    bool v_has_children(size_t s) const override final {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        return 0 != a[s].meta.n_cnt_type;
    }
    bool has_children(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        return 0 != a[s].meta.n_cnt_type;
    }
    bool more_than_one_child(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        return a[s].meta.n_cnt_type > 1;
    }
    state_id_t get_single_child(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(1 == a[s].meta.n_cnt_type);
        return a[s+1].child;
    }
    auchar_t get_single_child_char(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(1 == a[s].meta.n_cnt_type);
        return a[s].meta.c_label[0];
    }
    size_t num_children(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(PatriciaNode_IsValid(a[s]));
        if (a[s].meta.n_cnt_type <= 6)
            return a[s].meta.n_cnt_type;
        else
            return a[s].big.n_children;
    }
    state_id_t single_target(size_t s) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        assert(s < total_states());
        assert(1 == a[s].meta.n_cnt_type);
        return a[s+1].child;
    }
    void compact();

    fstring get_zpath_data(size_t state, MatchContext* = NULL) const {
        assert(state < total_states());
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        auto p = a + state;
        assert(15 != p->meta.n_cnt_type); // 15 never has zpath
        assert(p->meta.n_cnt_type <= 8);
        size_t cnt_type = p->meta.n_cnt_type;
        size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
        size_t skip = s_skip_slots[cnt_type];
        size_t zlen = p->meta.n_zpath_len;
        assert(zlen > 0);
        assert(zlen < 255);
        size_t zpos = AlignSize * (skip + n_children);
        // relaxed a valsize: real size should be zpos + zlen + valsize
        assert(AlignSize*state + zpos + zlen <= m_mempool.size());
        auto   zptr = p->bytes + zpos;
        return fstring(zptr, zlen);
    }

    typedef const PatriciaNode* StateMoveContext;

    size_t state_move_slow(size_t parent, auchar_t ch, StateMoveContext& a)
    const {
        auto ap = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        a = ap;
	    return state_move_fast(parent, ch, ap);
    }

    size_t state_move(size_t curr, auchar_t ch) const {
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
	    return state_move_fast(curr, ch, a);
    }

    size_t state_move_fast(size_t curr, auchar_t ch, StateMoveContext a)
    const {
        assert(curr < total_states());
        assert(ch <= 255);
        size_t  cnt_type = a[curr].meta.n_cnt_type;
        switch (cnt_type) {
        default:
            assert(false);
            break;
        case 0:
            assert(a[curr].meta.b_is_final);
            break;
        case 2: if (ch == a[curr].meta.c_label[1]) return a[curr+2].child; no_break_fallthrough;
        case 1: if (ch == a[curr].meta.c_label[0]) return a[curr+1].child; break;

#if defined(__SSE4_2__) && !defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
        case 6: case 5: case 4:
            {
                auto label = a[curr].meta.c_label;
                size_t idx = sse4_2_search_byte(label, cnt_type, byte_t(ch));
                if (idx < cnt_type)
                    return a[curr + 2 + idx].child;
            }
            break;
#else
        case 6: if (ch == a[curr].meta.c_label[5]) return a[curr+7].child; no_break_fallthrough;
        case 5: if (ch == a[curr].meta.c_label[4]) return a[curr+6].child; no_break_fallthrough;
        case 4: if (ch == a[curr].meta.c_label[3]) return a[curr+5].child; no_break_fallthrough;
#endif
        case 3: if (ch == a[curr].meta.c_label[2]) return a[curr+4].child;
                if (ch == a[curr].meta.c_label[1]) return a[curr+3].child;
                if (ch == a[curr].meta.c_label[0]) return a[curr+2].child;
			break;
        case 7: // cnt in [ 7, 16 ]
            {
                size_t n_children = a[curr].big.n_children;
                assert(n_children >=  7);
                assert(n_children <= 16);
                auto label = a[curr].meta.c_label + 2; // do not use [0,1]
#if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
                if (ch <= label[n_children-1]) {
                    size_t idx = size_t(-1);
                    do idx++; while (label[idx] < byte_t(ch));
                    if (byte_t(ch) == label[idx])
                        return a[curr + 1 + 4 + idx].child;
                }
#else
                size_t idx = fast_search_byte_max_16(label, n_children, byte_t(ch));
                if (idx < n_children)
                    return a[curr + 1 + 4 + idx].child;
#endif
            }
            break;
        case 8: // cnt >= 17
            assert(popcount_rs_256(a[curr+1].bytes) == a[curr].big.n_children);
            if (terark_bit_test(&a[curr+1+1].child, ch)) {
                size_t idx = fast_search_byte_rs_idx(a[curr+1].bytes, byte_t(ch));
                return a[curr + 10 + idx].child;
            }
            break;
        case 15:
            assert(256 == a[curr].big.n_children);
            return a[curr + 2 + ch].child;
        }
        return nil_state;
    }

    template<class OP>
    void for_each_move(size_t state, OP op) const {
        assert(state < total_states());
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
#if !defined(NDEBUG)
    #define op(x, c) \
        do { \
            assert(PatriciaNode_IsValid(a[x])); \
            op(x, c); \
        } while (0)
#endif
        size_t  n_children = a[state].meta.n_cnt_type;
        switch (n_children) {
        default:
            assert(false);
            break;
        case 0:
            assert(a[state].meta.b_is_final);
            break;
        case 1: op(a[state+1].child, a[state].meta.c_label[0]); break;
        case 2: op(a[state+1].child, a[state].meta.c_label[0]);
                op(a[state+2].child, a[state].meta.c_label[1]); break;
        case 3: case 4: case 5: case 6:
            for (size_t i = 0; i < n_children; ++i)
                op(a[state+2+i].child, a[state].meta.c_label[i]);
            break;
        case 7: // cnt in [ 7, 16 ]
            n_children = a[state].big.n_children;
            assert(n_children >=  7);
            assert(n_children <= 16);
            for (size_t i = 0; i < n_children; ++i)
                op(a[state+5+i].child, a[state].meta.c_label[2+i]);
            break;
        case 8: // cnt >= 17
            {
                n_children = a[state].big.n_children;
                assert(n_children >=  17);
                assert(n_children <= 256);
                size_t pos = 0;
                auto bits = (const uint64_t*)(a[state+1+1].bytes);
                auto children =  &a[state+1+9].child;
                for (int i = 0; i < 256/64; ++i) {
                    uint64_t bm = bits[i];
                    auchar_t ch = 64 * i;
                    for (; bm; ++pos) {
                        uint32_t target = children[pos];
                        int ctz = fast_ctz(bm);
                        ch += ctz;
                        op(target, ch);
                        ch++;
                        bm >>= ctz; // must not be bm >>= ctz + 1
                        bm >>= 1;   // because ctz + 1 may be bits of bm(32 or 64)
                    }
                }
                assert(pos == n_children);
            }
            break;
        case 15:
            {
            #if !defined(NDEBUG)
                assert(256 == a[state].big.n_children);
                size_t real_n_children = a[state+1].big.n_children;
                size_t cnt = 0;
            #endif
                auto children = &a[state+2].child;
                for(size_t ch = 0; ch < 256; ++ch) {
                    uint32_t child = children[ch];
                    if (nil_state != child) {
                        op(child, auchar_t(ch));
                        TERARK_IF_DEBUG(cnt++,);
                    }
                }
                assert(cnt == real_n_children);
            }
            break;
        }
#undef op
    }

    template<class OP>
    void for_each_dest(size_t state, OP op) const {
        assert(state < total_states());
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
#define for_each_dest_impl(skip, num) \
        { \
            auto children = &a[state + skip].child; \
            for (size_t i = 0, n = num; i < n; ++i) \
                op(children[i]); \
        }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        size_t  cnt_type = a[state].meta.n_cnt_type;
        switch (cnt_type) {
        default:
            assert(false);
            break;
        case 0:
            assert(a[state].meta.b_is_final);
            break;
        case 1: op(a[state+1].child); break;
        case 2: op(a[state+1].child);
                op(a[state+2].child); break;
        case 3: case 4: case 5: case 6:
            for_each_dest_impl(2, cnt_type);
            break;
        case 7: // cnt in [7, 16]
        case 8: // cnt >= 17
            for_each_dest_impl(s_skip_slots[cnt_type], a[state].big.n_children);
            break;
        case 15:
            assert(256 == a[state].big.n_children);
            for (size_t ch = 0; ch < 256; ++ch) {
                uint32_t child = a[state + 2 + ch].child;
                if (nil_state != child)
                    op(child);
            }
            break;
        }
#undef for_each_dest_rev
    }

    template<class OP>
    void for_each_dest_rev(size_t state, OP op) const {
        assert(state < total_states());
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
#define for_each_dest_rev_impl(skip, num) \
        { \
            auto children = &a[state + skip].child; \
            size_t i = num; \
            while (i) \
                op(children[--i]); \
        }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        size_t  cnt_type = a[state].meta.n_cnt_type;
        switch (cnt_type) {
        default:
            assert(false);
            break;
        case 0:
            assert(a[state].meta.b_is_final);
            break;
        case 2: op(a[state+2].child); no_break_fallthrough;
        case 1: op(a[state+1].child); break;
        case 3: case 4: case 5: case 6:
            for_each_dest_rev_impl(2, cnt_type);
            break;
        case 7: // cnt in [7, 16]
        case 8: // cnt >= 17
            for_each_dest_rev_impl(s_skip_slots[cnt_type], a[state].big.n_children);
            break;
        case 15:
            for (size_t ch = 256; ch-- > 0; ) {
                uint32_t child = a[state + 2 + ch].child;
                if (nil_state != child)
                    op(child);
            }
            break;
        }
#undef for_each_dest_rev_impl
    }

    size_t first_child(const PatriciaNode* p, byte_t* ch) const;
    size_t last_child(const PatriciaNode* p, byte_t* ch) const;
    size_t nth_child(const PatriciaNode* p, size_t nth, byte_t* ch) const;

    /// MainPatricia only:
    //
    /// when using iterator, use this method to get value from state id
    const void* get_valptr(size_t state) const {
        assert(state < total_states());
        auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
        auto x = get_valpos(a, state);
        return a->bytes + x;
    }
    void* get_valptr(size_t state) {
        assert(state < total_states());
        auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        auto x = get_valpos(a, state);
        return a->bytes + x;
    }

    bool lookup(fstring key, TokenBase* token) const override final;

    void set_insert_func(ConcurrentLevel conLevel);

    size_t state_move_impl(const PatriciaNode* a, size_t curr,
                           auchar_t ch, size_t* child_slot) const;
    template<ConcurrentLevel>
    bool insert_one_writer(fstring key, void* value, WriterToken* token);
    bool insert_multi_writer(fstring key, void* value, WriterToken* token);

    struct NodeInfo;

    template<ConcurrentLevel>
    void revoke_list(PatriciaNode* a, size_t state, size_t valsize, LazyFreeListTLS*);

    template<MainPatricia::ConcurrentLevel>
    size_t new_suffix_chain(fstring tail, size_t* pValpos, size_t* chainLen, size_t valsize, LazyFreeListTLS*);

    template<MainPatricia::ConcurrentLevel>
    size_t fork(size_t curr, size_t pos, NodeInfo*, byte_t newChar, size_t newSuffixNode, LazyFreeListTLS*);

    template<MainPatricia::ConcurrentLevel>
    size_t split_zpath(size_t curr, size_t pos, NodeInfo*, size_t* pValpos, size_t valsize, LazyFreeListTLS*);

    template<MainPatricia::ConcurrentLevel>
    size_t add_state_move(size_t curr, byte_t ch, size_t suffix_node, size_t valsize, LazyFreeListTLS*);

    size_t get_valpos(const PatriciaNode* a, size_t state) const {
        assert(state < total_states());
        size_t cnt_type = a[state].meta.n_cnt_type;
        size_t n_children = cnt_type <= 6 ? cnt_type : a[state].big.n_children;
        size_t skip = s_skip_slots[cnt_type];
        size_t zlen = a[state].meta.n_zpath_len;
        return AlignSize * (state + skip + n_children) + pow2_align_up(zlen, AlignSize);
    }

    static size_t get_val_self_pos(const PatriciaNode* p) {
        size_t cnt_type = p->meta.n_cnt_type;
        size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
        size_t skip = s_skip_slots[cnt_type];
        size_t zlen = p->meta.n_zpath_len;
        return AlignSize * (skip + n_children) + pow2_align_up(zlen, AlignSize);
    }

	typedef MainPatricia MyType;

#if defined(TerarkFSA_HighPrivate)
#include "ppi/for_each_suffix.hpp"
#include "ppi/match_key.hpp"
#include "ppi/match_path.hpp"
#include "ppi/match_prefix.hpp"
#include "ppi/accept.hpp"
#endif // TerarkFSA_HighPrivate

#include "ppi/dfa_const_virtuals.hpp"

   #define  TERARK_PATRICIA_USE_CHEAP_ITERATOR
#if defined(TERARK_PATRICIA_USE_CHEAP_ITERATOR)
    ADFA_LexIterator* adfa_make_iter(size_t = 0) const override final;
    ADFA_LexIterator16* adfa_make_iter16(size_t = 0) const override final;
#else
#include "ppi/adfa_iter.hpp"
#endif

#if defined(TerarkFSA_HighPrivate)
#include "ppi/for_each_word.hpp"

    size_t match_pinyin(auchar_t delim, const valvec<fstrvec>& pinyin
        , const OnNthWord& onHanZiWord
        , valvec<size_t> workq[2]
        ) const override final {
        THROW_STD(logic_error, "Unsupported");
        //return m_main->match_pinyin(delim, pinyin, onHanZiWord, workq);
    }
#endif // TerarkFSA_HighPrivate
};

template<size_t Align>
inline
typename
PatriciaMem<Align>::ReaderTokenTLS_Holder*
PatriciaMem<Align>::ReaderTokenTLS_Object::tls_owner() const {
    auto trie = static_cast<PatriciaMem<Align>*>(m_token->trie());
    return &trie->m_reader_token_sgl_tls;
}

} // namespace terark

#if __clang__
# pragma clang diagnostic pop
#endif

