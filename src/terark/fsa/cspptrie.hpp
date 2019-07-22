#pragma once
#include "fsa.hpp"
#include <mutex>

namespace terark {

template<size_t Align> class PatriciaMem;
class MainPatricia;

#define TERARK_friend_class_Patricia \
    friend class MainPatricia;   \
    friend class PatriciaMem<4>; \
    friend class PatriciaMem<8>

class TERARK_DLL_EXPORT Patricia : public MatchingDFA, public boost::noncopyable {
public:
    enum ConcurrentLevel {
        NoWriteReadOnly,     // 0
        SingleThreadStrict,  // 1
        SingleThreadShared,  // 2, iterator with token will keep valid
        OneWriteMultiRead,   // 3
        MultiWriteMultiRead, // 4
    };
    enum TokenUpdatePolicy {
        UpdateNow,  // 0
        UpdateLazy, // 1
    };
protected:
    struct TERARK_DLL_EXPORT TokenLink : private boost::noncopyable {
        TokenLink*    m_prev;
        TokenLink*    m_next;
        uint64_t      m_age;
    };
    class TERARK_DLL_EXPORT TokenBase : protected TokenLink {
        TERARK_friend_class_Patricia;
    protected:
        Patricia*     m_trie;
        void*         m_value;
    public:
        virtual ~TokenBase();
        virtual bool update(TokenUpdatePolicy) = 0;
        inline  bool update_lazy() { return update(UpdateLazy); }
        inline  bool update_now() { return update(UpdateNow); }
        Patricia* trie() const { return m_trie; }
        const void* value() const { return m_value; }
        template<class T>
        T value_of() const {
            assert(sizeof(T) == m_trie->m_valsize);
            assert(NULL != m_value);
            assert(size_t(m_value) % m_trie->mem_align_size() == 0);
            return unaligned_load<T>(m_value);
        }
        template<class T>
        T& mutable_value_of() const {
            assert(sizeof(T) == m_trie->m_valsize);
            assert(NULL != m_value);
            assert(size_t(m_value) % m_trie->mem_align_size() == 0);
            return *reinterpret_cast<T*>(m_value);
        }
    };
public:
    class TERARK_DLL_EXPORT ReaderToken : public TokenBase {
        TERARK_friend_class_Patricia;
    protected:
        void update_list(ConcurrentLevel, Patricia*);
    public:
        ReaderToken();
        explicit ReaderToken(Patricia*);
        virtual ~ReaderToken();
        void acquire(Patricia*);
        void release();
        bool update(TokenUpdatePolicy) override;
        bool lookup(fstring);
    };
    class TERARK_DLL_EXPORT WriterToken : public TokenBase {
        void* m_tls;
        TERARK_friend_class_Patricia;
        void update_list(Patricia*);
    protected:
        virtual bool init_value(void* valptr, size_t valsize) noexcept;
        virtual void destroy_value(void* valptr, size_t valsize) noexcept;
    public:
        WriterToken();
        explicit WriterToken(Patricia*);
        virtual ~WriterToken();
        void acquire(Patricia*);
        void release();
        bool update(TokenUpdatePolicy) override;
        bool insert(fstring key, void* value);
    };
    class TERARK_DLL_EXPORT Iterator : public ReaderToken, public ADFA_LexIterator {
    protected:
        Iterator(Patricia* sub) : ReaderToken(sub), ADFA_LexIterator(valvec_no_init()) {}
    public:
        virtual void token_detach_iter() = 0;
    };
    class TERARK_DLL_EXPORT IterMem : boost::noncopyable {
        byte_t  m_iter[sizeof(Iterator)];
        union { valvec<uint64_t> m_vstk; };
        size_t  m_flag;
    public:
        IterMem() noexcept;
        ~IterMem() noexcept;
        bool is_constructed() const noexcept;
        void construct(Patricia*);
        Iterator* iter() noexcept { return reinterpret_cast<Iterator*>(this); }
        const Iterator* iter() const noexcept { return reinterpret_cast<const Iterator*>(this); }
    };
    static const size_t ITER_SIZE = sizeof(IterMem);

    /// ptr size must be allocated ITER_SIZE
    virtual void construct_iter(void* ptr) const = 0;

    struct MemStat {
        valvec<size_t> fastbin;
        size_t used_size;
        size_t capacity;
        size_t frag_size; // = fast + huge
        size_t huge_size;
        size_t huge_cnt;
        size_t lazy_free_sum;
        size_t lazy_free_cnt;
    };
    static Patricia* create(size_t valsize,
                            size_t maxMem = 512<<10,
                            ConcurrentLevel = OneWriteMultiRead);
    MemStat mem_get_stat() const;
    virtual size_t mem_align_size() const = 0;
    virtual size_t mem_frag_size() const = 0;
    virtual void mem_get_stat(MemStat*) const = 0;

    /// @returns
    ///  true: key does not exists
    ///     token->value() == NULL : reached memory limit
    ///     token->value() != NULL : insert success,
    ///                              and value is copyed to token->value()
    ///  false: key has existed
    ///
    terark_forceinline
    bool insert(fstring key, void* value, WriterToken* token) {
        return (this->*m_insert)(key, value, token);
    }

    virtual bool lookup(fstring key, ReaderToken* token) const = 0;
    virtual void set_readonly() = 0;
    virtual bool  is_readonly() const = 0;
    virtual std::unique_ptr<WriterToken>& tls_writer_token() = 0;
    virtual ReaderToken* acquire_tls_reader_token() = 0;

    struct Stat {
        size_t n_fork;
        size_t n_split;
        size_t n_mark_final;
        size_t n_add_state_move;
        size_t sum() const { return n_fork + n_split + n_mark_final + n_add_state_move; }
    };
    const Stat& trie_stat() const { return m_stat; }
    size_t get_valsize() const { return m_valsize; }
    size_t num_words() const { return m_n_words; }
    ~Patricia();
protected:
    Patricia();
    virtual bool update_reader_token(ReaderToken*, TokenUpdatePolicy) = 0;
    virtual bool update_writer_token(WriterToken*, TokenUpdatePolicy) = 0;
    void update_min_age_inlock(TokenLink* token);
    bool insert_readonly_throw(fstring key, void* value, WriterToken*);
    typedef bool (Patricia::*insert_func_t)(fstring, void*, WriterToken*);
    insert_func_t m_insert;
    ConcurrentLevel     m_writing_concurrent_level;
    ConcurrentLevel     m_mempool_concurrent_level;
    bool                m_is_virtual_alloc;
    size_t    m_valsize;
    size_t    m_n_words;
    Stat      m_stat;
    std::mutex    m_token_mutex;
    TokenLink     m_token_head;
    size_t        m_min_age;
};

} // namespace terark
