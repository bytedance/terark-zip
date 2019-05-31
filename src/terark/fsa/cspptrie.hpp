#pragma once
#include "fsa.hpp"

namespace terark {

class MainPatricia;
class TERARK_DLL_EXPORT Patricia : public MatchingDFA, public boost::noncopyable {
public:
    static const size_t AlignSize = 4;
    static const size_t max_state = UINT32_MAX - 1;
    static const size_t nil_state = UINT32_MAX;
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
        friend class MainPatricia;
    protected:
        MainPatricia* m_trie;
        void*         m_value;
    public:
        virtual ~TokenBase();
        virtual bool update(TokenUpdatePolicy) = 0;
        inline  bool update_lazy() { return update(UpdateLazy); }
        inline  bool update_now() { return update(UpdateNow); }
        Patricia* trie() const { return reinterpret_cast<Patricia*>(m_trie); }
        const void* value() const { return m_value; }
        template<class T>
        T value_of() const {
            assert(sizeof(T) == trie()->get_valsize());
            assert(NULL != m_value);
            assert(size_t(m_value) % AlignSize == 0);
            if (sizeof(T) == AlignSize)
                return   aligned_load<T>(m_value);
            else
                return unaligned_load<T>(m_value);
        }
        template<class T>
        T& mutable_value_of() const {
            assert(sizeof(T) == trie()->get_valsize());
            assert(NULL != m_value);
            assert(size_t(m_value) % AlignSize == 0);
            return *reinterpret_cast<T*>(m_value);
        }
    };
public:
    class TERARK_DLL_EXPORT ReaderToken : public TokenBase {
    protected:
        void update_list(ConcurrentLevel, MainPatricia*);
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
        friend class MainPatricia;
        void update_list(MainPatricia*);
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
    class TERARK_DLL_EXPORT Iterator :
                     public ReaderToken, public ADFA_LexIterator {
    protected:
        class MyImpl;
        // valvec::param_type requires Entry to be complete
        struct Entry {
            uint32_t state;
            uint16_t n_children;
            uint08_t nth_child;
            uint08_t zpath_len;
            bool has_next() const { return nth_child + 1 < n_children; }
        };
        valvec<Entry> m_iter;
        size_t        m_flag;
    public:
        Iterator();
        explicit Iterator(const Patricia*);
        ~Iterator();
        void token_detach_iter();
        bool update(TokenUpdatePolicy) override final;
        void reset(const BaseDFA*, size_t root = 0) override final;
        bool seek_begin() override final;
        bool seek_end() override final;
        bool seek_lower_bound(fstring key) override final;
        bool incr() override final;
        bool decr() override final;
        size_t seek_max_prefix(fstring) override final;
    };
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
    typedef bool (Patricia::*insert_func_t)(fstring, void*, WriterToken*);
    insert_func_t m_insert;
    size_t    m_valsize;
    size_t    m_n_words;
    Stat      m_stat;
};

} // namespace terark
