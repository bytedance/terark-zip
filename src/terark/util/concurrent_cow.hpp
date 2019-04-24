/* vim: set tabstop=4 : */
#pragma once

#include <terark/stdtypes.hpp>
#include <terark/valvec.hpp>
#include <boost/noncopyable.hpp>

namespace terark { namespace cow_mman {

static const size_t AlignSize = 4;
enum ConcurrentLevel : char {
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
class TERARK_DLL_EXPORT TokenList;

struct TERARK_DLL_EXPORT TokenLink : private boost::noncopyable {
    TokenLink*    m_prev;
    TokenLink*    m_next;
    uint64_t      m_age;
};
class TERARK_DLL_EXPORT TokenBase : protected TokenLink {
    friend class cow_mman;
protected:
    class CowMemPool* m_main;
    class TokenList* m_sub;
    void*          m_value;
public:
    virtual ~TokenBase();
    virtual bool update(TokenUpdatePolicy) = 0;
    inline  bool update_lazy() { return update(UpdateLazy); }
    inline  bool update_now() { return update(UpdateNow); }
    class TokenList* sub()  const { return m_sub; }
    class CowMemPool* main() const { return m_main; }
    const void* value() const { return m_value; }
    template<class T>
    T value_of() const {
    //  assert(sizeof(T) == sub()->get_valsize());
        assert(NULL != m_value);
        assert(size_t(m_value) % AlignSize == 0);
        if (sizeof(T) == AlignSize)
            return   aligned_load<T>(m_value);
        else
            return unaligned_load<T>(m_value);
    }
    template<class T>
    T& mutable_value_of() const {
    //  assert(sizeof(T) == sub()->get_valsize());
        assert(NULL != m_value);
        assert(size_t(m_value) % AlignSize == 0);
        return *reinterpret_cast<T*>(m_value);
    }
};
class TERARK_DLL_EXPORT ReaderToken : public TokenBase {
protected:
    void update_list(ConcurrentLevel, class TokenList*);
public:
    ReaderToken();
    explicit ReaderToken(TokenList* sub);
    virtual ~ReaderToken();
    void attach(TokenList* sub);
    void detach();
    bool update(TokenUpdatePolicy) override;
};
class TERARK_DLL_EXPORT WriterToken : public TokenBase {
    friend class CowMemPool;
    void update_list(TokenList* sub);
protected:
    virtual bool init_value(void* valptr, size_t valsize);
public:
    explicit WriterToken(TokenList* sub);
    virtual ~WriterToken();
    bool update(TokenUpdatePolicy) override;
};
struct MemStat {
    valvec<size_t> fastbin;
    size_t used_size;
    size_t capacity;
    size_t frag_size; // = fast + huge
    size_t huge_size;
    size_t huge_cnt;
};

}} // namespace terark::cow_mmap
