#if __clang__
# pragma clang diagnostic ignored "-Warray-bounds"
#endif

#include "cspptrie.inl"
#include "tmplinst.hpp"
#include <terark/util/small_memcpy.hpp>
#include <terark/util/hugepage.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/profiling.hpp>
#include <terark/num_to_str.hpp>
#include "fast_search_byte.hpp"

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#	include <sys/types.h>
#	include <sys/stat.h>
#	include <fcntl.h>
#   define WIN32_LEAN_AND_MEAN
#   define NOMINMAX
#   include <windows.h>
#else
#	include <sys/types.h>
#	include <sys/stat.h>
#   include <sys/mman.h>
#	include <unistd.h>
#	include <fcntl.h>
#	include <errno.h>
#endif

#include <thread>
#include <iomanip>

#if BOOST_OS_LINUX
    //#include <sched.h>
    //#include <linux/getcpu.h>
    #include <syscall.h>
#elif BOOST_OS_WINDOWS
#elif BOOST_OS_MACOS
    #include <cpuid.h>
#else
    #error ThisCpuID unsupported
#endif

#if defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 8000
    #pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
namespace terark {

static constexpr uint08_t FLAG_final     = 0x1 << 4;
static constexpr uint08_t FLAG_lazy_free = 0x1 << 5;
static constexpr uint08_t FLAG_set_final = 0x1 << 6; // fast node set final
static constexpr uint08_t FLAG_lock      = 0x1 << 7;

#undef prefetch
#define prefetch(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)

inline void cas_unlock(bool& lock) {
    as_atomic(lock).store(false, std::memory_order_release);
}

template<size_t> struct CSPP_size_to_uint;
template<> struct CSPP_size_to_uint<4> { typedef unsigned int   type; };
template<> struct CSPP_size_to_uint<8> { typedef unsigned long long type; };

inline static size_t ThisThreadID() {
    auto id = std::this_thread::get_id();
    return (size_t)(CSPP_size_to_uint<sizeof(id)>::type&)(id);
}

/*
inline static unsigned ThisCpuID() {
  #if BOOST_OS_LINUX
    //return sched_getcpu();
    unsigned cpu = -1, node = -1;

    //::getcpu(&cpu, &node, NULL);
    syscall(SYS_getcpu, &cpu, &node, NULL);

    // ::getcpu(&cpu, &node); // only in glibc-2.29 <sched.h>

    return node << 8 | cpu;

  #elif BOOST_OS_WINDOWS
    return (int)GetCurrentProcessorNumber();
  #elif BOOST_OS_MACOS
  #define CPUID(INFO, LEAF, SUBLEAF) \
      __cpuid_count(LEAF, SUBLEAF, INFO[0], INFO[1], INFO[2], INFO[3])
    int CPU = -1;
    uint32_t CPUInfo[4];
    CPUID(CPUInfo, 1, 0);
    // CPUInfo[1] is EBX, bits 24-31 are APIC ID
    if ( (CPUInfo[3] & (1 << 9)) == 0) {
        CPU = -1;  // no APIC on chip
    }
    else {
        CPU = (unsigned)CPUInfo[1] >> 24;
    }
    return CPU;
  #else
    #error ThisCpuID unsupported
  #endif
}
*/

template<class T>
void cpback(T* dst, const T* src, size_t num) {
    for (size_t i = num; i-- > 0; ) {
        dst[i] = src[i];
    }
}
template<class T>
void cpfore(T* dst, const T* src, size_t num) {
    for (size_t i = 0; i < num; ++i) {
        dst[i] = src[i];
    }
}

template<class T>
bool array_eq(T* x, const T* y, size_t num) {
    for (size_t i = 0; i < num; ++i) {
        if (x[i] != y[i])
            return false;
    }
    return true;
}

const uint32_t MainPatricia::s_skip_slots[16] = {
     1, 1, 1,    // cnt_type = 0, 1, 2
     2, 2, 2, 2, // cnt_type = 3, 4, 5, 6
     5,          // cnt_type = 7, n_children = big.n_children in [7, 16]
    10,          // cnt_type = 8, n_children = big.n_children  >= 17
    UINT32_MAX, UINT32_MAX, //  9, 10,
    UINT32_MAX, UINT32_MAX, // 11, 12,
    UINT32_MAX, UINT32_MAX, // 13, 14,
    2,                      // 15, never has zpath, now just for fast nodes,
                            // NOTE: initial_state must be fast node!
};
//const size_t MainPatricia::max_state;
//const size_t MainPatricia::nil_state;

static profiling g_pf;

#define m_token_tail m_tail.next

template<size_t Align>
struct PatriciaMem<Align>::LazyFreeListTLS : TCMemPoolOneThread<AlignSize>, LazyFreeList {
    size_t m_n_nodes = 0;
    size_t m_n_words = 0;
    size_t m_max_word_len = 0;
/// corresponds to BaseDFA fields, do not reset on each token write
    size_t m_adfa_total_words_len = 0;
    size_t m_total_zpath_len = 0;
    size_t m_zpath_states = 0;
    // size_t m_revoke_try_cnt = 0;
///----------------------------------------
    size_t m_n_retry = 0;
    Stat   m_stat = {0,0,0,0};
    PatriciaMem<Align>* m_trie;
    WriterTokenPtr m_writer_token;
    ReaderTokenPtr m_reader_token;
    std::map<size_t, size_t>     m_retry_histgram;
    long long m_race_wait = 0;
    void sync_atomic(PatriciaMem<Align>*);
    void sync_no_atomic(PatriciaMem<Align>*);
    void reset_zero();
    void reuse() final;
    LazyFreeListTLS(PatriciaMem<Align>* trie);
    ~LazyFreeListTLS();
};

Patricia::Patricia() {
    m_dyn_sigma = 256;
    m_is_dag = true;
    m_insert = nullptr;
    m_valsize = 0;
}
Patricia::~Patricia() {}

template<size_t Align>
void PatriciaMem<Align>::init(ConcurrentLevel conLevel) {
    if (conLevel >= MultiWriteMultiRead) {
    }
    else {
        new(&m_lazy_free_list_sgl)LazyFreeList();
        new(&m_reader_token_sgl_tls)ReaderTokenTLS_Holder();
    }
    m_n_nodes = 1; // root will be pre-created
    m_max_word_len = 0;
    m_dummy.m_flags.state = DisposeDone;
    m_tail = {&m_dummy, 0};
    m_token_qlen = 0;
    m_n_words = 0;
    memset(&m_stat, 0, sizeof(Stat));

    m_head_is_dead = false;
    m_head_lock = false;
    m_is_virtual_alloc = false;
    m_fd = -1;
    m_appdata_offset = size_t(-1);
    m_appdata_length = 0;
    m_writing_concurrent_level = conLevel;
    m_mempool_concurrent_level = conLevel;
}

template<size_t Align>
inline typename
PatriciaMem<Align>::LazyFreeList&
PatriciaMem<Align>::lazy_free_list(ConcurrentLevel conLevel) {
    assert(conLevel == m_mempool_concurrent_level);
    if (MultiWriteMultiRead == conLevel) {
        auto tc = m_mempool_lock_free.tls();
        return static_cast<LazyFreeListTLS&>(*tc);
    } else {
        return m_lazy_free_list_sgl;
    }
}

template<size_t Align>
Patricia::WriterTokenPtr&
PatriciaMem<Align>::tls_writer_token() {
    if (MultiWriteMultiRead == m_mempool_concurrent_level) {
        auto tc = m_mempool_lock_free.tls();
        auto lzf = static_cast<LazyFreeListTLS*>(tc);
        return lzf->m_writer_token;
    }
    else {
        // not tls
        return m_writer_token_sgl;
    }
}

template<size_t Align>
void PatriciaMem<Align>::
ReaderTokenTLS_Holder::reuse(ReaderTokenTLS_Object* token) {
    assert(NULL != token->m_token.get());
    switch (token->m_token->m_flags.state) {
    default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
    case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
    case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
    case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
    case AcquireIdle: break; // OK
    case AcquireLock: break; // OK
    case ReleaseWait: break; // OK
    case ReleaseDone: break; // OK
    }
}

template<size_t Align>
Patricia::ReaderToken* PatriciaMem<Align>::tls_reader_token() {
    ReaderToken* tok = NULL;
    if (MultiWriteMultiRead == m_mempool_concurrent_level) {
        auto tc = m_mempool_lock_free.tls();
        auto lzf = static_cast<LazyFreeListTLS*>(tc);
        assert(NULL != lzf->m_reader_token.get());
        tok = lzf->m_reader_token.get();
    }
    else {
        tok = m_reader_token_sgl_tls.get_tls(
            []{ return new ReaderTokenTLS_Object; })->m_token.get();
    }
    return tok;
}

template<size_t Align>
void PatriciaMem<Align>::LazyFreeListTLS::sync_atomic(PatriciaMem<Align>* trie) {
    atomic_maximize(trie->m_max_word_len, m_max_word_len, std::memory_order_relaxed);
    as_atomic(trie->m_n_words).fetch_add(m_n_words, std::memory_order_relaxed);
    as_atomic(trie->m_n_nodes).fetch_add(m_n_nodes, std::memory_order_relaxed);
//  as_atomic(trie->m_adfa_total_words_len).fetch_add(m_adfa_total_words_len, std::memory_order_relaxed);
//  as_atomic(trie->m_total_zpath_len).fetch_add(m_total_zpath_len, std::memory_order_relaxed);
//  as_atomic(trie->m_zpath_states).fetch_add(m_zpath_states, std::memory_order_relaxed);
    as_atomic(trie->m_stat.n_fork).fetch_add(m_stat.n_fork, std::memory_order_relaxed);
    as_atomic(trie->m_stat.n_split).fetch_add(m_stat.n_split, std::memory_order_relaxed);
    as_atomic(trie->m_stat.n_mark_final).fetch_add(m_stat.n_mark_final, std::memory_order_relaxed);
    as_atomic(trie->m_stat.n_add_state_move).fetch_add(m_stat.n_add_state_move, std::memory_order_relaxed);
}

template<size_t Align>
inline void PatriciaMem<Align>::LazyFreeListTLS::reset_zero() {
    m_n_nodes = 0;
    m_n_words = 0;
//  m_adfa_total_words_len = 0;
//  m_total_zpath_len = 0;
//  m_zpath_states = 0;
    m_stat.n_fork = 0;
    m_stat.n_split = 0;
    m_stat.n_mark_final = 0;
    m_stat.n_add_state_move = 0;
    // m_revoke_try_cnt = 0;
}

template<size_t Align>
inline void PatriciaMem<Align>::LazyFreeListTLS::sync_no_atomic(PatriciaMem<Align>* trie) {
    maximize(trie->m_max_word_len, m_max_word_len);
    trie->m_n_words += m_n_words;
    trie->m_n_nodes += m_n_nodes;
//  trie->m_adfa_total_words_len += m_adfa_total_words_len;
//  trie->m_total_zpath_len += m_total_zpath_len;
//  trie->m_zpath_states += m_zpath_states;
    trie->m_stat.n_fork += m_stat.n_fork;
    trie->m_stat.n_split += m_stat.n_split;
    trie->m_stat.n_mark_final += m_stat.n_mark_final;
    trie->m_stat.n_add_state_move += m_stat.n_add_state_move;
}

template<size_t Align>
PatriciaMem<Align>::LazyFreeListTLS::LazyFreeListTLS(PatriciaMem<Align>* trie)
    : TCMemPoolOneThread<AlignSize>(&trie->m_mempool_lock_free)
    , m_trie(trie)
{
    m_reader_token.reset(new ReaderToken());
}

static const bool forceLeakMem = getEnvBool("PatriciaForceLeakMem", false);
#if !defined(NDEBUG)
// falseConcurrent for 'ConLevel is MultiWrite but the real writer num is 1'
// this is just for debug
static const bool falseConcurrent = getEnvBool("PatriciaMultiWriteFalse", false);
#endif
static const long csppDebugLevel = getEnvLong("PatriciaDebugLevel", 0);

///@param type: "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
#define PTrieLog(level, type, fmt, ...) do { \
    if (csppDebugLevel >= level) \
        fprintf(stderr, type ": %s:%d: %s: " fmt, \
            __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, ##__VA_ARGS__); \
    } while (0)

#undef  INFO
#define INFO(level, ...) PTrieLog(level, "INFO", __VA_ARGS__)
#undef  DBUG
#define DBUG(level, ...) PTrieLog(level, "DBUG", __VA_ARGS__)

template<size_t Align>
PatriciaMem<Align>::LazyFreeListTLS::~LazyFreeListTLS() {
    m_reader_token.reset();
    if (m_n_words) {
        auto trie = m_trie;
        trie->m_counter_mutex.lock();
        sync_no_atomic(trie);
        trie->m_counter_mutex.unlock();
    }
}

template<size_t Align>
void PatriciaMem<Align>::LazyFreeListTLS::reuse() {
    {
        TokenFlags flags = m_reader_token->m_flags;
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireIdle: break; // OK
        case AcquireLock: break; // OK
        case ReleaseDone: break; // OK
        case ReleaseWait: break; // OK
        }
    }
    if (m_writer_token) {
        TokenFlags flags = m_writer_token->m_flags;
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireIdle: break; // OK
        case AcquireLock: break; // OK
        case ReleaseDone: break; // OK
        case ReleaseWait: break; // OK
        }
    }
}

template<size_t Align>
void PatriciaMem<Align>::set_readonly() {
    assert(m_mempool_concurrent_level > NoWriteReadOnly);
    if (NoWriteReadOnly == m_writing_concurrent_level) {
        return; // fail over on release
    }
    sync_stat();
    mempool_set_readonly();
}

template<size_t Align>
const Patricia::Stat& PatriciaMem<Align>::sync_stat() {
  if (MultiWriteMultiRead == m_writing_concurrent_level) {
    long long sum_wait = 0;
    size_t sum_retry = 0;
    size_t uni_retry = 0;
    size_t thread_nth = 0;
    std::map<size_t, size_t> retry_histgram;
    auto sync = [&](TCMemPoolOneThread<AlignSize>* tc) {
      auto lzf = static_cast<LazyFreeListTLS*>(tc);
      assert(this == lzf->m_trie);
      lzf->sync_no_atomic(this);
      lzf->reset_zero();

      this->m_adfa_total_words_len += lzf->m_adfa_total_words_len;
      this->m_total_zpath_len += lzf->m_total_zpath_len;
      this->m_zpath_states += lzf->m_zpath_states;
      lzf->m_adfa_total_words_len = 0;
      lzf->m_total_zpath_len = 0;
      lzf->m_zpath_states = 0;

      sum_retry += lzf->m_n_retry;
      if (csppDebugLevel >= 1) {
        sum_wait += lzf->m_race_wait;
        fprintf(stderr,
                "PatriciaMW: thread_nth = %3zd, tls_retry = %8zd, wait = %10.6f sec\n",
                thread_nth, lzf->m_n_retry, g_pf.sf(0, lzf->m_race_wait));
        lzf->m_race_wait = 0;
      }
      lzf->m_n_retry = 0;
      if (csppDebugLevel >= 2) {
        for (auto& kv : lzf->m_retry_histgram) {
          uni_retry += kv.second;
          retry_histgram[kv.first] += kv.second;
          kv.second = 0;
        }
      }
      thread_nth++;
    };
    m_counter_mutex.lock();
    m_mempool_lock_free.alltls().for_each_tls(sync);
    m_counter_mutex.unlock();
    m_mempool_lock_free.sync_frag_size();
    if (csppDebugLevel >= 1 && m_n_words) {
      fprintf(stderr,
              "PatriciaMW: thread_num = %3zd, sum_retry = %8zd, wait = %10.6f sec, retry/total = %f\n",
              thread_nth, sum_retry, g_pf.sf(0, sum_wait), double(sum_retry)/m_n_words);
    }
    if (csppDebugLevel >= 2 && m_n_words) {
      fprintf(stderr
          , "PatriciaMW: uni_retry[num = %zd, ratio = %f], retry_hist = {\n"
          , uni_retry, double(uni_retry)/m_n_words);
      for (auto& kv : retry_histgram) {
        fprintf(stderr, "\t%5zd %5zd\n", kv.first, kv.second);
      }
      fprintf(stderr, "}\n");
    }
  }
  return m_stat;
}

template<size_t Align>
void PatriciaMem<Align>::mempool_set_readonly() {
  if (m_is_virtual_alloc && mmap_base) {
    assert(-1 != m_fd);
    // file based
    get_stat(const_cast<DFA_MmapHeader*>(this->mmap_base));
    auto base = (byte_t*)mmap_base;
    assert(m_mempool.data() == (byte_t*)(mmap_base + 1));
    size_t realsize = sizeof(DFA_MmapHeader) + m_mempool.size();
#if defined(_MSC_VER)
    FlushViewOfFile(base, realsize); // this flush is async
        // windows can not unmap unused address range
#else
    size_t filesize = sizeof(DFA_MmapHeader) + m_mempool.capacity();
    size_t alignedsize = pow2_align_up(realsize, 4*1024);
    msync(base, realsize, MS_ASYNC);
    munmap(base + alignedsize, filesize - alignedsize);
    ftruncate(m_fd, realsize);
    m_mempool.risk_set_capacity(m_mempool.size());
#endif
  }
  m_insert = (insert_func_t)&PatriciaMem::insert_readonly_throw;
  m_writing_concurrent_level = NoWriteReadOnly;
}

void MainPatricia::set_insert_func(ConcurrentLevel conLevel) {
    switch (conLevel) {
default: TERARK_DIE("Unknown == conLevel"); break;
case NoWriteReadOnly    : m_insert = (insert_func_t)&MainPatricia::insert_readonly_throw;                 break;
case SingleThreadStrict : m_insert = (insert_func_t)&MainPatricia::insert_one_writer<SingleThreadStrict>; break;
case SingleThreadShared : m_insert = (insert_func_t)&MainPatricia::insert_one_writer<SingleThreadShared>; break;
case OneWriteMultiRead  : m_insert = (insert_func_t)&MainPatricia::insert_one_writer<OneWriteMultiRead >; break;
case MultiWriteMultiRead: m_insert = (insert_func_t)&MainPatricia::insert_multi_writer;                   break;
    }
}

// to avoid too large backup buffer in MultiWriteMultiRead insert
static const size_t MAX_VALUE_SIZE = 128;
#if 0
static const size_t MAX_STATE_SIZE = 4 * (10 + 256) + 256 + MAX_VALUE_SIZE;
#else
static const size_t MAX_STATE_SIZE = 256;
#endif

MainPatricia::MainPatricia() : PatriciaMem<4>()
{
    set_insert_func(m_writing_concurrent_level);
}
MainPatricia::MainPatricia(size_t valsize, intptr_t maxMem,
                           ConcurrentLevel concurrentLevel, fstring fpath)
  : PatriciaMem<4>(valsize, maxMem, concurrentLevel, fpath)
{
    set_insert_func(m_writing_concurrent_level);
}

// default constructor, may be used for load_mmap
// concurrent level is SingleThread
// can also be used for single thread write as a set(valsize=0)
// when valsize != 0, it is a map
template<size_t Align>
PatriciaMem<Align>::PatriciaMem()
{
    init(SingleThreadShared);
    new(&m_mempool_lock_none)MemPool_LockNone<AlignSize>(MAX_STATE_SIZE);
    m_valsize = 0;
    new_root(0); // m_valsize == 0
}

template<size_t Align>
void PatriciaMem<Align>::check_valsize(size_t valsize) const {
    if (m_writing_concurrent_level >= MultiWriteMultiRead) {
        assert(valsize <= MAX_VALUE_SIZE);
        if (valsize > MAX_VALUE_SIZE) {
            THROW_STD(logic_error
                , "valsize = %zd, exceeds MAX_VALUE_SIZE = %zd"
                , valsize, MAX_VALUE_SIZE);
        }
    }
    assert(valsize % AlignSize == 0);
    if (valsize % AlignSize != 0) {
        THROW_STD(logic_error, "valsize = %zd, must align to 4", valsize);
    }
}
template<size_t Align>
void PatriciaMem<Align>::mempool_lock_free_cons(size_t valsize) {
    new(&m_mempool_lock_free)ThreadCacheMemPool<AlignSize>(MAX_STATE_SIZE + valsize);
    m_mempool_lock_free.m_new_tc =
    [this](ThreadCacheMemPool<AlignSize>*) -> TCMemPoolOneThread<AlignSize>*
    { return new LazyFreeListTLS(this); };
}

template<size_t Align>
PatriciaMem<Align>::PatriciaMem(size_t valsize, intptr_t maxMem,
                           ConcurrentLevel concurrentLevel, fstring fpath)
{
try
{
    init(concurrentLevel);
    m_valsize = valsize;
    check_valsize(valsize);
    switch (concurrentLevel) {
    default: assert(false); THROW_STD(logic_error, "invalid concurrentLevel = %d", concurrentLevel);
    case MultiWriteMultiRead: mempool_lock_free_cons(valsize);    break;
    case   OneWriteMultiRead: new(&m_mempool_fixed_cap)MemPool_FixedCap<AlignSize>(MAX_STATE_SIZE + valsize); break;
    case  SingleThreadStrict:
    case  SingleThreadShared: new(&m_mempool_lock_none)MemPool_LockNone<AlignSize>(MAX_STATE_SIZE + valsize); break;
    case     NoWriteReadOnly: memset(&m_mempool_lock_free, 0, sizeof(m_mempool_lock_free)); break; // do nothing
    }
    if (NoWriteReadOnly == concurrentLevel) {
        if (fpath.empty()) {
            return;
        }
        MmapWholeFile mmap(fpath);
        std::unique_ptr<BaseDFA> dfa(load_mmap_user_mem(mmap.base, mmap.size));
        if (auto pt = dynamic_cast<PatriciaMem*>(dfa.get())) {
            finish_load_mmap((DFA_MmapHeader*)mmap.base);
            mmap_base = (DFA_MmapHeader*)mmap.base;
            m_kv_delim = pt->m_kv_delim;
            m_zpath_states = pt->m_zpath_states;
            m_total_zpath_len = pt->m_total_zpath_len;
            m_adfa_total_words_len = pt->m_adfa_total_words_len;
            m_mmap_type = 1; // readonly mmap
            mmap.base = nullptr; // release ownership
        }
        else {
            THROW_STD(invalid_argument, "%s is not a Patricia", fpath.c_str());
        }
    }
    else {
        if (fpath.empty()) {
            alloc_mempool_space(maxMem);
        }
        else {
            assert(maxMem > 0);
            maximize(maxMem, 2<<20); // min is 2M
            MmapWholeFile mmap;
            mmap.base = mmap_write(fpath, &mmap.size, &m_fd);
            get_stat((DFA_MmapHeader*)mmap.base);
            mmap_base = (DFA_MmapHeader*)mmap.base;
            m_mempool.risk_set_data((byte_t*)mmap.base + sizeof(DFA_MmapHeader));
            m_mempool.risk_set_capacity(maxMem - sizeof(DFA_MmapHeader));
            m_is_virtual_alloc = true;
            mmap.base = nullptr; // release ownership
        }
        size_t root = new_root(valsize);
        TERARK_VERIFY_F(0 == root, "real root = %zd", root);
    }
}
catch (const std::exception&) {
    destroy();
    throw;
}
}

template<size_t Align>
void PatriciaMem<Align>::alloc_mempool_space(intptr_t maxMem) {
    if (NoWriteReadOnly == m_mempool_concurrent_level) {
        // just for load_mmap later
        return;
    }
    if (maxMem >= 0) {
        maxMem = align_up(maxMem, 16 << 10); // align to 16K
        maxMem = std::max(maxMem, intptr_t(512) << 10); // min 512K
        maxMem = std::min(maxMem, intptr_t( 16) << 30); // max  16G
        if (m_mempool_concurrent_level >= MultiWriteMultiRead) {
            m_mempool_lock_free.reserve(maxMem);
        }
        else {
    AllowRealloc:
            if (maxMem < intptr_t(hugepage_size)) {
                m_mempool.reserve(maxMem);
            } else {
                use_hugepage_resize_no_init(m_mempool.get_valvec(), maxMem);
                m_mempool.get_valvec()->risk_set_size(0);
            }
        }
    }
    else {
        maxMem = align_up(-maxMem, hugepage_size);
        maxMem = std::min(maxMem, intptr_t(16) << 30); // max 16G
        if (m_mempool_concurrent_level < OneWriteMultiRead) {
            goto AllowRealloc;
        }
        m_is_virtual_alloc = true;
#if defined(_MSC_VER)
        byte_t* mem = (byte_t*)VirtualAlloc(
            NULL, maxMem, MEM_RESERVE|MEM_COMMIT, PAGE_READWRITE);
        TERARK_VERIFY_F(NULL != mem, "VirtualAlloc(%zd) : ErrCode = %zd\n",
            maxMem, (size_t)GetLastError());
#else
  #if !defined(MAP_UNINITIALIZED)
    #define MAP_UNINITIALIZED 0
  #endif
  #if !defined(MAP_HUGETLB)
    #define MAP_HUGETLB 0
  #endif
        byte_t* mem = (byte_t*)mmap(NULL, maxMem,
            PROT_READ|PROT_WRITE,
            MAP_PRIVATE|
            MAP_ANONYMOUS|MAP_HUGETLB|MAP_UNINITIALIZED|MAP_NORESERVE,
            -1, 0);
        TERARK_VERIFY_F(MAP_FAILED != mem,
            "mmap(size = %zd) = %s\n", maxMem, strerror(errno));
#endif
        m_mempool.risk_set_data(mem);
        m_mempool.risk_set_capacity(maxMem);
    }
}

template<size_t Align>
size_t PatriciaMem<Align>::new_root(size_t valsize) {
    assert(valsize % AlignSize == 0);
    check_valsize(valsize);
    size_t root_size = AlignSize * (2 + 256) + valsize;
    size_t root = mem_alloc(root_size);
    if (mem_alloc_fail == root) {
        assert(m_writing_concurrent_level >= OneWriteMultiRead);
        return mem_alloc_fail;
    }
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    auto init_fast_meta = [](PatriciaNode* node, uint16_t n_children) {
        node[0].child = 0; // all zero
        node[0].meta.n_cnt_type = 15;
        node[0].big.n_children = 256;
        node[1].big.n_children = n_children; // real n_children
        node[1].big.unused  = 0;
    };
    init_fast_meta(a + root, 0);
    std::fill_n(&a[root + 2].child, 256, uint32_t(nil_state));
    tiny_memset_align_4(a + root + 2 + 256, 0xABAB, valsize);
    return root;
}

template<size_t Align>
PatriciaMem<Align>::~PatriciaMem() {
    destroy();
}

template<size_t Align>
void PatriciaMem<Align>::mempool_tc_populate(size_t sz) {
    if (m_writing_concurrent_level < MultiWriteMultiRead) {
        return;
    }
    m_mempool_lock_free.tc_populate(sz);
}

template<class T>
static void destroy_obj(T* p) { p->~T(); }

template<size_t Align>
void PatriciaMem<Align>::destroy() {
    auto conLevel = m_mempool_concurrent_level;
    if (NoWriteReadOnly != m_writing_concurrent_level) {
        set_readonly();
    }
    if (conLevel >= MultiWriteMultiRead) {
        assert(m_writer_token_sgl.get() == nullptr);
    }
    else {
        m_writer_token_sgl.reset();
        m_lazy_free_list_sgl.~LazyFreeList();
        m_reader_token_sgl_tls.~ReaderTokenTLS_Holder();
    }
    if (this->mmap_base && !m_is_virtual_alloc) {
        assert(-1 == m_fd);
        assert(NoWriteReadOnly == m_mempool_concurrent_level);
        switch (m_mempool_concurrent_level) {
        default:   assert(false); break;
        case MultiWriteMultiRead: m_mempool_lock_free.risk_release_ownership(); break;
        case   OneWriteMultiRead: m_mempool_fixed_cap.risk_release_ownership(); break;
        case  SingleThreadStrict:
        case  SingleThreadShared: m_mempool_lock_none.risk_release_ownership(); break;
        case     NoWriteReadOnly: break; // do nothing
        }
    }
    else if (!this->mmap_base && m_is_virtual_alloc) {
        assert(-1 == m_fd);
        assert(NoWriteReadOnly != m_mempool_concurrent_level);
  #if defined(_MSC_VER)
        if (!VirtualFree(m_mempool.data(), 0, MEM_RELEASE)) {
            std::terminate();
        }
  #else
        munmap(m_mempool.data(), m_mempool.capacity());
  #endif
        m_mempool.risk_release_ownership();
    }
    else if (-1 != m_fd) {
        assert(nullptr != mmap_base);
        assert(m_is_virtual_alloc);
        assert(m_mempool.data() == (byte_t*)(mmap_base + 1));
        size_t fsize = sizeof(DFA_MmapHeader) + m_mempool.capacity();
        mmap_close((void*)mmap_base, fsize, m_fd);
        mmap_base = nullptr;
        m_mempool.risk_release_ownership();
    }
    switch (m_mempool_concurrent_level) {
    default:   assert(false); break;
    case MultiWriteMultiRead: destroy_obj(&m_mempool_lock_free); break;
    case   OneWriteMultiRead: destroy_obj(&m_mempool_fixed_cap); break;
    case  SingleThreadStrict:
    case  SingleThreadShared: destroy_obj(&m_mempool_lock_none); break;
    case     NoWriteReadOnly: break; // do nothing
    }
    // delete waiting tokens, and check errors
    assert(m_token_tail->m_link.next == NULL);
    while (!cas_weak(m_head_lock, false, true)) {
        _mm_pause();
    }
    for(TokenBase* curr = m_dummy.m_link.next; curr; ) {
        TokenBase* next = curr->m_link.next;
        auto flags = curr->m_flags;
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case DisposeWait: break; // OK
        case ReleaseWait: break; // OK
        case AcquireIdle: break; // OK
        case AcquireLock: break; // OK
        }
        curr->m_flags.state = DisposeDone;
        delete curr;
        curr = next;
    }
    cas_unlock(m_head_lock);
}

template<size_t Align>
void PatriciaMem<Align>::mem_get_stat(MemStat* ms) const {
    ms->fastbin.erase_all();
    ms->used_size = m_mempool.size();
    ms->capacity  = m_mempool.capacity();
    ms->lazy_free_cnt = 0;
    ms->lazy_free_sum = 0;
    int thread_idx = 0;
    auto get_lzf = [&,ms](const LazyFreeList* lzf) {
        if (csppDebugLevel >= 2) {
            fprintf(stderr
                , "trie = %p, thread-%03d, lazyfree: cnt = %7zd, sum = %10.6f M, avg = %8.3f\n"
                , this, thread_idx, lzf->size(), lzf->m_mem_size / 1e6
                , (lzf->m_mem_size + 0.001) / (lzf->size() + 0.001)
            );
            thread_idx++;
        }
        ms->lazy_free_cnt += lzf->size();
        ms->lazy_free_sum += lzf->m_mem_size;
    };
    switch (m_mempool_concurrent_level) {
    default:   assert(false); break;
    case MultiWriteMultiRead:
        m_mempool_lock_free.get_fastbin(&ms->fastbin);
        const_cast<ThreadCacheMemPool<AlignSize>&>(m_mempool_lock_free).
          alltls().for_each_tls([=](TCMemPoolOneThread<AlignSize>* tc) {
            auto lzf = static_cast<LazyFreeListTLS*>(tc);
            get_lzf(lzf);
          });
        ms->huge_cnt  = m_mempool_lock_free.get_huge_stat(&ms->huge_size);
        ms->frag_size = m_mempool_lock_free.frag_size();
        break;
    case   OneWriteMultiRead:
        m_mempool_fixed_cap.get_fastbin(&ms->fastbin);
        get_lzf(&m_lazy_free_list_sgl);
        ms->huge_cnt  = m_mempool_fixed_cap.get_huge_stat(&ms->huge_size);
        ms->frag_size = m_mempool_fixed_cap.frag_size();
        break;
    case  SingleThreadStrict:
    case  SingleThreadShared:
        m_mempool_lock_none.get_fastbin(&ms->fastbin);
        get_lzf(&m_lazy_free_list_sgl);
        ms->huge_cnt  = m_mempool_lock_none.get_huge_stat(&ms->huge_size);
        ms->frag_size = m_mempool_lock_none.frag_size();
        break;
    case     NoWriteReadOnly: break; // do nothing
    }
}

template<size_t Align>
void PatriciaMem<Align>::shrink_to_fit() {}
void MainPatricia::compact() {}

size_t
MainPatricia::state_move_impl(const PatriciaNode* a, size_t curr,
                              auchar_t ch, size_t* child_slot)
const {
    assert(curr < total_states());
    assert(ch <= 255);
    auto p = a + curr;
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default:
        assert(false);
        break;
    case 0:
        assert(p->meta.b_is_final);
        break;
  #define return_on_slot(slot) \
        auto x = slot; \
        *child_slot = x; \
        return a[x].child
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #define return_if_match_ch(skip, idx) \
    if (ch == p->meta.c_label[idx]) { \
        return_on_slot(curr + skip +idx); \
    }
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    case 2: return_if_match_ch(1, 1); no_break_fallthrough;
    case 1: return_if_match_ch(1, 0);
            break;

#if defined(__SSE4_2__) && !defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
    case 6: case 5: case 4:
        {
            auto label = p->meta.c_label;
            size_t idx = sse4_2_search_byte(label, cnt_type, byte_t(ch));
            if (idx < cnt_type) {
                return_on_slot(curr + 2 + idx);
            }
        }
        break;
#else
    case 6: return_if_match_ch(2, 5); no_break_fallthrough;
    case 5: return_if_match_ch(2, 4); no_break_fallthrough;
    case 4: return_if_match_ch(2, 3); no_break_fallthrough;
#endif
    case 3: return_if_match_ch(2, 2);
            return_if_match_ch(2, 1);
            return_if_match_ch(2, 0); break;
    case 7: // cnt in [ 7, 16 ]
        {
            size_t n_children = p->big.n_children;
            assert(n_children >=  7);
            assert(n_children <= 16);
            auto label = p->meta.c_label + 2; // do not use [0,1]
#if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
            if (byte_t(ch) <= label[n_children-1]) {
                size_t idx = size_t(-1);
                do idx++; while (label[idx] < byte_t(ch));
                if (label[idx] == byte_t(ch)) {
                    return_on_slot(curr + 5 + idx);
                }
            }
#else
            size_t idx = fast_search_byte_max_16(label, n_children, byte_t(ch));
            if (idx < n_children) {
                return_on_slot(curr + 5 + idx);
            }
#endif
        }
        break;
    case 8: // cnt >= 17
        assert(p->big.n_children >= 17);
        if (terark_bit_test(&a[curr+1+1].child, ch)) {
            size_t idx = fast_search_byte_rs_idx(a[curr+1].bytes, byte_t(ch));
            return_on_slot(curr + 10 + idx);
        }
        break;
    case 15:
        assert(256 == p->big.n_children);
        return_on_slot(curr + 2 + ch);
    }
    *child_slot = size_t(-1);
    return nil_state;
}

static inline size_t node_size(const PatriciaNode* p, size_t valsize) {
    size_t cnt_type = p->meta.n_cnt_type;
    size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
    size_t skip = MainPatricia::s_skip_slots[cnt_type];
    size_t zlen = p->meta.n_zpath_len;
    return zlen + sizeof(PatriciaNode) * (skip + n_children)
           + (p->meta.b_is_final ? valsize : 0);
}

struct MainPatricia::NodeInfo {
    uint16_t n_skip = UINT16_MAX;
    uint16_t n_children = UINT16_MAX;
    uint32_t zp_offset = nil_state;
    uint32_t va_offset = nil_state;
    uint32_t node_size = nil_state;
    uint32_t oldSuffixNode = nil_state;
    uint32_t newSuffixNode = nil_state;
    fstring zpath;

    inline
    void set(const PatriciaNode* p, size_t zlen, size_t valsize) {
        assert(0==valsize || p->meta.b_is_final);
        assert(p->meta.n_zpath_len == zlen);
        size_t cnt_type = p->meta.n_cnt_type;
        assert(cnt_type <= 8 || cnt_type == 15);
        size_t skip = s_skip_slots[cnt_type];
        assert(skip <= 10);
        n_skip = skip;
        n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
        assert(n_children <= 256);
        zp_offset = sizeof(PatriciaNode) * (skip + n_children);
        va_offset = zp_offset + pow2_align_up(zlen, AlignSize);
        node_size = va_offset + valsize;
        zpath.p = p->chars + zp_offset;
        zpath.n = zlen;
    }

    size_t suffix_node_size(size_t zidx) const {
        size_t valsize = node_size - va_offset;
        return valsize + zp_offset + zpath.size() - zidx - 1;
    }
    size_t node_valsize() const { return node_size - va_offset; }
};

template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel>
void PatriciaMem<Align>::free_node(size_t nodeId, size_t nodeSize, LazyFreeListTLS* tls) {
    assert(mem_alloc_fail != nodeId);
    size_t nodePos = AlignSize * nodeId;
    free_raw<ConLevel>(nodePos, nodeSize, tls);
}

template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel>
void PatriciaMem<Align>::free_raw(size_t nodePos, size_t nodeSize, LazyFreeListTLS* tls) {
    assert(nodePos < m_mempool.size());
    if (forceLeakMem) {
      return;
    }
    if (ConLevel >= MultiWriteMultiRead)
        m_mempool_lock_free.sfree(nodePos, nodeSize, tls);
    else if (ConLevel == OneWriteMultiRead)
        m_mempool_fixed_cap.sfree(nodePos, nodeSize);
    else
        m_mempool_lock_none.sfree(nodePos, nodeSize);
}

template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel>
size_t PatriciaMem<Align>::alloc_node(size_t nodeSize, LazyFreeListTLS* tls) {
    size_t nodePos = alloc_raw<ConLevel>(nodeSize, tls);
    return nodePos / AlignSize;
}

template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel>
size_t PatriciaMem<Align>::alloc_raw(size_t nodeSize, LazyFreeListTLS* tls) {
    if (ConLevel >= MultiWriteMultiRead) {
        return m_mempool_lock_free.alloc(nodeSize, tls);
    }
    else if (ConLevel == OneWriteMultiRead) {
        return m_mempool_fixed_cap.alloc(nodeSize);
    }
    else {
        return m_mempool_lock_none.alloc(nodeSize);
    }
}

//static size_t PT_MAX_ZPATH =   6; // % 4 == 2, 6 for debug
static size_t PT_MAX_ZPATH = 254; // % 4 == 2
static size_t PT_LINK_NODE_SIZE = 4 + 4 + PT_MAX_ZPATH; // AlignSize = 4

template<Patricia::ConcurrentLevel ConLevel>
void MainPatricia::revoke_list(PatriciaNode* a, size_t head, size_t valsize,
                               LazyFreeListTLS* tls) {
    if (size_t(-1) == head)
        return;
    size_t curr = head;
    while (nil_state != curr) {
        assert(curr < total_states());
        assert(curr >= 2 + 256); // not in initial_state's area
        if (!a[curr].meta.b_is_final) {
            size_t next = a[curr + 1].child;
            assert(1 == a[curr].meta.n_cnt_type);
            assert(PT_MAX_ZPATH == a[curr].meta.n_zpath_len);
            assert(node_size(a + curr, valsize) == PT_LINK_NODE_SIZE);
            free_node<ConLevel>(curr, PT_LINK_NODE_SIZE, tls);
            curr = next;
        }
        else {
            free_node<ConLevel>(curr, node_size(a + curr, valsize), tls);
            break;
        }
    }
}

template<MainPatricia::ConcurrentLevel ConLevel>
size_t
MainPatricia::new_suffix_chain(fstring suffix, size_t* pValpos,
                               size_t* chainLen, size_t valsize,
                               LazyFreeListTLS* tls)
{
    size_t clen = 0; // for *chainLen
    size_t head = size_t(-1);
    size_t parent = size_t(-1);
    while (suffix.size() > PT_MAX_ZPATH) {
        size_t node = alloc_node<ConLevel>(PT_LINK_NODE_SIZE, tls);
        auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == node) {
            revoke_list<ConLevel>(a, head, valsize, tls);
            return size_t(-1);
        }
        clen++;
        a[node].child = 0; // zero it
        a[node].meta.n_cnt_type = 1;
        a[node].meta.n_zpath_len = PT_MAX_ZPATH;
        a[node].meta.c_label[0] = suffix[PT_MAX_ZPATH];
        a[node+1].child = nil_state;

        memcpy(a[node+2].bytes, suffix.data(), PT_MAX_ZPATH);
        a[node+2].bytes[PT_MAX_ZPATH + 0] = 0; // padding
        a[node+2].bytes[PT_MAX_ZPATH + 1] = 0; // padding

        if (size_t(-1) == head) {
            head = node;
        } else {
            assert(size_t(-1) != parent);
            a[parent + 1].child = node;
        }
        suffix = suffix.substr(PT_MAX_ZPATH + 1);
        parent = node;
    }
    size_t node = alloc_node<ConLevel>(AlignSize + valsize + suffix.size(), tls);
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == node) {
        revoke_list<ConLevel>(a, head, valsize, tls);
        return size_t(-1);
    }
    *chainLen = ++clen;
    a[node].child = 0; // zero it
    a[node].meta.b_is_final = true;
    a[node].meta.n_zpath_len = suffix.size();
    auto dst = a[node + 1].bytes;
    dst = small_memcpy_align_1(dst, suffix.data(), suffix.size());
    dst =  tiny_memset_align_p(dst, 0, AlignSize);
    *pValpos = dst - a->bytes;
    if (size_t(-1) == head) {
        return node;
    } else {
        a[parent+1].child = node;
        return head;
    }
}

template<MainPatricia::ConcurrentLevel ConLevel>
size_t
MainPatricia::fork(size_t parent, size_t zidx,
                   NodeInfo* ni, byte_t newChar, size_t newSuffixNode,
                   LazyFreeListTLS* tls) {
    assert(zidx < ni->zpath.size());
    size_t oldSuffixNode = alloc_node<ConLevel>(ni->suffix_node_size(zidx), tls);
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == oldSuffixNode) {
        return size_t(-1);
    }
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    if (ConLevel < OneWriteMultiRead) {
        ni->zpath.p = a[parent].chars + ni->zp_offset; // update zpath.p
    }
    assert(!a[parent].meta.b_is_final || ni->node_valsize() == m_valsize);
    byte_t  oldChar = ni->zpath[zidx];
    fstring oldTail = ni->zpath.substr(zidx+1);
    auto dst = a[oldSuffixNode].bytes;
    dst = small_memcpy_align_4(dst, a + parent, ni->zp_offset);
    a[oldSuffixNode].meta.n_zpath_len = byte_t(oldTail.n);
    dst = small_memcpy_align_1(dst, oldTail.p, oldTail.n);
    dst =  tiny_memset_align_p(dst, 0, AlignSize);
    tiny_memcpy_align_4(dst, a[parent].bytes + ni->va_offset, ni->node_valsize());
    size_t newParentSize = AlignSize*(1 + 2) + zidx; // must not final state
    size_t newParent = alloc_node<ConLevel>(newParentSize, tls);
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == newParent) {
        assert(node_size(a+ oldSuffixNode, m_valsize) == ni->suffix_node_size(zidx));
        free_node<ConLevel>(oldSuffixNode, ni->suffix_node_size(zidx), tls);
        return size_t(-1);
    }
    if (ConLevel < OneWriteMultiRead) {
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data()); // update a
        ni->zpath.p = a[parent].chars + ni->zp_offset; // update zpath.p
    }
    PatriciaNode* pParent = a + newParent;
    dst = pParent[3].bytes;
    dst = small_memcpy_align_1(dst, ni->zpath.p, zidx);
           tiny_memset_align_p(dst, 0, AlignSize);
    pParent->child = 0; // zero it
    pParent->meta.n_zpath_len = zidx;
    pParent->meta.n_cnt_type = 2;
    if (oldChar < newChar) {
        pParent->meta.c_label[0] = oldChar; pParent[1].child = oldSuffixNode;
        pParent->meta.c_label[1] = newChar; pParent[2].child = newSuffixNode;
    }
    else {
        pParent->meta.c_label[0] = newChar; pParent[1].child = newSuffixNode;
        pParent->meta.c_label[1] = oldChar; pParent[2].child = oldSuffixNode;
    }
    ni->oldSuffixNode = oldSuffixNode;
    ni->newSuffixNode = newSuffixNode;
    return newParent;
}

// split a zpath into prefix and suffix/tail
template<MainPatricia::ConcurrentLevel ConLevel>
size_t MainPatricia::split_zpath(size_t curr, size_t splitPos,
                                 NodeInfo* ni, size_t* pValpos,
                                 size_t valsize, LazyFreeListTLS* tls) {
    assert(splitPos < ni->zpath.size());
    size_t suffixNode = alloc_node<ConLevel>(ni->suffix_node_size(splitPos), tls);
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == suffixNode) {
        return size_t(-1);
    }
    size_t prefixNodeSize = AlignSize*2 + valsize + splitPos;
    size_t prefixNode = alloc_node<ConLevel>(prefixNodeSize, tls);
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == prefixNode) {
        free_node<ConLevel>(suffixNode, ni->suffix_node_size(splitPos), tls);
        return size_t(-1);
    }
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    if (ConLevel < OneWriteMultiRead) {
        ni->zpath.p = a[curr].chars + ni->zp_offset; // update zpath.p
    }
    assert(!a[curr].meta.b_is_final || ni->node_valsize() == valsize);
    fstring suffix = ni->zpath.substr(splitPos+1);
    auto dst = a[suffixNode].bytes;
    dst = small_memcpy_align_4(dst, a + curr, ni->zp_offset);
    dst = small_memcpy_align_1(dst, suffix.p, suffix.n);
    dst =  tiny_memset_align_p(dst, 0, AlignSize);
           tiny_memcpy_align_4(dst, a[curr].bytes + ni->va_offset,
                               ni->node_valsize());
    a[suffixNode].meta.n_zpath_len = byte_t(suffix.n);
    a[prefixNode].child = 0; // zero it
    a[prefixNode].meta.b_is_final = true;
    a[prefixNode].meta.n_zpath_len = splitPos;
    a[prefixNode].meta.n_cnt_type = 1;
    a[prefixNode].meta.c_label[0] = ni->zpath[splitPos];
    a[prefixNode+1].child = suffixNode;
    dst = a[prefixNode+2].bytes;
    dst = small_memcpy_align_1(dst, ni->zpath.p, splitPos);
    dst =  tiny_memset_align_p(dst, 0, AlignSize);
    *pValpos = dst - a->bytes;
    ni->oldSuffixNode = suffixNode;
    return prefixNode;
}

inline static size_t SuffixZpathStates(size_t chainLen, size_t pos, size_t keylen) {
    size_t suffixLen = keylen - pos - 1;
    // to reduce computation time, since div is slow
    if (terark_likely(suffixLen <= PT_MAX_ZPATH + 1)) {
        if (suffixLen > 0)
            return chainLen;
        else
            return chainLen - 1;
    }
    if (suffixLen % (PT_MAX_ZPATH + 1) == 0) {
        assert(suffixLen / (PT_MAX_ZPATH + 1) == chainLen-1);
        return chainLen-1; // last node of chain is not a zpath state
    } else {
        return chainLen;
    }
}

bool Patricia::insert_readonly_throw(fstring key, void* value, WriterToken*) {
    assert(NoWriteReadOnly == m_writing_concurrent_level);
    THROW_STD(logic_error, "invalid operation: insert to readonly trie");
}

template<MainPatricia::ConcurrentLevel ConLevel>
bool
MainPatricia::insert_one_writer(fstring key, void* value, WriterToken* token) {
    assert(AcquireDone == token->m_flags.state);
    assert(token->m_link.verseq <= m_token_tail->m_link.verseq);
    assert(m_writing_concurrent_level >= SingleThreadStrict);
    assert(m_writing_concurrent_level <= OneWriteMultiRead);
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    size_t const valsize = m_valsize;
    size_t curr_slot = size_t(-1);
    size_t curr = initial_state;
    size_t pos = 0;
    NodeInfo ni;
#define SingleThreadShared_check_for_sync_token_list() \
    ConLevel == SingleThreadShared &&                  \
        terark_unlikely(m_mempool.data() != a->bytes)  \
  ? SingleThreadShared_sync_token_list(a->bytes)  \
  : (void)(0)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
auto update_curr_ptr = [&](size_t newCurr, size_t nodeIncNum) {
    assert(newCurr != curr);
    if (ConLevel != SingleThreadStrict) {
        ullong age = token->m_link.verseq;
        m_lazy_free_list_sgl.push_back({age, uint32_t(curr), ni.node_size});
        m_lazy_free_list_sgl.m_mem_size += ni.node_size;
    }
    else {
        free_node<SingleThreadStrict>(curr, ni.node_size, nullptr);
    }
    if (ConLevel < OneWriteMultiRead)
        SingleThreadShared_check_for_sync_token_list(),
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    this->m_n_nodes += nodeIncNum;
    this->m_n_words += 1;
    this->m_adfa_total_words_len += key.size();
    this->m_total_zpath_len += key.size() - pos - nodeIncNum;
    a[curr_slot].child = uint32_t(newCurr);
    maximize(this->m_max_word_len, key.size());
};
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// begin search key...
size_t zidx;
for (;; pos++) {
    auto p = a + curr;
    size_t zlen = p->meta.n_zpath_len;
    if (zlen) {
        ni.set(p, zlen, 0);
        auto kkn = key.size() - pos;
        auto zkn = std::min(zlen, kkn);
        auto pkey = key.udata() + pos;
        for (zidx = 0; zidx < zkn; ++zidx) {
            if (terark_unlikely(pkey[zidx] != ni.zpath[zidx])) {
                pos += zidx;
                goto ForkBranch;
            }
        }
        pos += zidx;
        if (kkn <= zlen) {
            if (kkn < zlen)
                goto SplitZpath;
            assert(key.size() == pos);
            if (p->meta.b_is_final) {
                token->m_value = (char*)p->bytes + ni.va_offset;
                return false; // existed
            }
            goto MarkFinalStateOmitSetNodeInfo;
        }
    }
    else {
        if (terark_unlikely(key.size() == pos)) {
            if (p->meta.b_is_final) {
                token->m_value = (void*)(p->bytes + get_val_self_pos(p));
                return false; // existed
            }
            if (terark_likely(15 != p->meta.n_cnt_type))
                goto MarkFinalState;
            else
                goto MarkFinalStateOnFastNode;
        }
    }
#if 0
    size_t next_slot;
    size_t next = state_move_impl(a, curr, (byte_t)key.p[pos], &next_slot);
    if (nil_state == next)
        break;
    prefetch(a + next);
    curr_slot = next_slot;
    curr = next;
#else
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define break_on_slot(skip, idx)   \
        curr_slot = curr+skip+idx; \
        curr = p[skip+idx].child;  \
        break
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define break_if_match_ch(skip, idx)  \
    if (ch == p->meta.c_label[idx]) { \
        break_on_slot(skip, idx);     \
    }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    byte_t  ch = (byte_t)key.p[pos];
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
    case 0: assert(p->meta.b_is_final); goto MatchFail;
    case 2: break_if_match_ch(1, 1); no_break_fallthrough;
    case 1: break_if_match_ch(1, 0); goto MatchFail;
#if defined(__SSE4_2__) && !defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
    case 6: case 5: case 4:
        {
            size_t idx = sse4_2_search_byte(p->meta.c_label, cnt_type, ch);
            if (idx < cnt_type) {
                break_on_slot(2, idx);
            }
        }
        goto MatchFail;
#else
    case 6: break_if_match_ch(2, 5); no_break_fallthrough;
    case 5: break_if_match_ch(2, 4); no_break_fallthrough;
    case 4: break_if_match_ch(2, 3); no_break_fallthrough;
#endif
    case 3: break_if_match_ch(2, 2);
            break_if_match_ch(2, 1);
            break_if_match_ch(2, 0); goto MatchFail;
    case 7: // cnt in [ 7, 16 ]
        {
            size_t n_children = p->big.n_children;
            assert(n_children >=  7);
            assert(n_children <= 16);
    #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
            if (ch <= p[1].bytes[n_children-1]) {
                size_t idx = size_t(-1);
                do idx++; while (p[1].bytes[idx] < ch);
                if (ch == p[1].bytes[idx]) {
                    break_on_slot(5, idx);
                }
            }
    #else
            size_t idx = fast_search_byte_max_16(p[1].bytes, n_children, ch);
            if (idx < n_children) {
                break_on_slot(5, idx);
            }
    #endif
        }
        goto MatchFail;
    case 8: // cnt >= 17
        assert(p->big.n_children >= 17);
        if (terark_bit_test(&p[1+1].child, ch)) {
            size_t idx = fast_search_byte_rs_idx(p[1].bytes, ch);
            break_on_slot(10, idx);
        }
        goto MatchFail;
    case 15:
        assert(256 == p->big.n_children);
        {
            size_t next = p[2 + ch].child;
            if (nil_state != next) {
                curr_slot = curr + 2 + ch;
                curr = next;
                break;
            }
        }
        goto MatchFail;
    }
#undef break_if_match_ch
#endif
}
MatchFail:
assert(pos < key.size());

// end search key...
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define init_token_value_fail_clean(new1, new2, list, tls) \
        if (size_t(-1) != size_t(new1)) {                  \
          size_t size = node_size(a + new1, valsize);      \
          free_node<ConLevel>(new1, size, tls);            \
        }                                                  \
        if (size_t(-1) != size_t(new2)) {                  \
          size_t size = node_size(a + new2, valsize);      \
          free_node<ConLevel>(new2, size, tls);            \
        }                                                  \
        if (size_t(-1) != size_t(list)) {                  \
          revoke_list<ConLevel>(a, list, valsize, tls);    \
        }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define init_token_value(new1, new2, list) do {                \
    bool initOk = token->init_value(value, valsize);           \
    if (ConLevel < OneWriteMultiRead) {                        \
        SingleThreadShared_check_for_sync_token_list();        \
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data()); \
        ni.zpath.p = a[curr].chars + ni.zp_offset;             \
    }                                                          \
    auto valptr = a->bytes + valpos;                           \
    tiny_memcpy_align_4(valptr, value, valsize);               \
    if (ConLevel >= OneWriteMultiRead) {                       \
        if (terark_unlikely(!initOk)) {                        \
            init_token_value_fail_clean(new1, new2, list, NULL)\
            token->m_value = NULL;                             \
            return true;                                       \
        }                                                      \
    } else {                                                   \
        assert(initOk);                                        \
    }                                                          \
    token->m_value = valptr;                                   \
} while (0)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

// insert on curr with a transition to a new child which may has zpath
{
    revoke_expired_nodes<ConLevel>();
    m_stat.n_add_state_move++;
    size_t valpos = size_t(-1);
    size_t chainLen = 0;
    byte_t ch = key[pos];
    size_t suffix_node = new_suffix_chain<ConLevel>(key.substr(pos+1),
                                          &valpos, &chainLen, valsize, nullptr);
    if (ConLevel >= OneWriteMultiRead && size_t(-1) == suffix_node) {
        token->m_value = NULL;
        return true;
    }
    size_t zp_states_inc = SuffixZpathStates(chainLen, pos, key.n);
    if (ConLevel < OneWriteMultiRead)
        SingleThreadShared_check_for_sync_token_list(),
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    if (0 == a[curr].meta.n_zpath_len) {
        ni.set(a + curr, 0, a[curr].meta.b_is_final ? valsize : 0);
    }
    else if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    if (15 != a[curr].meta.n_cnt_type) {
        size_t newCurr = add_state_move<ConLevel>(curr, ch, suffix_node, valsize, nullptr);
        if (ConLevel == OneWriteMultiRead && size_t(-1) == newCurr) {
            revoke_list<OneWriteMultiRead>(a, suffix_node, valsize, nullptr);
            token->m_value = NULL;
            return true;
        }
        init_token_value(-1, newCurr, suffix_node);
        if (pos + 1 < key.size()) {
            m_zpath_states += zp_states_inc;
        }
        update_curr_ptr(newCurr, chainLen);
    }
    else // curr.type=15, special case, direct update curr[ch]
    {
        assert(a->bytes == m_mempool.data());
        assert(a[curr+2+ch].child == nil_state);
        init_token_value(-1, -1, suffix_node);
        m_total_zpath_len += key.size() - pos - 1;
        if (pos + 1 < key.size()) {
            m_zpath_states += zp_states_inc;
        }
        m_n_nodes += 1;
        m_n_words += 1;
        m_adfa_total_words_len += key.size() - pos - 1;
        a[curr+2+ch].child = suffix_node;
        a[curr+1].big.n_children++;
        maximize(m_max_word_len, key.size());
    }
    return true;
}
ForkBranch: {
    if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    revoke_expired_nodes<ConLevel>();
    m_stat.n_fork++;
    size_t valpos = size_t(-1);
    size_t chainLen = 0;
    size_t newSuffixNode = new_suffix_chain<ConLevel>(key.substr(pos+1),
                                            &valpos, &chainLen, valsize, nullptr);
    if (ConLevel == OneWriteMultiRead && size_t(-1) == newSuffixNode) {
        token->m_value = NULL;
        return true;
    }
    size_t newCurr = fork<ConLevel>(curr, zidx, &ni, key[pos], newSuffixNode, nullptr);
    if (ConLevel == OneWriteMultiRead && size_t(-1) == newCurr) {
        revoke_list<OneWriteMultiRead>(a, newSuffixNode, valsize, nullptr);
        token->m_value = NULL;
        return true;
    }
    size_t zp_states_inc = SuffixZpathStates(chainLen, pos, key.n);
    assert(state_move(newCurr, key[pos]) == newSuffixNode);
    assert(state_move(newCurr, ni.zpath[zidx]) == ni.oldSuffixNode);
    init_token_value(newCurr, ni.oldSuffixNode, newSuffixNode);
    if (terark_likely(1 != ni.zpath.n)) {
        if (0 != zidx && zidx + 1 != size_t(ni.zpath.n))
            zp_states_inc++;
    }
    else { // 1 == ni.zpath.n
        zp_states_inc--;
    }
    // signed(zp_states_inc) may < 0, it's ok for +=
    m_zpath_states += zp_states_inc;
    update_curr_ptr(newCurr, 1 + chainLen);
    return true;
}
SplitZpath: {
    if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    revoke_expired_nodes<ConLevel>();
    m_stat.n_split++;
    size_t valpos = size_t(-1);
    size_t newCurr = split_zpath<ConLevel>(curr, zidx, &ni, &valpos, valsize, nullptr);
    if (ConLevel == OneWriteMultiRead && size_t(-1) == newCurr) {
        token->m_value = NULL;
        return true;
    }
    init_token_value(newCurr, ni.oldSuffixNode, -1);
    if (terark_likely(1 != ni.zpath.n)) {
        if (0 != zidx && zidx + 1 != size_t(ni.zpath.n))
            m_zpath_states++;
    }
    else { // 1 == ni.zpath.n
        m_zpath_states--;
    }
    update_curr_ptr(newCurr, 1);
    return true;
}

// FastNode: cnt_type = 15 always has value space
MarkFinalStateOnFastNode: {
    size_t valpos = AlignSize * (curr + 2 + 256);
    init_token_value(-1, -1, -1);
    m_n_words++;
    m_stat.n_mark_final++;
    m_adfa_total_words_len += key.size();
    a[curr].meta.b_is_final = true;
    return true;
}
MarkFinalState: {
    ni.set(a + curr, 0, 0);
MarkFinalStateOmitSetNodeInfo:
    revoke_expired_nodes<ConLevel>();
    m_stat.n_mark_final++;
    assert(15 != a[curr].meta.n_cnt_type);
    assert(ni.node_size == ni.va_offset);
    size_t oldlen = ni.node_size;
    size_t newlen = ni.node_size + valsize;
    if (ConLevel == SingleThreadStrict) {
        size_t oldpos = AlignSize*curr;
        size_t newpos = m_mempool_lock_none.alloc3(oldpos, oldlen, newlen);
        size_t newcur = newpos / AlignSize;
        bool initOk = token->init_value(value, valsize);
        assert(initOk); TERARK_UNUSED_VAR(initOk);
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        byte_t* valptr = a[newcur].bytes + ni.va_offset;
        a[newcur].meta.b_is_final = true;
        token->m_value = valptr;
        tiny_memcpy_align_4(valptr, value, valsize);
        m_n_words += 1;
        m_adfa_total_words_len += key.size();
        a[curr_slot].child = uint32_t(newcur);
    }
    else if (ConLevel == SingleThreadShared) {
        size_t newpos = m_mempool_lock_none.alloc(newlen);
        size_t newcur = newpos / AlignSize;
        bool initOk = token->init_value(value, valsize);
        assert(initOk); TERARK_UNUSED_VAR(initOk);
        SingleThreadShared_check_for_sync_token_list();
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
        auto p = tiny_memcpy_align_4(a+newcur, a+curr, ni.va_offset);
        a[newcur].meta.b_is_final = true;
        token->m_value = p; // now p is newly added value
        tiny_memcpy_align_4(p, value, valsize);
        m_adfa_total_words_len += key.size();
        update_curr_ptr(newcur, 0);
    }
    else {
        size_t newcur = alloc_node<ConLevel>(newlen, nullptr);
        if (mem_alloc_fail == newcur) {
            token->m_value = NULL;
            return true;
        }
        auto p = tiny_memcpy_align_4(a+newcur, a+curr, ni.va_offset);
        a[newcur].meta.b_is_final = true;
        token->m_value = p; // now p is newly added value
        if (!token->init_value(value, valsize)) {
            free_node<ConLevel>(newcur, newlen, nullptr);
            token->m_value = NULL;
            return true;
        }
        tiny_memcpy_align_4(p, value, valsize);
        m_adfa_total_words_len += key.size();
        update_curr_ptr(newcur, 0);
    }
    return true;
}
}

bool
MainPatricia::insert_multi_writer(fstring key, void* value, WriterToken* token) {
    constexpr auto ConLevel = MultiWriteMultiRead;
    assert(MultiWriteMultiRead == m_writing_concurrent_level);
    assert(nullptr != m_token_tail);
    assert(ThisThreadID() == token->m_thread_id);
    assert(token->m_min_age <= token->m_link.verseq);
    assert(token->m_min_age <= m_token_tail->m_link.verseq);
    assert(token->m_link.verseq <= m_token_tail->m_link.verseq);
    TERARK_ASSERT_GE(token->m_link.verseq, m_dummy.m_min_age);
    auto const lzf = reinterpret_cast<LazyFreeListTLS*>(token->m_tls);
    assert(nullptr != lzf);
    assert(static_cast<LazyFreeListTLS*>(m_mempool_lock_free.tls()) == lzf);
    assert(AcquireDone == token->m_flags.state);
    if (terark_unlikely(token->m_flags.is_head)) {
        //now is_head is set before m_dummy.m_link.next, this assert
        //may fail false positive
        //assert(token == m_dummy.m_link.next);
        if (lzf->m_mem_size > 32*1024) {
            auto header = const_cast<DFA_MmapHeader*>(mmap_base);
            if (header) {
                header->dawg_num_words += lzf->m_n_words;
                header->transition_num += lzf->m_n_nodes;
                header->file_size = sizeof(DFA_MmapHeader) + m_mempool.size();
            }
            lzf->sync_no_atomic(this);
            token->mt_update(this);
            lzf->reset_zero();
        }
    }
    else {
        //this is a false assert, because m_token_head may be set by others
        //assert(token != m_token_head);
        if (terark_unlikely(m_head_is_dead)) {
            reclaim_head();
        }
        else if (lzf->m_mem_size > 512*1024) {
            if (m_head_is_idle) {
                reclaim_head();
            }
        }
    }
    auto const a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    bool is_value_inited = false;
    size_t const valsize = m_valsize;
    size_t n_retry = 0;
    if (0) {
    retry:
        n_retry++;
        lzf->m_n_retry++;
    }
    size_t parent = size_t(-1);
    size_t curr_slot = size_t(-1);
    size_t curr = initial_state;
    size_t pos = 0;
    NodeInfo ni;
    uint32_t backup[256];
    TERARK_IF_DEBUG(PatriciaNode bkskip[16],);
auto update_curr_ptr_concurrent = [&](size_t newCurr, size_t nodeIncNum, int lineno) {
    assert(reinterpret_cast<PatriciaNode*>(m_mempool.data()) == a);
    assert(newCurr != curr);
    assert(parent < curr_slot);
    assert(curr_slot < total_states());
    assert(!a[newCurr].meta.b_lazy_free);
    assert(nil_state != ni.node_size);
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PatriciaNode parent_unlock, parent_locked;
    PatriciaNode curr_unlock, curr_locked;
    parent_unlock = as_atomic(a[parent]).load(std::memory_order_relaxed);
    parent_unlock.meta.b_lazy_free = 0;
    parent_unlock.meta.b_lock = 0;
    parent_locked = parent_unlock;
    parent_locked.meta.b_lock = 1;
    if (!cas_weak(a[parent], parent_unlock, parent_locked)) {
        goto RaceCondition2;
    }
    // now a[parent] is locked, try lock curr:
    curr_unlock = as_atomic(a[curr]).load(std::memory_order_relaxed);
    curr_unlock.meta.b_lazy_free = 0;
    curr_unlock.meta.b_lock = 0;
    curr_locked = curr_unlock;
    curr_locked.meta.b_lazy_free = 1;
    if (!cas_weak(a[curr], curr_unlock, curr_locked)) {
        goto RaceCondition1;
    }
    // now a[curr] is locked, because --
    // now a[curr] is set as lazyfree, lazyfree flag also implies lock
    // lazyfree flag is permanent, it will not be reset to zero!
    if (!array_eq(backup, &a[curr + ni.n_skip].child, ni.n_children)) {
        goto RaceCondition0;
    }
    if (cas_weak(a[curr_slot].child, uint32_t(curr), uint32_t(newCurr))) {
        as_atomic(a[parent].flags).fetch_and(uint08_t(~FLAG_lock), std::memory_order_release);
        ullong age = token->m_link.verseq;
        assert(age >= m_dummy.m_min_age);
        maximize(lzf->m_max_word_len, key.size());
        lzf->m_n_nodes += nodeIncNum;
        lzf->m_n_words += 1;
        lzf->m_adfa_total_words_len += key.size();
        lzf->m_total_zpath_len += key.size() - pos - nodeIncNum;
        lzf->push_back({ age, uint32_t(curr), ni.node_size });
        lzf->m_mem_size += ni.node_size;
        if (terark_unlikely(n_retry && csppDebugLevel >= 2)) {
            lzf->m_retry_histgram[n_retry]++;
        }
        return true;
    }
    else { // parent has been lazy freed or updated by other threads
      RaceCondition0: as_atomic(a[curr].flags).fetch_and(uint08_t(~FLAG_lazy_free), std::memory_order_release);
      RaceCondition1: as_atomic(a[parent].flags).fetch_and(uint08_t(~FLAG_lock), std::memory_order_release);
      RaceCondition2:
        size_t min_age = (size_t)token->m_min_age;
        size_t age = (size_t)token->m_link.verseq;
        if (csppDebugLevel >= 3) {
            if (a[parent].meta.b_lazy_free) {
                fprintf(stderr,
                        "thread-%08zX: line: %d, age = %zd, min_age = %zd, retry%5zd, "
                        "a[parent = %zd].b_lazy_free == true: key: %.*s\n",
                        ThisThreadID(), lineno, age, min_age, n_retry,
                        parent, key.ilen(), key.data());
            }
            if (a[curr].meta.b_lock) {
                fprintf(stderr,
                        "thread-%08zX: line: %d, age = %zd, min_age = %zd, retry%5zd, "
                        "a[curr = %zd].b_lock == true: key: %.*s\n",
                        ThisThreadID(), lineno, age, min_age, n_retry,
                        curr, key.ilen(), key.data());
            }
            if (a[curr_slot].child != curr) {
                fprintf(stderr,
                        "thread-%08zX: line: %d, age = %zd, min_age = %zd, retry%5zd, "
                        "(a[curr_slot = %zd] = %zd) != (curr = %zd): key: %.*s\n",
                        ThisThreadID(), lineno, age, min_age, n_retry,
                        curr_slot, size_t(a[curr_slot].child), curr, key.ilen(), key.data());
            }
            if (!array_eq(backup, &a[curr + ni.n_skip].child, ni.n_children)) {
                fprintf(stderr,
                        "thread-%08zX: line: %d, age = %zd, min_age = %zd, retry%5zd, "
                        "curr confilict(curr = %zd, size = %zd) != 0: key: %.*s\n",
                        ThisThreadID(), lineno, age, min_age, n_retry,
                        curr, size_t(ni.node_size), key.ilen(), key.data());
            }
        }
        free_node<MultiWriteMultiRead>(newCurr, node_size(a + newCurr, valsize), lzf);
        if (nil_state != ni.newSuffixNode) {
            revoke_list<MultiWriteMultiRead>(a, ni.newSuffixNode, valsize, lzf);
        }
        if (nil_state != ni.oldSuffixNode) {
            size_t size = node_size(a + ni.oldSuffixNode, valsize);
            free_node<MultiWriteMultiRead>(ni.oldSuffixNode, size, lzf);
        }
        return false;
    }
};
#define update_curr_ptr(newCurr, nodeIncNum) \
    if (!update_curr_ptr_concurrent(newCurr, nodeIncNum, __LINE__)) { \
        goto retry; \
    }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// begin search key...
size_t zidx;
for (;; pos++) {
    auto p = a + curr;
    size_t zlen = p->meta.n_zpath_len;
    if (zlen) {
        ni.set(p, zlen, 0);
        auto kkn = key.size() - pos;
        auto zkn = std::min(zlen, kkn);
        auto pkey = key.udata() + pos;
        for (zidx = 0; zidx < zkn; ++zidx) {
            if (terark_unlikely(pkey[zidx] != ni.zpath[zidx])) {
                pos += zidx;
                goto ForkBranch;
            }
        }
        pos += zidx;
        if (kkn <= zlen) {
            if (kkn < zlen)
                goto SplitZpath;
            assert(key.size() == pos);
            if (p->meta.b_is_final) {
                token->m_value = (char*)p->bytes + ni.va_offset;
                goto HandleDupKey;
            }
            goto MarkFinalStateOmitSetNodeInfo;
        }
    }
    else {
        if (terark_unlikely(key.size() == pos)) {
            if (p->meta.b_is_final) {
                token->m_value = (void*)(p->bytes + get_val_self_pos(p));
                goto HandleDupKey;
            }
            if (terark_likely(15 != p->meta.n_cnt_type))
                goto MarkFinalState;
            else
                goto MarkFinalStateOnFastNode;
        }
    }
#if 0
    size_t next_slot;
    size_t next = state_move_impl(a, curr, (byte_t)key.p[pos], &next_slot);
    if (nil_state == next)
        break;
    prefetch(a + next);
    parent = curr;
    curr_slot = next_slot;
    curr = next;
#else
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#undef break_on_slot
#define break_on_slot(skip, idx)   \
        parent = curr; \
        curr_slot = curr+skip+idx; \
        curr = p[skip+idx].child;  \
        break
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define break_if_match_ch(skip, idx)  \
    if (ch == p->meta.c_label[idx]) { \
        break_on_slot(skip, idx);     \
    }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    byte_t  ch = (byte_t)key.p[pos];
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
    case 0: assert(p->meta.b_is_final); goto MatchFail;
    case 2: break_if_match_ch(1, 1); no_break_fallthrough;
    case 1: break_if_match_ch(1, 0); goto MatchFail;
#if defined(__SSE4_2__) && !defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
    case 6: case 5: case 4:
        {
            size_t idx = sse4_2_search_byte(p->meta.c_label, cnt_type, ch);
            if (idx < cnt_type) {
                break_on_slot(2, idx);
            }
        }
        goto MatchFail;
#else
    case 6: break_if_match_ch(2, 5); no_break_fallthrough;
    case 5: break_if_match_ch(2, 4); no_break_fallthrough;
    case 4: break_if_match_ch(2, 3); no_break_fallthrough;
#endif
    case 3: break_if_match_ch(2, 2);
            break_if_match_ch(2, 1);
            break_if_match_ch(2, 0); goto MatchFail;
    case 7: // cnt in [ 7, 16 ]
        {
            size_t n_children = p->big.n_children;
            assert(n_children >=  7);
            assert(n_children <= 16);
    #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
            if (ch <= p[1].bytes[n_children-1]) {
                size_t idx = size_t(-1);
                do idx++; while (p[1].bytes[idx] < ch);
                if (ch == p[1].bytes[idx]) {
                    break_on_slot(5, idx);
                }
            }
    #else
            size_t idx = fast_search_byte_max_16(p[1].bytes, n_children, ch);
            if (idx < n_children) {
                break_on_slot(5, idx);
            }
    #endif
        }
        goto MatchFail;
    case 8: // cnt >= 17
        assert(p->big.n_children >= 17);
        if (terark_bit_test(&p[1+1].child, ch)) {
            size_t idx = fast_search_byte_rs_idx(p[1].bytes, ch);
            break_on_slot(10, idx);
        }
        goto MatchFail;
    case 15:
        assert(256 == p->big.n_children);
        {
            size_t next = p[2 + ch].child;
            if (nil_state != next) {
                parent = curr;
                curr_slot = curr + 2 + ch;
                curr = next;
                break;
            }
        }
        goto MatchFail;
    }
#undef break_if_match_ch
#endif
}
MatchFail:
assert(pos < key.size());

// end search key...
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

// insert on curr with a transition to a new child which may has zpath
{
    lzf->m_stat.n_add_state_move += 1;
    revoke_expired_nodes<MultiWriteMultiRead>(*lzf, token);
    size_t valpos = size_t(-1);
    size_t chainLen = 0;
    byte_t ch = key[pos];
    size_t suffix_node = new_suffix_chain<MultiWriteMultiRead>(key.substr(pos+1),
                                               &valpos, &chainLen, valsize, lzf);
    size_t zp_states_inc = SuffixZpathStates(chainLen, pos, key.n);
    if (size_t(-1) == suffix_node) {
        token->m_value = NULL; // fail flag
        return true;
    }
    if (0 == a[curr].meta.n_zpath_len) {
        ni.set(a + curr, 0, a[curr].meta.b_is_final ? valsize : 0);
    }
    else if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    if (15 != a[curr].meta.n_cnt_type) {
        assert(ni.n_skip <= 10);
        assert(ni.n_children <= 256);
        cpfore(backup, &a[curr + ni.n_skip].child, ni.n_children);
        size_t newCurr = add_state_move<MultiWriteMultiRead>(curr, ch, suffix_node, valsize, lzf);
        if (size_t(-1) == newCurr) {
            revoke_list<MultiWriteMultiRead>(a, suffix_node, valsize, lzf);
            token->m_value = NULL; // fail flag
            return true;
        }
        if (a[newCurr].flags & (FLAG_lazy_free|FLAG_lock)) {
            free_node<MultiWriteMultiRead>(newCurr, node_size(a+newCurr, valsize), lzf);
            revoke_list<MultiWriteMultiRead>(a, suffix_node, valsize, lzf);
            if (csppDebugLevel >= 3)
                fprintf(stderr,
                    "thread-%08zX: retry %zd, add_state_move confict(curr = %zd)\n",
                    ThisThreadID(), n_retry, curr);
            goto retry;
        }
#define init_token_value_mw(new1, new2, list) do {               \
    if (terark_likely(!is_value_inited)) {                       \
      if (terark_unlikely(!token->init_value(value, valsize))) { \
        init_token_value_fail_clean(new1, new2, list, lzf)       \
        token->m_value = NULL;                                   \
        return true;                                             \
      }                                                          \
      is_value_inited = true;                                    \
    }                                                            \
    auto valptr = a->bytes + valpos;                             \
    tiny_memcpy_align_4(valptr, value, valsize);                 \
    token->m_value = valptr;                                     \
} while (0)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        init_token_value_mw(-1, newCurr, suffix_node);
        if (pos + 1 < key.size()) {
            lzf->m_zpath_states += zp_states_inc;
        }
        ni.newSuffixNode = suffix_node; // revoke if fail in update_curr_ptr
        update_curr_ptr(newCurr, chainLen);
        return true;
    }
    //
    // curr.type=15, special case, direct update curr[ch]
    //
    assert(a->bytes == m_mempool.data());
    assert(reinterpret_cast<PatriciaNode*>(m_mempool.data()) == a);
    assert(0 == a[curr].meta.b_lazy_free);
    assert(0 == a[curr].meta.n_zpath_len);
    init_token_value_mw(-1, -1, suffix_node); // must before cas set child
    uint32_t nil = nil_state;
    if (cas_weak(a[curr+2+ch].child, nil, uint32_t(suffix_node))) {
        as_atomic(a[curr+1].big.n_children).fetch_add(1, std::memory_order_relaxed);
        lzf->m_n_nodes += 1;
        lzf->m_n_words += 1;
        lzf->m_adfa_total_words_len += key.size();
        lzf->m_total_zpath_len += key.size() - pos - 1;
        if (pos + 1 < key.size()) {
            lzf->m_zpath_states += zp_states_inc;
        }
        maximize(lzf->m_max_word_len, key.size());
        return true;
    }
    else { // curr has updated by other threads
        free_node<MultiWriteMultiRead>(suffix_node, node_size(a + suffix_node, valsize), lzf);
        if (csppDebugLevel >= 3)
            fprintf(stderr,
                "thread-%08zX: retry %zd, set root child confict(root(=curr) = %zd)\n",
                ThisThreadID(), n_retry, curr);
        goto retry;
    }
}
ForkBranch: {
    if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    lzf->m_stat.n_fork += 1;
    revoke_expired_nodes<MultiWriteMultiRead>(*lzf, token);
    size_t valpos = size_t(-1);
    size_t chainLen = 0;
    size_t newSuffixNode = new_suffix_chain<MultiWriteMultiRead>(key.substr(pos+1),
                                                 &valpos, &chainLen, valsize, lzf);
    if (size_t(-1) == newSuffixNode) {
        token->m_value = NULL; // fail flag
        return true;
    }
    assert(ni.n_skip <= 10);
    assert(ni.n_children <= 256);
    cpfore(backup, &a[curr + ni.n_skip].child, ni.n_children);
    TERARK_IF_DEBUG(cpfore(bkskip, &a[curr], ni.n_skip),);
    size_t newCurr = fork<MultiWriteMultiRead>(curr, zidx, &ni, key[pos], newSuffixNode, lzf);
    if (size_t(-1) == newCurr) {
        assert(reinterpret_cast<PatriciaNode*>(m_mempool.data()) == a);
        revoke_list<MultiWriteMultiRead>(a, newSuffixNode, valsize, lzf);
        token->m_value = NULL; // fail flag
        return true;
    }
    if (a[ni.oldSuffixNode].flags & (FLAG_lazy_free|FLAG_lock)) {
        free_node<MultiWriteMultiRead>(newCurr, node_size(a+newCurr, valsize), lzf);
        free_node<MultiWriteMultiRead>(ni.oldSuffixNode, node_size(a+ni.oldSuffixNode, valsize), lzf);
        revoke_list<MultiWriteMultiRead>(a, newSuffixNode, valsize, lzf);
        if (csppDebugLevel >= 3)
            fprintf(stderr,
                "thread-%08zX: retry %zd, fork confict(curr = %zd)\n",
                ThisThreadID(), n_retry, curr);
        goto retry;
    }
    size_t zp_states_inc = SuffixZpathStates(chainLen, pos, key.n);
    assert(state_move(newCurr, key[pos]) == newSuffixNode);
    assert(state_move(newCurr, ni.zpath[zidx]) == ni.oldSuffixNode);
    init_token_value_mw(newCurr, ni.oldSuffixNode, newSuffixNode);
    update_curr_ptr(newCurr, 1 + chainLen);
    if (terark_likely(1 != ni.zpath.n)) {
        if (0 != zidx && zidx + 1 != size_t(ni.zpath.n))
            zp_states_inc++;
    }
    else { // 1 == ni.zpath.n
        zp_states_inc--;
    }
    // signed(zp_states_inc) may < 0, it's ok for fetch_add
    lzf->m_zpath_states += zp_states_inc;
    return true;
}
SplitZpath: {
    if (a[curr].meta.b_is_final) {
        ni.node_size += valsize;
    }
    lzf->m_stat.n_split += 1;
    revoke_expired_nodes<MultiWriteMultiRead>(*lzf, token);
    assert(ni.n_skip <= 10);
    assert(ni.n_children <= 256);
    cpfore(backup, &a[curr + ni.n_skip].child, ni.n_children);
    size_t valpos = size_t(-1);
    size_t newCurr = split_zpath<MultiWriteMultiRead>(curr, zidx, &ni, &valpos, valsize, lzf);
    if (size_t(-1) == newCurr) {
        token->m_value = NULL; // fail flag
        return true;
    }
    // ni.oldSuffixNode is the copy of curr with zpath updated to suffix_str
    // curr may be lazy_freed or locked when we copy it
    if (a[ni.oldSuffixNode].flags & (FLAG_lazy_free|FLAG_lock)) {
        free_node<MultiWriteMultiRead>(newCurr, node_size(a+newCurr, valsize), lzf);
        free_node<MultiWriteMultiRead>(ni.oldSuffixNode, node_size(a+ni.oldSuffixNode, valsize), lzf);
        if (csppDebugLevel >= 3)
            fprintf(stderr,
                "thread-%08zX: retry %zd, split confict(curr = %zd)\n",
                ThisThreadID(), n_retry, curr);
        goto retry;
    }
    init_token_value_mw(newCurr, ni.oldSuffixNode, -1);
    update_curr_ptr(newCurr, 1);
    if (terark_likely(1 != ni.zpath.n)) {
        if (0 != zidx && zidx + 1 != size_t(ni.zpath.n))
            lzf->m_zpath_states += 1;
    }
    else { // 1 == ni.zpath.n
        lzf->m_zpath_states += 1;
    }
    return true;
}

// FastNode: cnt_type = 15 always has value space
MarkFinalStateOnFastNode: {
    assert(0 == a[curr].meta.b_lazy_free);
    assert(0 == a[curr].meta.n_zpath_len);
    size_t valpos = AlignSize * (curr + 2 + 256);
    if (as_atomic(a[curr].flags).fetch_or(FLAG_set_final, std::memory_order_acq_rel) & FLAG_set_final) {
      // very rare: other thread set final
      // FLAG_set_final is permanent for FastNode: once set, never clear
      while (!(as_atomic(a[curr].flags).load(std::memory_order_relaxed) & FLAG_final)) {
          _mm_pause();
      }
      token->m_value = (char*)a->chars + valpos;
      goto HandleDupKey;
    }
    else {
      init_token_value_mw(-1, -1, -1);
      // value must be set before set FLAG_final
      as_atomic(a[curr].flags).fetch_or(FLAG_final, std::memory_order_release);
      lzf->m_n_words += 1;
      lzf->m_stat.n_mark_final += 1;
      lzf->m_adfa_total_words_len += key.size();
      return true;
    }
}
MarkFinalState: {
    ni.set(a + curr, 0, 0);
MarkFinalStateOmitSetNodeInfo:
    assert(15 != a[curr].meta.n_cnt_type);
    revoke_expired_nodes<MultiWriteMultiRead>(*lzf, token);
    size_t oldpos = AlignSize*curr;
    size_t newlen = ni.node_size + valsize;
    size_t newpos = m_mempool_lock_free.alloc(newlen);
    size_t newcur = newpos / AlignSize;
    size_t valpos = newpos + ni.va_offset;
    if (size_t(-1) == newpos) {
        token->m_value = NULL;
        return true;
    }
    assert(ni.n_skip <= 10);
    assert(ni.n_children <= 256);
    cpfore(backup, &a[curr + ni.n_skip].child, ni.n_children);
    tiny_memcpy_align_4(a->bytes + newpos,
                        a->bytes + oldpos, ni.va_offset);
    if (a[newcur].flags & (FLAG_lazy_free|FLAG_lock)) {
        free_node<MultiWriteMultiRead>(newcur, newlen, lzf);
        if (csppDebugLevel >= 3)
            fprintf(stderr,
                "thread-%08zX: retry %zd, mark final confict(curr = %zd)\n",
                ThisThreadID(), n_retry, curr);
        goto retry;
    }
    a[newcur].meta.b_is_final = true;
    init_token_value_mw(newcur, -1, -1);
    update_curr_ptr(newcur, 0);
    lzf->m_stat.n_mark_final += 1;
    return true;
}
HandleDupKey: {
    if (terark_unlikely(is_value_inited)) {
        assert(n_retry > 0);
        token->destroy_value(value, valsize);
    }
    return false;
}
}

template<MainPatricia::ConcurrentLevel ConLevel>
size_t
MainPatricia::add_state_move(size_t curr, byte_t ch,
                             size_t suffix_node, size_t valsize, LazyFreeListTLS* tls) {
    assert(curr < total_states());
    auto a = reinterpret_cast<PatriciaNode*>(m_mempool.data());
    size_t  cnt_type = a[curr].meta.n_cnt_type;
    size_t  zplen = a[curr].meta.n_zpath_len;
    size_t  aligned_valzplen = pow2_align_up(zplen, AlignSize);
    if (a[curr].meta.b_is_final) {
        aligned_valzplen += valsize;
    }
    size_t  node;
    auto insert_child =
    [&](byte_t new_cnt_type, size_t oldskip, size_t newskip, size_t oldnum,
        byte_t* newlabels, bool copy_skip_area = true)
    {
        if (copy_skip_area) { // skip/(meta+label) area
            memcpy(a + node, a + curr, AlignSize*oldskip);
        }
        a[node].meta.n_cnt_type = new_cnt_type;
        uint32_t* oldchilds = &a[curr + oldskip].child;
        uint32_t* newchilds = &a[node + newskip].child;
      #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
        size_t idx; // use linear search
        if (oldnum > 0 && ch < newlabels[oldnum-1]) {
            idx = size_t(-1);
            do idx++; while (newlabels[idx] < ch);
        }
        else {
            assert(0 == oldnum || ch > newlabels[oldnum-1]);
            idx = oldnum;
        }
      #else
        size_t idx = lower_bound_0(newlabels, oldnum, ch);
      #endif
        cpfore(newchilds, oldchilds, idx);
        newchilds[idx] = suffix_node;
        cpfore(newchilds + idx + 1, oldchilds + idx, oldnum-idx);
        cpback(newlabels + idx + 1, newlabels + idx, oldnum-idx);
        newlabels[idx] = ch;
        small_memcpy_align_4(newchilds + oldnum + 1,
                             oldchilds + oldnum, aligned_valzplen);
    };
  #define my_alloc_node(BaseUnits) \
    node = alloc_node<ConLevel>(AlignSize*(BaseUnits) + aligned_valzplen, tls); \
    if (ConLevel >= OneWriteMultiRead && mem_alloc_fail == node)           \
        return size_t(-1);                                                 \
    if (ConLevel < OneWriteMultiRead)                                      \
        a = reinterpret_cast<PatriciaNode*>(m_mempool.data())
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    switch (cnt_type) {
    default:
        assert(false);
        break;
    case 0:
        assert(a[curr].meta.b_is_final);
        my_alloc_node(2);
        insert_child(1, 1, 1, 0, a[node].meta.c_label);
        break;
    case 1:
        my_alloc_node(3);
        insert_child(2, 1, 1, 1, a[node].meta.c_label);
        break;
    case 2:
        my_alloc_node(5);
        insert_child(3, 1, 2, 2, a[node].meta.c_label);
        memset(a[node].meta.c_label + 3, 0, 3);
        break;
    case 3: case 4: case 5: // meta(1) + label(1) + child(n)
        my_alloc_node(2+cnt_type+1);
        insert_child(cnt_type+1, 2, 2, cnt_type, a[node].meta.c_label);
        break;
    case 6: // convert to meta(1) + label(4) + child(n)
        // now meta.c_label is n_children
        my_alloc_node(5+7);
        a[node] = a[curr];
        memset(a + node + 2, 0, AlignSize*3);
        memcpy(a + node + 1, a[curr].meta.c_label, 6);
        insert_child(7, 2, 5, 6, a[node+1].bytes, false);
        a[node].big.n_children = 7;
        break;
    case 7: { // cnt in [ 7, 16 ]
        size_t n_children = a[curr].big.n_children;
        assert(n_children >=  7);
        assert(n_children <= 16);
        if (n_children < 16) {
            my_alloc_node(5+n_children+1);
            insert_child(7, 5, 5, n_children, a[node+1].bytes);
            a[node].big.n_children = n_children + 1;
        }
        else { // n_children == 16
            my_alloc_node(10+17);
            a[node] = a[curr];
            a[node].meta.n_cnt_type = 8;
            a[node].big.n_children = 17;
            uint32_t* bits = &a[node+2].child;
            memset(bits, 0, AlignSize*8);
            for (size_t i = 0; i < n_children; i++) {
                terark_bit_set1(bits, a[curr+1].bytes[i]);
            }
            terark_bit_set1(bits, ch);
            size_t rank1 = 0;
            for (size_t i = 0; i < 4; ++i) {
                a[node+1].bytes[i] = byte_t(rank1);
                ullong w = unaligned_load<uint64_t>(bits, i);
                rank1 += fast_popcount64(w);
            }
            uint32_t* oldchilds = &a[curr +  5].child;
            uint32_t* newchilds = &a[node + 10].child;
            size_t idx = fast_search_byte_rs_idx(a[node+1].bytes, byte_t(ch));
            assert(idx<= 16);
            newchilds[idx] = suffix_node;
            cpfore(newchilds      , oldchilds    , idx);
            cpfore(newchilds+idx+1, oldchilds+idx, 16-idx + aligned_valzplen/AlignSize);
        }
        break; }
    case 8: { // cnt >= 17
            size_t n_children = a[curr].big.n_children;
            assert(n_children >=  17);
            assert(n_children <= 255);
            if (n_children >= 64 && 0 == a[curr].meta.n_zpath_len) {
                // alloc fast node, always alloc value space
                aligned_valzplen = valsize; // alloc value space
                my_alloc_node(2 + 256);
                a[node] = a[curr];
                a[node].meta.n_cnt_type = 15; // fast node
                a[node].big.n_children = 256;
                a[node+1].big.n_children = n_children + 1;
                a[node+1].big.unused = 0;
                uint32_t* bits = &a[curr+2].child;
                for (size_t i = 0, k = curr + 10; i < 256/32; ++i) {
                    uint32_t bm = bits[i];
                    size_t   ci = node + 2 + i*32;
                    if (uint32_t(-1) == bm) {
                        cpfore(a+ci, a+k, 32);
                        k += 32;
                    }
                    else if (bm) {
                        for (size_t j = 0; j < 32; ++j, ++ci, bm >>= 1) {
                            if (bm & 1)
                                a[ci].child = a[k++].child;
                            else
                                a[ci].child = nil_state;
                        }
                    }
                    else {
                        for (size_t j = 0; j < 32; ++j, ++ci) {
                            a[ci].child = nil_state;
                        }
                    }
                }
                assert(nil_state == a[node+2+ch].child);
                a[node+2+ch].child = suffix_node;
                break;
            }
            my_alloc_node(10+n_children+1);
            // bits[ch] is not set, but it's ok for fast_search_byte_rs_idx(ch)
            size_t idx = fast_search_byte_rs_idx(a[curr+1].bytes, byte_t(ch));
            size_t nb1 = AlignSize*(10+idx); // nb1 & nb2 may be up to 1KB
            size_t nb2 = AlignSize*(n_children-idx) + aligned_valzplen;
            auto*  Old = &a[curr].child;
            auto*  New = &a[node].child;
            memcpy(New         , Old       , nb1); New[10+idx] = suffix_node;
            memcpy(New+10+idx+1, Old+10+idx, nb2);
            uint32_t* bits = &a[node+2].child;
            terark_bit_set1(bits, ch);
            size_t rank1 = 0;
            for (size_t i = 0; i < 4; ++i) {
                a[node+1].bytes[i] = byte_t(rank1);
                ullong w = unaligned_load<uint64_t>(bits, i);
                rank1 += fast_popcount64(w);
            }
            a[node].big.n_children = n_children + 1;
        break; }
    case 15: // direct update curr_slot later
        TERARK_DIE("15 == cnt_type");
        break;
    }
  #if !defined(NDEBUG)
    if (ConLevel != MultiWriteMultiRead || falseConcurrent)
    {
        size_t suf2 = state_move(node, ch);
        assert(suf2 == suffix_node);
        if (15 != a[node].meta.n_cnt_type) {
            assert(num_children(node) == num_children(curr)+1);
        }
        assert(a[node].meta.n_cnt_type  == a[node].meta.n_cnt_type );
        assert(a[node].meta.b_is_final  == a[node].meta.b_is_final );
        assert(a[node].meta.n_zpath_len == a[node].meta.n_zpath_len);
        if (a[node].meta.n_zpath_len) {
            assert(get_zpath_data(node) == get_zpath_data(curr));
        }
        if (a[node].meta.b_is_final) {
            assert(memcmp(a->bytes + get_valpos(a, curr),
                          a->bytes + get_valpos(a, node), valsize) == 0);
        }
        if (csppDebugLevel >= 2) { // deep debug
            if (csppDebugLevel >= 3) {
                for(size_t cc = 0; cc < ch; ++cc) {
                    size_t t1 = state_move(curr, cc);
                    size_t t2 = state_move(node, cc);
                    assert(t1 == t2);
                }
                for(size_t cc = ch+1; cc < 256; ++cc) {
                    size_t t1 = state_move(curr, cc);
                    size_t t2 = state_move(node, cc);
                    assert(t1 == t2);
                }
            }
            for_each_move(node, [](size_t child, size_t ch) {});
        }
    }
  #endif
    return node;
}

static const size_t BULK_FREE_NUM = 32;

template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel>
void PatriciaMem<Align>::revoke_expired_nodes() {
    static_assert(ConLevel <= OneWriteMultiRead, "ConLevel <= OneWriteMultiRead");
    revoke_expired_nodes<ConLevel>(lazy_free_list(ConLevel), NULL);
}
template<size_t Align>
template<Patricia::ConcurrentLevel ConLevel, class LazyList>
void PatriciaMem<Align>::revoke_expired_nodes(LazyList& lazy_free_list, TokenBase* token) {
static long g_lazy_free_debug_level =
    getEnvLong("Patricia_lazy_free_debug_level", 0);

    if (ConLevel == SingleThreadStrict) {
        return;
    }
    ullong min_age = ConLevel >= MultiWriteMultiRead
                   ? token->m_min_age // Cheap to read token->m_min_age
                   : this->m_dummy.m_min_age;

    auto print = [&](const char* sig) {
        if (!lazy_free_list.empty()) {
            const LazyFreeItem& head = lazy_free_list.front();
            if (ConLevel >= MultiWriteMultiRead)
              fprintf(stderr
                , "%s:%08zX: is_head=%d, LazyFreeList.size = %zd, mem_size = %zd, min_age = %llu, trie.age = %llu, "
                  "head = { age = %llu, node = %llu, size = %llu }\n"
                , sig
                , token->m_thread_id
                , token->m_flags.is_head
                , lazy_free_list.size()
                , lazy_free_list.m_mem_size
                , (long long)min_age
                , (long long)token->m_link.verseq
                , (long long)head.age, (long long)head.node, (long long)head.size
              );
            else
              fprintf(stderr
                , "%s:%08zX: LazyFreeList.size = %zd, mem_size = %zd, min_age = %llu, trie.age = %llu, "
                  "head = { age = %llu, node = %llu, size = %llu }\n"
                , sig
                , ThisThreadID()
                , lazy_free_list.size()
                , lazy_free_list.m_mem_size
                , (long long)min_age
                , (long long)m_dummy.m_link.verseq
                , (long long)head.age, (long long)head.node, (long long)head.size
              );
        }
    };

    if (g_lazy_free_debug_level > 1)
        print("A");
  #if !defined(NDEBUG)
    //auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
  #endif
    auto tls = static_cast<LazyFreeListTLS*>(&lazy_free_list);
    size_t n = std::min(lazy_free_list.size(), BULK_FREE_NUM);
    for (size_t i = 0; i < n; ++i) {
        const LazyFreeItem& head = lazy_free_list.front();
        //assert(a[head.node].meta.b_lazy_free); // only for debug Patricia
        //assert(align_up(node_size(a + head.node, m_valsize), AlignSize) == head.size);
    // RetryCurr:
    //     if (ConLevel == MultiWriteMultiRead) {
    //         tls->m_revoke_try_cnt++;
    //     }
        if (head.age < min_age) {
            free_node<ConLevel>(head.node, head.size, tls);
            lazy_free_list.m_mem_size -= head.size;
            lazy_free_list.pop_front();
        } else {
            // if (ConLevel == MultiWriteMultiRead) {
            //     assert(tls == token->m_tls);
            //     if (tls->m_revoke_try_cnt >= BULK_FREE_NUM) {
            //         tls->m_revoke_try_cnt = 0;
            //         //// Expensive to read m_dummy.m_min_age
            //         ullong new_min_age = m_dummy.m_min_age;
            //         if (new_min_age != min_age) {
            //             TERARK_VERIFY(min_age < new_min_age);
            //             token->m_min_age = new_min_age;
            //             min_age = new_min_age;
            //             goto RetryCurr;
            //         }
            //     }
            // }
            break;
        }
    }
    if (g_lazy_free_debug_level > 0)
        print("B");
}

bool MainPatricia::lookup(fstring key, TokenBase* token) const {
  #if !defined(NDEBUG)
    if (m_writing_concurrent_level >= SingleThreadShared) {
        assert(NULL == mmap_base || -1 != m_fd);
        assert(NULL != m_dummy.m_link.next);
        assert(token->m_link.verseq <= m_token_tail->m_link.verseq);
        assert(token->m_link.verseq >= m_dummy.m_min_age);
        assert(ThisThreadID() == token->m_thread_id);
    }
    assert(this == token->m_trie);
  #endif

    auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
    size_t curr = initial_state;
    size_t pos = 0;

  // #define PatriciaTrie_lookup_readable
  #if defined(PatriciaTrie_lookup_readable)
    #define loop_condition nil_state != curr
  #else
    #define loop_condition
  #endif
    for (; loop_condition; pos++) {
        auto p = a + curr;
        size_t zlen = p->meta.n_zpath_len;
        size_t cnt_type = p->meta.n_cnt_type;
        if (zlen) {
            size_t skip = s_skip_slots[cnt_type];
            size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
            size_t kkn = key.size() - pos;
            size_t zkn = std::min(zlen, kkn);
            const byte_t* zptr = p[skip + n_children].bytes;
            auto pkey = key.udata() + pos;
            for (size_t zidx = 0; zidx < zkn; ++zidx) {
                if (terark_unlikely(pkey[zidx] != zptr[zidx])) {
                    goto Fail;
                }
            }
            if (terark_unlikely(kkn <= zlen)) {
                if (kkn == zlen && p->meta.b_is_final) {
                    token->m_value = (char*)zptr + pow2_align_up(zlen, AlignSize);
                    return true; // done
                }
                goto Fail;
            }
            pos += zlen;
        }
        else {
            if (terark_unlikely(key.size() == pos)) {
                if (p->meta.b_is_final) {
                    size_t skip = s_skip_slots[cnt_type];
                    size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
                    token->m_value = (void*)(p[skip + n_children].bytes);
                    return true; // done
                }
            //  goto Fail; // false positive assertion at Fail
                token->m_value = NULL;
                return false;
            }
        }
        byte_t ch = key.p[pos];
  #if defined(PatriciaTrie_lookup_readable)
        curr = state_move_fast(curr, ch, a);
  #else
    // manually inline, faster
  //#define PatriciaTrie_lookup_speculative
   #if defined(PatriciaTrie_lookup_speculative)
     #define  maybe_prefetch  prefetch
  // #define  maybe_prefetch(x)
        switch (cnt_type) {
        default: assert(false);              goto Fail;
        case 0:  assert(p->meta.b_is_final); goto Fail;
        case 1:
            curr = p[1].child;
            maybe_prefetch(a+curr);
            if (ch == p->meta.c_label[0])
                break;
            else
                goto Fail;
        case 2:
            if (ch == p->meta.c_label[0]) {
                curr = p[1+0].child;
                // do not prefetch
                break;
            }
            curr = p[1+1].child;
            maybe_prefetch(a+curr);
            if (ch == p->meta.c_label[1])
                break;
            else
                goto Fail;
        case 3: case 4: case 5: case 6:
            if (ch <= p->meta.c_label[cnt_type - 1]) {
                intptr_t idx = -1;
                do idx++; while (p->meta.c_label[idx] < ch);
                curr = p[2+idx].child;
                maybe_prefetch(a+curr);
                if (p->meta.c_label[idx] == ch)
                    break;
            }
            goto Fail;
        case 7: // cnt in [ 7, 16 ]
            assert(p->big.n_children >=  7);
            assert(p->big.n_children <= 16);
            if (ch <= p[1].bytes[p->big.n_children-1]) {
                intptr_t idx = -1;
                do idx++; while (p[1].bytes[idx] < ch);
                curr = p[5+idx].child;
                maybe_prefetch(a + curr);
                if (p[1].bytes[idx] == ch)
                    break;
            }
            goto Fail;
        case 8: // cnt >= 17
            assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
            {
                size_t i = ch / TERARK_WORD_BITS;
                size_t j = ch % TERARK_WORD_BITS;
                size_t w = unaligned_load<size_t>(p+2, i);
                size_t idx = p[1].bytes[i] + fast_popcount_trail(w, j);
                curr = p[10 + idx].child;
                maybe_prefetch(a + curr);
                if ((w >> j) & 1)
                    break;
            }
            goto Fail;
        case 15:
            assert(256 == p->big.n_children);
            curr = p[2 + ch].child; // may be nil_state
            // do not prefetch, root's child is expected in L1 cache
            if (terark_likely(nil_state != curr))
                break;
            else
                goto Fail;
        }
   #else // !PatriciaTrie_lookup_speculative
    #define fail_return  goto Fail
    #define move_to(next) { curr = next; break; }
    #define break_if_match_ch(skip, idx) \
            if (ch == p->meta.c_label[idx]) move_to(p[skip + idx].child)

        switch (cnt_type) {
        default:
            assert(false);
            fail_return;
        case 0:
            assert(p->meta.b_is_final);
            fail_return;
        case 2: break_if_match_ch(1, 1); no_break_fallthrough;
        case 1: break_if_match_ch(1, 0); fail_return;

      #if defined(__SSE4_2__) && !defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
        case 6: case 5: case 4:
            {
                auto label = p->meta.c_label;
                size_t idx = sse4_2_search_byte(label, cnt_type, ch);
                if (idx < cnt_type)
                    move_to(p[2 + idx].child);
            }
            fail_return;
      #else
        case 6: break_if_match_ch(2, 5); no_break_fallthrough;
        case 5: break_if_match_ch(2, 4); no_break_fallthrough;
        case 4: break_if_match_ch(2, 3); no_break_fallthrough;
      #endif
        case 3: break_if_match_ch(2, 2);
                break_if_match_ch(2, 1);
                break_if_match_ch(2, 0);
            fail_return;
        case 7: // cnt in [ 7, 16 ]
            {
                size_t n_children = p->big.n_children;
                assert(n_children >=  7);
                assert(n_children <= 16);
                auto label = p->meta.c_label + 2; // do not use [0,1]
              #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
                if (ch <= label[n_children-1]) {
                    size_t idx = size_t(-1);
                    do idx++; while (label[idx] < ch);
                    if (label[idx] == ch)
                        move_to(p[1 + 4 + idx].child);
                }
              #else
                size_t idx = fast_search_byte_max_16(label, n_children, ch);
                if (idx < n_children)
                    move_to(p[1 + 4 + idx].child);
              #endif
            }
            fail_return;
        case 8: // cnt >= 17
            assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
            if (terark_bit_test(&p[1+1].child, ch)) {
                size_t idx = fast_search_byte_rs_idx(p[1].bytes, ch);
                move_to(p[10 + idx].child);
            }
            fail_return;
        case 15:
            assert(256 == p->big.n_children);
            curr = p[2 + ch].child; // may be nil_state
            if (nil_state == curr) {
                fail_return;
            }
            break;
        }
    #undef break_if_match_ch
   #endif // PatriciaTrie_lookup_speculative
  #endif
    }
  Fail:
    //assert(pos < key.size());
    token->m_value = NULL;
    return false;
}

template<size_t Align>
size_t PatriciaMem<Align>::mem_alloc(size_t size) {
    size_t pos = alloc_aux(size);
    return pos / AlignSize;
}

template<size_t Align>
size_t PatriciaMem<Align>::alloc_aux(size_t size) {
    auto tls = static_cast<LazyFreeListTLS*>(&lazy_free_list(m_writing_concurrent_level));
    switch (m_writing_concurrent_level) {
    default:   assert(false); return size_t(-1);
    case MultiWriteMultiRead: return alloc_raw<MultiWriteMultiRead>(size, tls);
    case   OneWriteMultiRead: return alloc_raw<  OneWriteMultiRead>(size, tls);
    case  SingleThreadStrict:
    case  SingleThreadShared: return alloc_raw< SingleThreadShared>(size, tls);
    case     NoWriteReadOnly: assert(false); return size_t(-1);
    }
}

template<size_t Align>
void PatriciaMem<Align>::mem_free(size_t loc, size_t size) {
    size_t pos = loc * AlignSize;
    free_aux(pos, size);
}

template<size_t Align>
void PatriciaMem<Align>::free_aux(size_t pos, size_t size) {
    auto tls = static_cast<LazyFreeListTLS*>(&lazy_free_list(m_writing_concurrent_level));
    switch (m_writing_concurrent_level) {
    default:   assert(false);                                           break;
    case MultiWriteMultiRead: free_raw<MultiWriteMultiRead>(pos, size, tls); break;
    case   OneWriteMultiRead: free_raw<  OneWriteMultiRead>(pos, size, tls); break;
    case  SingleThreadStrict:
    case  SingleThreadShared: free_raw< SingleThreadShared>(pos, size, tls); break;
    case     NoWriteReadOnly: assert(false);                            break;
    }
}

template<size_t Align>
void PatriciaMem<Align>::mem_lazy_free(size_t loc, size_t size) {
    auto conLevel = m_writing_concurrent_level;
    if (conLevel >= SingleThreadShared) {
        ullong verseq = m_token_tail->m_link.verseq;
        auto& lzf = lazy_free_list(conLevel);
        lzf.push_back({ verseq, uint32_t(loc), uint32_t(size) });
        lzf.m_mem_size += size;
    }
    else {
        mem_free(loc, size);
    }
}

template<size_t Align>
void* PatriciaMem<Align>::alloc_appdata(size_t len) {
    assert(size_t(-1) == m_appdata_offset); // just allowing call once
    assert(size_t(00) == m_appdata_length);
    constexpr size_t appdata_align = 256; // max possible cache line size
    len = pow2_align_up(len, AlignSize);
    size_t extlen = len + appdata_align;
    size_t offset = alloc_aux(extlen);
    if (size_t(-1) == offset) {
        return nullptr;
    }
    size_t len1 = offset - offset % appdata_align;
    size_t len2 = extlen - len1 - len;
    assert(len1 % AlignSize == 0);
    assert(len2 % AlignSize == 0);
    if (len1) {
        free_aux(offset, len1);
    }
    if (len2) {
        free_aux(offset + len1 + len, len2);
    }
    m_appdata_offset = offset + len1;
    m_appdata_length = len;
    assert(m_appdata_offset % appdata_align == 0);
    auto h = const_cast<DFA_MmapHeader*>(mmap_base);
    if (h) {
        h->louds_dfa_min_zpath_id = uint32_t(m_appdata_offset / AlignSize);
        h->louds_dfa_cache_states = uint32_t(m_appdata_length / AlignSize);
    }
    return m_mempool.data() + m_appdata_offset;
}

template<size_t Align>
void PatriciaMem<Align>::SingleThreadShared_sync_token_list(byte_t* oldmembase) {
    assert(SingleThreadShared == m_writing_concurrent_level);
    assert(m_mempool.data() != oldmembase);
    TERARK_IF_DEBUG(size_t cnt = 0,);
    byte_t   * newmembase = m_mempool.data();
    for(auto curr = m_dummy.m_link.next; curr != NULL; curr = curr->m_link.next) {
        TokenBase* token = static_cast<TokenBase*>(curr);
        //assert(dynamic_cast<ReaderToken*>(token) != nullptr);
        if (byte_t* pvalue = (byte_t*)(token->m_value)) {
            size_t  offset = pvalue - oldmembase;
            token->m_value = newmembase + offset;
        }
        TERARK_IF_DEBUG(cnt++,);
    }
}

template<size_t Align>
void PatriciaMem<Align>::finish_load_mmap(const DFA_MmapHeader* base) {
    byte_t* bbase = (byte_t*)base;
    if (base->total_states >= max_state) {
        THROW_STD(out_of_range, "total_states=%lld",
                (long long)base->total_states);
    }
    if (m_valsize) {
        size_t valsize = base->louds_dfa_min_cross_dst;
        if (m_valsize != valsize) {
            THROW_STD(logic_error,
                "m_valsize = %zd, but Mmap.valsize = %zd",
                size_t(m_valsize), valsize);
        }
    }
    assert(base->num_blocks == 1);
    auto  blocks = base->blocks;
    if (AlignSize * base->total_states != blocks[0].length) {
        THROW_STD(out_of_range, "total_states=%lld  block[0].length = %lld, dont match"
            , (long long)base->total_states
            , (long long)blocks[0].length
        );
    }
    switch (m_mempool_concurrent_level) {
    default:   assert(false); break;
    case MultiWriteMultiRead: m_mempool_lock_free.destroy_and_clean(); break;
    case   OneWriteMultiRead: m_mempool_fixed_cap.destroy_and_clean(); break;
    case  SingleThreadStrict:
    case  SingleThreadShared: m_mempool_lock_none.destroy_and_clean(); break;
    case     NoWriteReadOnly: break; // do nothing
    }
    m_mempool_concurrent_level = NoWriteReadOnly;
    m_writing_concurrent_level = NoWriteReadOnly;
    m_mempool.risk_set_data(bbase + blocks[0].offset, blocks[0].length);
    m_valsize = base->louds_dfa_min_cross_dst; // use as m_valsize
    m_n_nodes = base->transition_num + 1;
    m_n_words = base->dawg_num_words;
    m_max_word_len = base->dfa_cluster_num;
    m_appdata_offset = size_t(base->louds_dfa_min_zpath_id) * AlignSize;
    m_appdata_length = size_t(base->louds_dfa_cache_states) * AlignSize;
}

template<size_t Align>
long PatriciaMem<Align>::prepare_save_mmap(DFA_MmapHeader* header,
                                     const void** dataPtrs) const {
    header->is_dag = true;
    header->num_blocks = 1;
    header->state_size = 4;
    header->transition_num          = this->m_n_nodes - 1;
    header->dawg_num_words          = this->m_n_words;
    header->louds_dfa_min_cross_dst = this->m_valsize;
    header->adfa_total_words_len    = this->m_adfa_total_words_len;
    header->dfa_cluster_num         = uint32_t(m_max_word_len);
    header->louds_dfa_min_zpath_id  = uint32_t(m_appdata_offset / AlignSize);
    header->louds_dfa_cache_states  = uint32_t(m_appdata_length / AlignSize);

    header->blocks[0].offset = sizeof(DFA_MmapHeader);
    header->blocks[0].length = m_mempool.size();
    dataPtrs[0] = this->m_mempool.data();

    return 0;
}

///////////////////////////////////////////////////////////////////////
#define CSPP_WAIT_FREE 0

Patricia::TokenBase::TokenBase() {
    m_tls   = NULL;
    m_live_verseq = 0;
    m_link.next  = NULL;
    m_link.verseq   = 0;
    m_min_age = 0;
    m_trie  = NULL;
    m_value = NULL;
    m_flags.state = ReleaseDone;
    m_flags.is_head = false;
    m_thread_id = UINT64_MAX;
}
Patricia::TokenBase::~TokenBase() {
    TERARK_VERIFY(m_flags.state == DisposeDone);
}

void Patricia::TokenBase::dispose() {
    int retry = 0;
    while (true) {
        auto flags = as_atomic(m_flags).load(std::memory_order_relaxed);
        auto trie = static_cast<MainPatricia*>(as_atomic(m_trie)
            .load(std::memory_order_relaxed));
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case ReleaseDone:
            TERARK_VERIFY(NULL == trie || this != trie->m_token_tail);
            if (cas_weak(m_flags, flags, {DisposeDone, false})) {
                delete this;
                return;
            }
            break;
        case AcquireLock:
            do { _mm_pause();
                flags = as_atomic(m_flags).load(std::memory_order_relaxed);
            } while (AcquireLock == flags.state);
            flags.state = AcquireIdle; // for next cas_weak
            no_break_fallthrough;
        case AcquireIdle:
            if (trie->m_writing_concurrent_level >= SingleThreadShared
                    || trie->m_token_qlen) {
                if (flags.is_head) {
                    mt_release(m_trie);
                } else if (cas_weak(m_flags, flags, {DisposeWait, false})) {
                    return;
                }
            }
            else {
                if (cas_strong(trie->m_head_lock, false, true)) {
                    if (cas_weak(m_flags, flags, {DisposeDone, false})) {
                        auto next = this->m_link.next;
                        if (next) {
                            this->m_link.next = next->m_link.next;
                        }
                        cas_unlock(trie->m_head_lock);
                        delete this;
                    }
                    return;
                }
                else {
                    if (cas_weak(m_flags, flags, {DisposeWait, false})) {
                        return;
                    }
                }
            }
            break;
        case ReleaseWait:
            if (cas_weak(m_flags, flags, {DisposeWait, flags.is_head})) {
                return;
            }
            break;
        }
        _mm_pause();
        INFO(1, "flags = {%s, %d}, retry = %d\n",
                enum_name(flags.state).c_str(), flags.is_head, ++retry);
    }
}

void Patricia::TokenBase::enqueue(Patricia* trie1) {
    auto trie = static_cast<MainPatricia*>(trie1);
    assert(AcquireDone == m_flags.state || AcquireLock == m_flags.state);
    assert(!m_flags.is_head);
    assert(trie->m_head_lock); // locked
    TokenBase* const p = trie->m_token_tail;
    const ullong verseq = p->m_link.verseq;
    this->m_link = {NULL, verseq+1};
    p->m_link.next = this;
    trie->m_tail = {this, verseq+1};
}

constexpr size_t MAX_DEL_PTRS = 32;

terark_forceinline
void Patricia::TokenBase::del_tokens(TokenBase* ptrs[], size_t num) {
    assert(num <= MAX_DEL_PTRS);
    for (size_t i = 0; i < num; ++i) {
        TokenBase* tok = ptrs[i];
        TERARK_VERIFY(DisposeWait == tok->m_flags.state);
        tok->m_flags.state = DisposeDone;
        delete tok;
    }
}

bool Patricia::TokenBase::dequeue(Patricia* trie1, TokenBase* delptrs[], size_t* pDelnum) {
    auto trie = static_cast<MainPatricia*>(trie1);
    assert(trie->m_head_lock);
    size_t     delnum = 0;
    TokenBase* curr = this;
    while (true) {
        assert(NULL != curr);
        TokenBase* next = curr->m_link.next;
        TokenFlags flags = curr->m_flags;

        // when acquire is ReleaseWait -> AcquireDone
        // this assert may be false positive
        // now we let is_head = true always in head lock
        // so restore the assert
        TERARK_VERIFY(false == flags.is_head);

        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireLock: TERARK_DIE("AcquireLock == m_flags.state"); break;
        case AcquireDone: {
            ullong min_age = curr->m_link.verseq;
        #if !defined(NDEBUG)
            auto p = curr->m_link.next;
            while (p) {
                assert(p->m_link.verseq >= p->m_min_age);
                assert(p->m_link.verseq >= min_age);
                p = p->m_link.next;
            }
        #endif
            if (cax_weak(curr->m_flags, flags, {AcquireDone, true})) {
                trie->m_dummy.m_min_age = min_age;
                trie->m_dummy.m_link.next = curr;
                curr->m_min_age = min_age;
                *pDelnum = delnum;
                return true; // done!!
            }
            else if (AcquireDone == flags.state) {
                // spuriously fail
            }
            else {
                // curr's thread call release/dispose
                TERARK_VERIFY(ReleaseWait == flags.state ||
                              AcquireIdle == flags.state ||
                              DisposeWait == flags.state);
                continue; // try 'curr' in next iteration
            }
            break; }
        case AcquireIdle:
            if (cas_weak(curr->m_flags, flags, {AcquireLock, false})) {
                ullong min_age = curr->m_link.verseq;
                trie->m_dummy.m_link.next = curr;
                trie->m_dummy.m_min_age = min_age;
                trie->m_head_is_idle = true;
                curr->m_min_age = min_age;
                as_atomic(curr->m_flags).store({AcquireIdle, true},
                        std::memory_order_release);
                *pDelnum = delnum;
                return true; // tread idle as live in normal case
            }
            else {
                _mm_pause(); // retry 'curr' in next iteration
            }
            break;
        case ReleaseWait:
            if (NULL != next) { // is not tail
                if (cas_weak(curr->m_flags, flags, {ReleaseDone, false})) {
                    //fprintf(stderr, "DEBUG: thread-%08zX ReleaseDone token of thread-%08zX\n", ThisThreadID(), curr->m_thread_id);
                    //curr->m_link = {UNLINKED_TOKEN, 0}; // curr is other thread's
                    curr = next;
                    as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
                }
                else {
                    _mm_pause(); // retry 'curr' in next iteration
                }
            }
            else {
                goto Done_HeadIsWait;
            }
            break;
        case DisposeWait:
            if (NULL != next) { // is not tail
                // now curr must before m_token_tail
                as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
                if (delnum < MAX_DEL_PTRS) {
                    delptrs[delnum++] = curr; // fast path
                } else { // unlikely, slow path, keep simple stupid
                    curr->m_flags.state = DisposeDone;
                    delete curr; // we delete other token
                }
                curr = next;
            }
            else {
                goto Done_HeadIsWait;
            }
            break;
        } // switch
    }
  Done_HeadIsWait:
    ullong min_age = curr->m_link.verseq;
    trie->m_dummy.m_link.next = curr;
    trie->m_dummy.m_min_age = min_age;
    curr->m_min_age = min_age;
    *pDelnum = delnum;
    return false;
}

void Patricia::TokenBase::mt_acquire(Patricia* trie1) {
  Retry:
    auto trie = static_cast<MainPatricia*>(trie1);
    auto flags = m_flags;
    switch (flags.state) {
    default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
    case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
    case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
    case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
    case AcquireLock:
        do { // wait for other threads to finish AcquireLock
            _mm_pause();
            // other thread change state from AcquireLock to AcquireIdle
            //  this thread change state from AcquireIdle to AcquireDone
        } while (!cas_weak(m_flags.state, AcquireIdle, AcquireDone));
        break;
    case AcquireIdle:
        while (!cas_weak(m_flags.state, AcquireIdle, AcquireDone)) {
            _mm_pause();
        }
        break;
    case ReleaseDone:
        m_flags = {AcquireDone, false};
        while (!cas_weak(trie->m_head_lock, false, true)) {
            _mm_pause();
        }
        as_atomic(trie->m_token_qlen).fetch_add(1, std::memory_order_relaxed);
        enqueue(trie);
        assert(NULL != trie->m_dummy.m_link.next);
        cas_unlock(trie->m_head_lock);
        break;
    case ReleaseWait:
        if (cax_weak(m_flags, flags, {AcquireDone, false})) {
            // acquire done, no one should change me
            assert(AcquireDone == m_flags.state);
        }
        else {
            if (AcquireDone == flags.state) {
                // spuriously fail
            }
            else {
                // we are unlocked by other threads
                // they can only make me ReleaseWait -> ReleaseDone
                // should be very unlikely
                TERARK_VERIFY(ReleaseDone == flags.state); // check compiler bug
                TERARK_VERIFY(ReleaseDone == m_flags.state);
            }
            _mm_pause();
            goto Retry;
        }
        break;
    }
    if (this == trie->m_dummy.m_link.next) {
        if (cas_strong(trie->m_head_lock, false, true)) {
            if (this == trie->m_dummy.m_link.next) {
                m_flags.is_head = true;
            }
            cas_unlock(trie->m_head_lock);
        }
    }
    else {
        // release may leave a dead(ReleaseWait) token at queue head
        // this dead token should be really deleted by acquire
        if (terark_unlikely(trie->m_head_is_dead)) {
            trie->reclaim_head();
        }
    }
}

void Patricia::TokenBase::mt_release(Patricia* trie1) {
    auto trie = static_cast<MainPatricia*>(trie1);
    int retry = 0;
    size_t delnum = 0;
    TokenBase* delptrs[MAX_DEL_PTRS];
    while (true) {
        auto flags = as_atomic(m_flags).load(std::memory_order_relaxed);
        auto next = this->m_link.next;
        if (flags.is_head) {
            switch (flags.state) {
            default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
            case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
            case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
            case ReleaseWait: TERARK_DIE("ReleaseWait == m_flags.state"); break;
            case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
            case AcquireDone:
                //may be false positive, this assert should be moved to later
                //assert(this == trie->m_dummy.m_link.next);
                if (NULL == next) {
                    // at this time point, 'this' is m_token_tail.
                    // queue has just one node which is 'this'
                    // do not change this->m_link.next, because other threads
                    // may calling acquire(append to the list)
                    // this is a dead token at head, may block the ring!
                    m_flags = {ReleaseWait, false};
                    trie->m_head_is_dead = true;
                    return;
                }
                if (trie->m_head_lock || !cas_weak(trie->m_head_lock, false, true)) {
                    // be wait free
                    m_flags = {ReleaseWait, false};
                    trie->m_head_is_dead = true;
                    return;
                }
                assert(this == trie->m_dummy.m_link.next); // now it must be true
                //assert(this != trie->m_token_tail); // may be false positive
                if (next->dequeue(trie, delptrs, &delnum)) {
                    //assert(this != trie->m_token_tail); // may be false positive
                    assert(this != trie->m_dummy.m_link.next);
                    //fprintf(stderr, "DEBUG: thread-%08zX ReleaseDone self token - dequeue ok\n", m_thread_id);
                    //m_link.verseq = 0; // DO NOT change m_link.verseq
                    //m_link.next = NULL; // safe, because this != trie->m_token_tail
                    m_min_age = m_link.verseq; // for later re-acquire
                }
                else {
                    //fprintf(stderr, "DEBUG: thread-%08zX ReleaseDone self token - dequeue fail\n", m_thread_id);
                    assert(this != trie->m_token_tail);
                    assert(this != trie->m_dummy.m_link.next);
                    assert(NULL != m_link.next);
                    assert(this != m_link.next);
                    trie->m_head_is_dead = true;
                }
                m_flags = {ReleaseDone, false};
                m_value = NULL;
                as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
                as_atomic(trie->m_head_lock).store(false, std::memory_order_release);
                del_tokens(delptrs, delnum);
                return;
            case AcquireLock:
                do { _mm_pause();
                    flags = as_atomic(m_flags).load(std::memory_order_relaxed);
                } while (AcquireLock == flags.state);
                flags = {AcquireIdle, true}; // for next cas_weak
                no_break_fallthrough;
            case AcquireIdle:
                if (NULL == next) {
                    if (cas_weak(m_flags, flags, {ReleaseWait, false})) {
                        trie->m_head_is_dead = true;
                        trie->m_head_is_idle = false;
                        return;
                    }
                    break; // break switch, retry
                }
                if (trie->m_head_lock || !cas_weak(trie->m_head_lock, false, true)) {
                    // be wait free
                    if (cas_weak(m_flags, flags, {ReleaseWait, false})) {
                        trie->m_head_is_dead = true;
                        trie->m_head_is_idle = false;
                        return;
                    }
                    break; // break switch, retry
                }
                if (cas_weak(m_flags, flags, {ReleaseDone, false})) {
                    assert(this == trie->m_dummy.m_link.next); // now it must be true
                    //assert(this != trie->m_token_tail); // may be false positive
                    if (next->dequeue(trie, delptrs, &delnum)) {
                        //assert(this != trie->m_token_tail); // may be false positive
                        assert(this != trie->m_dummy.m_link.next);
                        //fprintf(stderr, "DEBUG: thread-%08zX ReleaseDone self token - dequeue ok\n", m_thread_id);
                        //m_link.verseq = 0; // DO NOT change m_link.verseq
                        //m_link.next = NULL; // safe, because this != trie->m_token_tail
                        m_min_age = m_link.verseq; // for later re-acquire
                    }
                    else {
                        //fprintf(stderr, "DEBUG: thread-%08zX ReleaseDone self token - dequeue fail\n", m_thread_id);
                        assert(this != trie->m_token_tail);
                        assert(this != trie->m_dummy.m_link.next);
                        assert(NULL != m_link.next);
                        assert(this != m_link.next);
                        trie->m_head_is_dead = true;
                    }
                    cas_unlock(trie->m_head_lock);
                    del_tokens(delptrs, delnum);
                    return;
                }
                else { // retry
                    cas_unlock(trie->m_head_lock);
                }
                break; // break switch, retry
            } // switch
        }
        else {
            switch (flags.state) {
            default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
            case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
            case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
            case ReleaseWait: TERARK_DIE("ReleaseWait == m_flags.state"); break;
            case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
            case AcquireDone:
                if (cas_weak(m_flags, flags, {ReleaseWait, false})) {
                    //fprintf(stderr, "DEBUG: thread-%08zX ReleaseWait self token\n", m_thread_id);
                    m_value = NULL;
                    return;
                }
                else {
                    // old head set me as new head:
                    //   cas_strong can ensure is_head be true,
                    //   but we use cas_weak for performance
                    flags = m_flags;
                    TERARK_VERIFY_F(AcquireDone == flags.state,
                                "(%d %d)", flags.state, flags.is_head);
                    //TERARK_VERIFY(this == trie->m_dummy.m_link.next); // false positive
                    if (!flags.is_head) INFO(1, "cas_weak spuriously failed");
                }
                break; // retry
            case AcquireLock:
                do { _mm_pause();
                    flags = as_atomic(m_flags).load(std::memory_order_relaxed);
                } while (AcquireLock == flags.state);
                flags = {AcquireIdle, false}; // for next cas_weak
                no_break_fallthrough;
            case AcquireIdle:
                if (cas_weak(m_flags, flags, {ReleaseWait, false})) {
                    //fprintf(stderr, "DEBUG: thread-%08zX ReleaseWait self token\n", m_thread_id);
                    m_value = NULL;
                    return;
                }
                break; // retry
            } // switch
        }
        _mm_pause();
        INFO(1, "retry = %d, flags = {%s, %d}\n", ++retry
              , enum_cstr(flags.state, "Unkown"), flags.is_head);
    } // while
}

terark_forceinline
void Patricia::TokenBase::mt_update(Patricia* trie1) {
    auto trie = static_cast<MainPatricia*>(trie1);
    assert(m_flags.is_head);
    assert(AcquireDone == m_flags.state);
    if (m_link.next) {
        // if (this != trie->m_dummy.m_link.next) {
        //     // this immediate return is for wait free:
        //     //  1. update is an advise, not a promise.
        //     //  2. to be wait free, just do nothing
        //     //  3. let this thread to do useful work(after the return)
        //     //fprintf(stderr, "DEBUG: very rare: wait for other thread set queue head as me(this = %p)\n", this);
        //     return;
        // }
        if (trie->m_head_lock || !cas_weak(trie->m_head_lock, false, true)) {
            // be wait free, do nothing
            return;
        }
        //assert(!trie->m_head_is_dead); // false positive
        assert(m_link.verseq <= m_link.next->m_link.verseq);
        assert(m_link.verseq < m_link.next->m_link.verseq);
        TokenBase* delptrs[MAX_DEL_PTRS];
        size_t delnum = 0;
        auto new_head = this->m_link.next;
        m_flags.is_head = false;
        enqueue(trie);
        assert(this == trie->m_dummy.m_link.next);
        // verify because at least, I'm alive
        TERARK_VERIFY(new_head->dequeue(trie, delptrs, &delnum));
        //m_min_age = trie->m_dummy.m_min_age; // do not update
        cas_unlock(trie->m_head_lock);
        del_tokens(delptrs, delnum);
    }
    else {
        if (cas_weak(trie->m_head_lock, false, true)) {
            //assert(this == trie->m_dummy.m_link.next); // false positive
            ullong verseq = m_link.verseq;
            if (cas_strong(m_link, {NULL, verseq}, {NULL, verseq+1})) {
                TERARK_VERIFY_F(
                    cas_strong(trie->m_tail, {this, verseq}, {this, verseq+1}),
                    "tail real = {%p, %llu}, expected = {%p, %llu}",
                    trie->m_tail.next, (ullong)trie->m_tail.verseq,
                    this, (ullong)verseq);
                trie->m_dummy.m_min_age = verseq+1;
                this->m_min_age = verseq+1;
            }
            cas_unlock(trie->m_head_lock);
        }
        else {
            // do nothing, ignore this update
        }
    }
}

// it is a AcquireDone token calling this function
template<size_t Align>
terark_no_inline
void PatriciaMem<Align>::reclaim_head() {
    if (m_head_lock) {
        return;
    }
    if (terark_unlikely(!cas_weak(m_head_lock, false, true))) {
        return;
    }
    // before calling this function m_head_is_dead is true
    // but after locked, it may be false(very rarely)
    if (terark_unlikely(!m_head_is_dead && !m_head_is_idle)) {
        as_atomic(m_head_lock).store(false, std::memory_order_release);
        return;
    }
    TokenBase* delptrs[MAX_DEL_PTRS];
    size_t     delnum = 0;
    TokenBase* head = m_dummy.m_link.next;
    if (terark_unlikely(NULL == head)) {
        assert(NoWriteReadOnly == m_mempool_concurrent_level);
        assert(0 == m_token_qlen);
        cas_unlock(m_head_lock);
        return;
    }
    while (true) {
        TokenBase* next = head->m_link.next;
        auto flags = head->m_flags;
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case ReleaseWait:
            assert(false == flags.is_head);
            TERARK_VERIFY(NULL != next); // must have stopped at AcquireDone
            if (cas_weak(head->m_flags, flags, {ReleaseDone, false})) {
                head->m_link = {NULL, 0};
                as_atomic(m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
                //fprintf(stderr, "DEBUG: reclaim: thread-%08zX ReleaseDone token of thread-%08zX\n", ThisThreadID(), head->m_thread_id);
                head = next;
            } else {
                _mm_pause(); // retry loop
            }
            break;
        case DisposeWait:
            assert(false == flags.is_head);
            TERARK_VERIFY(NULL != next); // must have stopped at AcquireDone
            //fprintf(stderr, "DEBUG: reclaim: thread-%08zX DisposeDone token of thread-%08zX\n", ThisThreadID(), head->m_thread_id);
            if (delnum < MAX_DEL_PTRS) {
                delptrs[delnum++] = head; // keep fast run path simple fast
            } else { // this is very unlikely, just let it works
                head->m_flags.state = DisposeDone;
                delete head;
            }
            head = next;
            as_atomic(m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
            break;
        case AcquireDone:
            //assert(false == flags.is_head); // unlikely but possiblely fail
            if (cas_weak(head->m_flags, flags, {AcquireDone, true})) {
                //fprintf(stderr, "DEBUG: reclaim: thread-%08zX {AcquireDone,true} token of thread-%08zX\n", ThisThreadID(), head->m_thread_id);
                goto Done;
            } else {
                _mm_pause(); // retry loop
            }
            break;
        case AcquireIdle:
            // must have stopped at AcquireDone before reached NULL
            TERARK_VERIFY(NULL != next);
            if (cas_weak(head->m_flags, flags, {AcquireLock, false})) {
                ullong min_age = head->m_link.verseq;
                head->enqueue(this);
                head->m_min_age = min_age;
                as_atomic(head->m_flags).store({AcquireIdle, false},
                            std::memory_order_release);
                head = next;
            }
            else {
            // std::this_thread::yield(); // yield may cause bad stall
                _mm_pause(); // retry loop
            }
            break;
        case AcquireLock: TERARK_DIE("AcquireLock == m_flags.state"); break;
        }
    }
  Done:
    ullong min_age = head->m_link.verseq;
    m_dummy.m_link.next = head;
    m_dummy.m_min_age = min_age;
    m_head_is_dead = false;
    m_head_is_idle = false;
    head->m_min_age = min_age;
    as_atomic(m_head_lock).store(false, std::memory_order_release);
    TokenBase::del_tokens(delptrs, delnum);// do delete outside of lock
}

void Patricia::TokenBase::idle() {
  Retry:
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto conLevel = trie->m_writing_concurrent_level;
    assert(ThisThreadID() == m_thread_id);
    auto flags = m_flags;
    TERARK_VERIFY_F(AcquireDone == flags.state, "real = %s", enum_cstr(flags.state));
    if (conLevel >= SingleThreadShared) {
        if (flags.is_head) {
            mt_update(trie);
        }
        if (!cas_weak(m_flags, flags, {AcquireIdle, false}))
            goto Retry;
        m_live_verseq = m_link.verseq;
    }
    else if (trie->m_token_qlen) {
        m_flags = {AcquireIdle, false};
        if (flags.is_head)
            goto CleanForSetReadOnly;
    }
    else {
        m_flags.state = AcquireIdle;
    }
    return;
  CleanForSetReadOnly:
    while (!cas_weak(trie->m_head_lock, false, true)) {
        _mm_pause();
    }
    assert(this == trie->m_dummy.m_link.next);
    TokenBase* delptrs[MAX_DEL_PTRS];
    size_t delnum = 0;
    TokenBase* curr = this->m_link.next;
    while (curr) {
        TokenBase* next = curr->m_link.next;
        TokenFlags flags = curr->m_flags;
        TERARK_VERIFY(false == flags.is_head);
        switch (flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireLock: TERARK_DIE("AcquireLock == m_flags.state"); break;
        case AcquireDone:
            if (cas_weak(curr->m_flags, flags, {AcquireDone, true})) {
                ullong min_age = curr->m_link.verseq;
                trie->m_dummy.m_min_age = min_age;
                curr->m_min_age = min_age;
                goto SetQueueHead;
            }
            break; // curr not updated, retry curr
        case AcquireIdle:
            curr->m_link.next = NULL;
            curr = next;
            as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
            break;
        case ReleaseWait:
            if (cas_weak(curr->m_flags, flags, {ReleaseDone, false})) {
                curr = next;
                as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
            }
            break;
        case DisposeWait:
            if (delnum < MAX_DEL_PTRS) {
                delptrs[delnum++] = curr; // fast path
            } else { // unlikely, slow path, keep simple stupid
                curr->m_flags.state = DisposeDone;
                delete curr; // we delete other token
            }
            curr = next;
            as_atomic(trie->m_token_qlen).fetch_sub(1, std::memory_order_relaxed);
            break;
        } // switch
    }
    trie->m_token_tail = NULL;
  SetQueueHead:
    trie->m_dummy.m_link.next = curr;
    TERARK_VERIFY((NULL == curr) ^ (0 != trie->m_token_qlen));
    cas_unlock(trie->m_head_lock);
    del_tokens(delptrs, delnum);
}

void Patricia::TokenBase::release() {
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto conLevel = trie->m_writing_concurrent_level;
    assert(ThisThreadID() == m_thread_id);
    assert(AcquireDone == m_flags.state || AcquireIdle == m_flags.state);
    if (conLevel >= SingleThreadShared || trie->m_token_qlen) {
        assert(m_link.verseq <= trie->m_token_tail->m_link.verseq);
        mt_release(trie);
    }
    else {
        // may be MultiReadMultiWrite some milliseconds ago
        switch (m_flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case ReleaseWait: TERARK_DIE("ReleaseWait == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireIdle:
        case AcquireLock:
            while (!cas_weak(m_flags.state, AcquireIdle, ReleaseDone)) {
                _mm_pause(); // spin
            }
            break;
        case AcquireDone:
            m_flags.state = ReleaseDone;
            break;
        }
    }
    m_live_verseq = 0;
}

///////////////////////////////////////////////////////////////////////////////
// ReaderToken

Patricia::ReaderToken::ReaderToken() {
}

void Patricia::ReaderToken::acquire(Patricia* trie1) {
    assert(NULL != trie1);
    assert(NULL == m_trie || trie1 == m_trie);
    auto trie = static_cast<MainPatricia*>(trie1);
    m_value = NULL;
    m_trie = trie;
    m_thread_id = ThisThreadID();
    auto conLevel = trie->m_writing_concurrent_level;
    if (conLevel >= SingleThreadShared) {
        mt_acquire(trie);
    }
    else {
        // may be MultiReadMultiWrite some milliseconds ago
        switch (m_flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case AcquireDone: TERARK_DIE("AcquireDone == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case AcquireIdle:
        case AcquireLock:
            while (!cas_weak(m_flags.state, AcquireIdle, AcquireDone)) {
                _mm_pause(); // spin
            }
            break;
        case ReleaseWait:
        case ReleaseDone:
            m_flags.state = AcquireDone;
            break;
        }
        m_link = {NULL, 0};
        m_tls = NULL;
      #if !defined(NDEBUG)
        if (SingleThreadStrict == conLevel) {
            assert(0 == m_link.verseq);
            assert(NULL == m_tls);
            assert(NULL == m_link.next);
        }
      #endif
    }
}

Patricia::ReaderToken::~ReaderToken() {
    TERARK_VERIFY(DisposeDone == m_flags.state);
}

///////////////////////////////////////////////////////////////////////

// for derived class, if concurrent level >= OneWriteMultiRead,
// init_value can return false to notify init fail, in fail case,
// MainPatricia::insert() will return true and set token.value to NULL.
bool
Patricia::WriterToken::init_value(void* value, size_t valsize)
noexcept {
    assert(valsize % MainPatricia::AlignSize == 0);
    assert(valsize == m_trie->m_valsize);
    return true;
}

void Patricia::WriterToken::destroy_value(void* valptr, size_t valsize)
noexcept {
    // do nothing by default
}

Patricia::WriterToken::WriterToken() {
}


void Patricia::WriterToken::acquire(Patricia* trie1) {
    auto trie = static_cast<MainPatricia*>(trie1);
    auto conLevel = trie->m_writing_concurrent_level;
    assert(NULL == m_trie || trie == m_trie);
    assert(NoWriteReadOnly != conLevel);
  #if !defined(NDEBUG)
    auto flags = m_flags; // must load
    TERARK_ASSERT_F(ReleaseDone == flags.state || ReleaseWait == flags.state
                 || AcquireIdle == flags.state || AcquireLock == flags.state,
            "m_flags.state = %d(%s)", flags.state, enum_cstr(flags.state));
  #endif
    m_thread_id = ThisThreadID();
    if (MultiWriteMultiRead == conLevel) {
        mt_acquire(trie);
        auto tc = trie->m_mempool_lock_free.tls();
        if (nullptr == tc) {
            THROW_STD(runtime_error, "Alloc TLS fail");
        }
        auto lzf = static_cast<MainPatricia::LazyFreeListTLS*>(tc);
        m_tls = lzf;
    }
    else if (conLevel >= SingleThreadShared) {
        mt_acquire(trie);
    }
    else {
        m_flags.state = AcquireDone;
        m_link = {NULL, 0};
    }
    m_value = NULL;
    m_trie = trie;
}

Patricia::WriterToken::~WriterToken() {
    TERARK_VERIFY(DisposeDone == m_flags.state);
}

/// Iterator
size_t MainPatricia::first_child(const PatriciaNode* p, byte_t* ch) const {
  #if !defined(NDEBUG)
    auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
    size_t curr = p - a;
    assert(curr < total_states());
  #endif
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default: assert(false); break;
    case 0: assert(p->meta.b_is_final); return nil_state;
    case 1:
    case 2: *ch = p->meta.c_label[0]; return p[1].child;
    case 3:
    case 4:
    case 5:
    case 6: *ch = p->meta.c_label[0]; return p[2].child;
    case 7: *ch = p->meta.c_label[2]; return p[5].child;
    case 8: // cnt >= 17
        assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
        for (size_t i = 0; i < 4; ++i) {
            ullong b = unaligned_load<uint64_t>(p+2, i);
            if (b) {
                *ch = byte_t(i*64 + fast_ctz64(b));
                return p[10].child;
            }
        }
        assert(false);
        break;
    case 15:
        assert(256 == p->big.n_children);
        for (size_t ich = 0; ich < 256; ++ich) {
            uint32_t child = p[2+ich].child;
            if (nil_state != child) {
                *ch = byte_t(ich);
                return child;
            }
        }
        // children may be all nil_state
        break;
    }
    return nil_state;
}

size_t MainPatricia::last_child(const PatriciaNode* p, byte_t* ch) const {
  #if !defined(NDEBUG)
    auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
    size_t curr = p - a;
    assert(curr < total_states());
  #endif
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default: assert(false); break;
    case 0: assert(p->meta.b_is_final); return nil_state;
    case 1: *ch = p->meta.c_label[0]; return p[1].child;
    case 2: *ch = p->meta.c_label[1]; return p[2].child;
    case 3:
    case 4:
    case 5:
    case 6: *ch = p->meta.c_label[cnt_type-1];
            return p[2+cnt_type-1].child;
    case 7:
        {
            size_t n_children = p->big.n_children;
            assert(n_children >=  7);
            assert(n_children <= 16);
            *ch = p[1].bytes[n_children-1];
            return p[5+n_children-1].child;
        }
    case 8:
        assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
        {
            size_t n_children = p->big.n_children;
            assert(n_children >= 17);
            for (size_t i = 4; i-- > 0;) {
                ullong w = unaligned_load<uint64_t>(p+2, i);
                if (w) {
                    *ch = i*64 + terark_bsr_u64(w);
                    return p[10+n_children-1].child;
                }
            }
        }
        TERARK_DIE("cnt_type == 8, must found ch");
        break;
    case 15:
        assert(256 == p->big.n_children);
        for (size_t ich = 256; ich-- > 0; ) {
            uint32_t child = p[2+ich].child;
            if (nil_state != child) {
                *ch = byte_t(ich);
                return child;
            }
        }
        // children may be all nil_state
        break;
    }
    return nil_state;
}
size_t
MainPatricia::nth_child(const PatriciaNode* p, size_t nth, byte_t* ch)
const {
  #if !defined(NDEBUG)
    auto a = reinterpret_cast<const PatriciaNode*>(m_mempool.data());
    size_t curr = p - a;
    assert(curr < total_states());
  #endif
    size_t  cnt_type = p->meta.n_cnt_type;
    switch (cnt_type) {
    default: assert(false); return nil_state;
    case 0: assert(p->meta.b_is_final); return nil_state;
    case 1:
        assert(0 == nth);
        *ch = p->meta.c_label[0];
        return p[1].child;
    case 2:
        assert(nth < 2);
        *ch = p->meta.c_label[nth];
        return p[1+nth].child;
    case 3:
    case 4:
    case 5:
    case 6:
        assert(nth < cnt_type);
        *ch = p->meta.c_label[nth];
        return p[2 + nth].child;
    case 7:
        {
            assert(p->big.n_children >=  7);
            assert(p->big.n_children <= 16);
            assert(nth < p->big.n_children);
            *ch = p[1].bytes[nth];
            return p[5+nth].child;
        }
    case 8:
        assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
        {
            assert(p->big.n_children >=  17);
            assert(p->big.n_children <= 256);
            assert(nth < p->big.n_children);
            *ch = rs_select1(p[1].bytes, nth);
            return p[10+nth].child;
        }
    case 15:
        assert(256 == p->big.n_children);
        assert(nth < p[1].big.n_children);
    //  assert(*ch < nth); // nth is ignored in this case
    //  *ch must be last char
        for (size_t ich = *ch + 1; ich < 256; ich++) {
            uint32_t child = p[2+ich].child;
            if (nil_state != child) {
                *ch = byte_t(ich);
                return child;
            }
        }
        return nil_state;
    }
    return nil_state;
}

#define mark_word_end_zero_at(curr) \
    m_curr = curr; \
    m_word.grow_capacity(1); \
    m_word.end()[0] = '\0'
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class MainPatricia::IterImpl : public Patricia::Iterator {
public:
    struct Entry {
        uint32_t state;
        uint16_t n_children;
        uint08_t nth_child;
        uint08_t zpath_len;
        bool has_next() const { return nth_child + 1 < n_children; }
    };
    valvec<Entry> m_iter;
    size_t        m_flag;

    IterImpl();
    explicit IterImpl(const Patricia*);
    ~IterImpl();
    void token_detach_iter() final;
    void reset(const BaseDFA*, size_t root = 0) override final;
    bool seek_begin() override final;
    bool seek_end() override final;
    bool seek_lower_bound(fstring key) override final;
    bool incr() override final;
    bool decr() override final;
    size_t seek_max_prefix(fstring) override final;

    inline static void Entry_init(Entry* e, size_t curr, size_t zlen) {
        BOOST_STATIC_ASSERT(sizeof(Entry) == 8);
      #if 0
        e->state      = uint32_t(curr);
        e->n_children = 0;
        e->nth_child  = 0;
        e->zpath_len  = byte_t(zlen);
      #else
        *(uint64_t*)e = curr | uint64_t(zlen) << 56;
      #endif
    }

    terark_no_inline
    void append_lex_min_suffix(size_t root, const PatriciaNode* a) {
        BOOST_STATIC_ASSERT(sizeof(Entry) == 8);
        auto trie = static_cast<MainPatricia*>(m_trie);
        size_t zlen; const byte_t* zptr = NULL;
        size_t curr = root;
        assert(nil_state != root);
        const PatriciaNode* p;
        do {
            p = a + curr;
            size_t cnt_type = p->meta.n_cnt_type;
            size_t skip = MainPatricia::s_skip_slots[cnt_type];
            size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
            zlen = p->meta.n_zpath_len;
            zptr = p[skip + n_children].bytes;
            Entry e;
            e.state      = uint32_t(curr);
            e.n_children = uint16_t(n_children);
            e.nth_child  = 0;
            e.zpath_len  = byte_t(zlen);
            m_iter.push_back(e);
            byte_t* pch = m_word.grow_no_init(zlen + 1);
            pch = tiny_memcpy_align_1(pch, zptr, zlen);
            curr = trie->first_child(p, pch);
        } while (nil_state != curr && !p->meta.b_is_final);
        m_curr = m_iter.back().state;
        m_word.back() = '\0';
        m_word.pop_back();
        m_value = (void*)(zptr + pow2_align_up(zlen, AlignSize));
    }
    terark_no_inline
    void append_lex_max_suffix(size_t root, const PatriciaNode* a) {
        BOOST_STATIC_ASSERT(sizeof(Entry) == 8);
        auto trie = static_cast<MainPatricia*>(m_trie);
        size_t zlen = 0; const byte_t* zptr = NULL;
        size_t curr = root;
        assert(nil_state != root);
        do {
            auto p = a + curr;
            size_t cnt_type = p->meta.n_cnt_type;
            size_t skip = MainPatricia::s_skip_slots[cnt_type];
            size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
            zlen = p->meta.n_zpath_len;
            zptr = p[skip + n_children].bytes;
            Entry e;
            e.state      = uint32_t(curr);
            e.n_children = uint16_t(n_children);
            e.nth_child  = n_children - 1;
            e.zpath_len  = byte_t(zlen);
            m_iter.push_back(e);
            byte_t* pch = m_word.grow_no_init(zlen + 1);
            pch = tiny_memcpy_align_1(pch, zptr, zlen);
            curr = trie->last_child(p, pch);
        } while (nil_state != curr);
        m_iter.back().nth_child = 0;
        m_curr = m_iter.back().state;
      #if !defined(NDEBUG)
        if (15 == a[m_curr].meta.n_cnt_type) {
            assert(0 == a[m_curr+1].big.n_children);
        } else {
            assert(a[m_curr].meta.b_is_final);
        }
      #endif
        m_word.back() = '\0';
        m_word.pop_back();
        m_value = (void*)(zptr + pow2_align_up(zlen, AlignSize));
    }
    void reset1();
    size_t calc_word_len() const {
      #if 0
        size_t len = 0;
        for (size_t pos = 0; pos < m_iter.size(); ++pos)
            len += m_iter[pos].zlen;
        len += m_iter.size() - 1;
        return len;
      #else
        return m_word.size();
      #endif
    }
    terark_forceinline void ensure_token_acquired() {
        switch (m_flags.state) {
        default:          TERARK_DIE("UnknownEnum == m_flags.state"); break;
        case ReleaseDone: TERARK_DIE("ReleaseDone == m_flags.state"); break;
        case ReleaseWait: TERARK_DIE("ReleaseWait == m_flags.state"); break;
        case DisposeDone: TERARK_DIE("DisposeDone == m_flags.state"); break;
        case DisposeWait: TERARK_DIE("DisposeWait == m_flags.state"); break;
        case AcquireIdle:
        case AcquireLock: ReaderToken::acquire(m_trie); break;
        case AcquireDone: break;
        }
    }
    bool seek_lower_bound_impl(fstring key);
};

//static const size_t IterFlag_lower_bound_fast  = 1;
//static const size_t IterFlag_omit_token_update = 2;

MainPatricia::IterImpl::IterImpl(const Patricia* trie1)
  : Iterator(const_cast<Patricia*>(trie1))
{
    m_dfa = trie1;
    reset1();
}

void MainPatricia::IterImpl::reset1() {
    m_curr = size_t(-1);
    m_word.risk_set_size(0);
    m_iter.risk_set_size(0);
    m_value = NULL;
    if (NULL == m_trie) {
        m_iter.reserve(16); // fast malloc small
        m_flag = 0;
        return;
    }
    auto trie = static_cast<const MainPatricia*>(m_trie);
    auto conLevel = trie->m_writing_concurrent_level;
    if (NoWriteReadOnly == conLevel) {
        size_t cap = trie->m_max_word_len + 2;
        m_flag = 1;
        m_iter.reserve(cap);
        m_word.reserve(cap);
    }
    else {
        m_flag = 0;
        m_iter.reserve( 16); // fast malloc small
        m_word.reserve(128);
    }
}

MainPatricia::IterImpl::~IterImpl() {
    TERARK_VERIFY(DisposeDone == m_flags.state);
}

// after calling this function, this->update() will not re-search iter
// thus this Iterator can be used as an ReaderToken
void MainPatricia::IterImpl::token_detach_iter() {
    m_dfa = NULL;
    this->reset1();
}

void MainPatricia::IterImpl::reset(const BaseDFA* dfa, size_t root) {
    // root is ignored
    m_dfa = dfa;
    if (NULL == dfa) {
        if (NULL != m_trie) {
            THROW_STD(invalid_argument, "Can not reset non-null trie as null");
        }
    }
    else {
        assert(dynamic_cast<const MainPatricia*>(dfa) != NULL);
        auto trie = static_cast<const MainPatricia*>(dfa);
        if (m_trie == trie) {
            if (ReaderToken::m_flags.is_head) {
                ReaderToken::release();
                ReaderToken::acquire(const_cast<MainPatricia*>(trie));
            }
        }
        else if (NULL == m_trie) {
            ReaderToken::acquire(const_cast<MainPatricia*>(trie));
        }
        else if (trie->m_mempool_concurrent_level <= SingleThreadStrict) {
            ReaderToken::release();
            m_trie = NULL;
            ReaderToken::acquire(const_cast<MainPatricia*>(trie));
        }
        else {
            TERARK_THROW(std::invalid_argument
                , "Can not reset for different trie: old = %p, new = %p"
                , m_trie, trie);
        }
    }
    reset1();
}

bool MainPatricia::IterImpl::seek_begin() {
    assert(NULL != m_trie);
    return this->seek_lower_bound("");
}

bool MainPatricia::IterImpl::seek_end() {
    assert(NULL != m_trie);
    ensure_token_acquired();
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto a = reinterpret_cast<const PatriciaNode*>(trie->m_mempool.data());
    reset1();
    append_lex_max_suffix(initial_state, a);
    return a[m_curr].meta.b_is_final;
}

bool MainPatricia::IterImpl::seek_lower_bound(fstring key) {
    assert(NULL != m_trie);
    ensure_token_acquired();
    return seek_lower_bound_impl(key);
}

terark_flatten
bool MainPatricia::IterImpl::seek_lower_bound_impl(fstring key) {
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto a = reinterpret_cast<const PatriciaNode*>(trie->m_mempool.data());
    if (m_flag & 1) {
        goto seek_lower_bound_fast;
    }
    else {
        auto conLevel = trie->m_writing_concurrent_level;
        if (conLevel <= SingleThreadShared) {
            // m_iter is very likely over allocated,
            // because trie depth can not be efficiently maintained
            size_t cap = trie->m_max_word_len + 2;
            m_iter.ensure_capacity(cap);
            m_word.ensure_capacity(cap);
            if (NoWriteReadOnly == conLevel) {
                m_flag = 1;
            }
            goto seek_lower_bound_fast;
        }
    }
// seek_lower_bound_slow:
{
    reset1();
    size_t pos = 0;
    size_t curr = initial_state;
    for (;; pos++) {
        const auto p = a + curr;
        const size_t  cnt_type = p->meta.n_cnt_type;
        const size_t  zlen = p->meta.n_zpath_len;
        const byte_t* zptr = NULL;
        Entry e;
        Entry_init(&e, curr, zlen);
        if (zlen) {
            const size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
            const size_t skip = MainPatricia::s_skip_slots[cnt_type];
            zptr = p[skip + n_children].bytes;
            auto kkn = key.size() - pos;
            auto zkn = std::min(zlen, kkn);
            auto pkey = key.udata() + pos;
            for (size_t zidx = 0; zidx < zkn; ++zidx) {
                if (terark_unlikely(pkey[zidx] != zptr[zidx])) {
                    if (pkey[zidx] < zptr[zidx]) // is lower bound
                        goto CurrMinSuffix_HasZpath;
                    else // next word is lower_bound
                        goto rewind_stack_for_next;
                }
            }
            if (terark_unlikely(kkn <= zlen)) { // OK, current word is lower_bound
            CurrMinSuffix_HasZpath:
                e.n_children = uint16_t(n_children);
                m_iter.push_back(e);
                byte_t* pch = m_word.grow_no_init(zlen + 1);
                pch = tiny_memcpy_align_1(pch, zptr, zlen);
                if (p->meta.b_is_final) {
                    *pch = '\0';
                    m_curr = curr;
                    m_word.pop_back();
                    m_value = (void*)(zptr + pow2_align_up(zlen, AlignSize));
                }
                else {
                    size_t next = trie->first_child(p, pch);
                    if (terark_likely(nil_state != next)) {
                        prefetch(a+next);
                        append_lex_min_suffix(next, a);
                    }
                    else {
                        assert(m_iter.size() == 1);
                        assert(curr == initial_state);
                        reset1();
                        return false;
                    }
                }
                return true;
            }
            pos += zlen;
        }
        else {
            assert(pos <= key.size());
            if (terark_unlikely(key.size() == pos)) { // done
                assert(m_word.size() == pos);
                const size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
                e.n_children = uint16_t(n_children);
                m_iter.push_back(e);
                if (p->meta.b_is_final) {
                    const size_t skip = MainPatricia::s_skip_slots[cnt_type];
                    m_value = (void*)(p[skip + n_children].bytes);
                    m_curr = curr;
                    m_word.ensure_capacity(pos + 1);
                    m_word.data()[pos] = '\0';
                }
                else {
                    size_t next = trie->first_child(p, m_word.grow_no_init(1));
                    if (terark_likely(nil_state != next)) {
                        prefetch(a+next);
                        append_lex_min_suffix(next, a);
                    }
                    else {
                        assert(m_iter.size() == 1);
                        assert(curr == initial_state);
                        reset1();
                        return false;
                    }
                }
                return true;
            }
        }
        assert(pos < key.size());
        size_t ch = (byte_t)key.p[pos];
        assert(curr < trie->total_states());
        assert(ch <= 255);
  #undef  SetNth
  #define SetNth(Skip, Nth) curr = p[Skip+Nth].child; prefetch(a+curr); e.nth_child = Nth
        switch (cnt_type) {
        default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
        case 0:
            assert(p->meta.b_is_final);
            assert(calc_word_len() == m_word.size());
            goto rewind_stack_for_next;
        case 1:
            if (ch <= p->meta.c_label[0]) {
                curr = p[1].child; // SetNth(1, 0);
                prefetch(a+curr);
                e.n_children = 1;
                if (ch == p->meta.c_label[0])
                    goto NextLoopL;
                ch = p->meta.c_label[0];
                goto IterNextL;
            }
            goto rewind_stack_for_next;
        case 2:
            if (ch <= p->meta.c_label[1]) {
                e.n_children = 2;
                if (ch <= p->meta.c_label[0]) {
                    curr = p[1].child; // SetNth(1, 0);
                    prefetch(a+curr);
                    if (ch == p->meta.c_label[0])
                        goto NextLoopL;
                    ch = p->meta.c_label[0];
                }
                else {
                    SetNth(1, 1);
                    if (ch == p->meta.c_label[1])
                        goto NextLoopL;
                    ch = p->meta.c_label[1];
                }
                goto IterNextL;
            }
            goto rewind_stack_for_next;
        case 6:
        case 5:
        case 4:
        case 3:
            {
                auto label = p->meta.c_label;
                if (ch <= label[cnt_type-1]) {
                    size_t lo = size_t(-1);
                    do lo++; while (label[lo] < ch);
                    SetNth(2, lo);
                    e.n_children = uint16_t(cnt_type);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
            }
            goto rewind_stack_for_next;
        case 7: // cnt in [ 7, 16 ]
            {
                size_t n_children = p->big.n_children;
                assert(n_children >=  7);
                assert(n_children <= 16);
                auto label = p->meta.c_label + 2; // do not use [0,1]
              #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
                if (ch <= label[n_children-1]) {
                    size_t lo = size_t(-1);
                    do lo++; while (label[lo] < ch);
                    SetNth(5, lo);
                    e.n_children = uint16_t(n_children);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
              #else
                size_t lo = lower_bound_0(label, n_children, ch);
                if (lo < n_children) {
                    SetNth(5, lo);
                    e.n_children = uint16_t(n_children);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
              #endif
            }
            goto rewind_stack_for_next;
        case 8: // cnt >= 17
            {
                size_t n_children = p->big.n_children;
                assert(n_children == popcount_rs_256(p[1].bytes));
                assert(n_children == p->big.n_children);
                size_t lo = fast_search_byte_rs_idx(p[1].bytes, ch);
                if (lo < n_children) {
                    SetNth(10, lo);
                    e.n_children = uint16_t(n_children);
                    if (terark_bit_test(&p[2].child, ch))
                        goto NextLoopL;
                    ch = rs_next_one_pos(&p[2].child, ch);
                    goto IterNextL;
                }
            }
            goto rewind_stack_for_next;
        case 15:
          {
            assert(curr == initial_state || !m_word.empty());
            assert(256 == p->big.n_children);
            assert(0 == zlen);
            assert(m_word.capacity() >= 1);
            assert(m_iter.capacity() >= 1);
            e.n_children = 256;
            curr = p[2 + ch].child;
            if (terark_likely(nil_state != curr)) {
                prefetch(a+curr);
                e.nth_child = ch;
                m_word.push_back(ch);
                goto NextLoopNoZpathL;
            }
            else {
                while (++ch < 256) {
                    curr = p[2 + ch].child;
                    if (nil_state != curr) {
                        prefetch(a+curr);
                        e.nth_child = ch;
                        m_word.push_back(ch);
                        goto IterNextNoZpath;
                    }
                }
                if (m_word.empty()) {
                    assert(p-a == initial_state);
                    reset1();
                    return false;
                }
                goto rewind_stack_for_next;
            }
          }
        }
        assert(false);
      IterNextL:
        assert(nil_state != curr); // now curr is child
        *tiny_memcpy_align_1(m_word.grow_no_init(zlen+1), zptr, zlen) = ch;
      IterNextNoZpath:
        m_iter.push_back(e);
        append_lex_min_suffix(curr, a);
        return true;
      NextLoopL:
        *tiny_memcpy_align_1(m_word.grow_no_init(zlen+1), zptr, zlen) = ch;
      NextLoopNoZpathL:
        m_iter.push_back(e);
    }
    TERARK_DIE("Infinite Loop should not goes here");
}

#define CurrMinSuffix_HasZpath CurrMinSuffix_HasZpath_2
#define IterNextNoZpathL       IterNextNoZpathL_2
#define IterNextL              IterNextL_2
#define NextLoopL              NextLoopL_2
seek_lower_bound_fast:
{
    auto wp = m_word.data();
    auto ip = m_iter.data();
    size_t pos = 0;
    size_t curr = initial_state;
    for (;; pos++) {
        const auto p = a + curr;
        const size_t  cnt_type = p->meta.n_cnt_type;
              size_t  zlen = p->meta.n_zpath_len;
        const byte_t* zptr = NULL;
        Entry_init(ip, curr, zlen);
        if (zlen) {
            const size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
            const size_t skip = MainPatricia::s_skip_slots[cnt_type];
            zptr = p[skip + n_children].bytes;
            auto kkn = key.size() - pos;
            auto zkn = std::min(zlen, kkn);
            auto pkey = key.udata() + pos;
            for (size_t zidx = 0; zidx < zkn; ++zidx) {
                if (terark_unlikely(pkey[zidx] != zptr[zidx])) {
                    if (pkey[zidx] < zptr[zidx]) // is lower bound
                        goto CurrMinSuffix_HasZpath;
                    else // next word is lower_bound
                        goto RewindStackForNext;
                }
            }
            if (terark_unlikely(kkn <= zlen)) { // OK, current word is lower_bound
            CurrMinSuffix_HasZpath:
                ip->n_children = uint16_t(n_children);
                m_iter.risk_set_end(ip+1);
                do *wp++ = *zptr++, zlen--; while (zlen);
                if (p->meta.b_is_final) {
                    *wp = '\0';
                    m_word.risk_set_end(wp);
                    m_curr = curr;
                    m_value = (void*)(pow2_align_up(size_t(zptr), AlignSize));
                }
                else {
                    size_t next = trie->first_child(p, wp++);
                    if (terark_likely(nil_state != next)) {
                        prefetch(a+next);
                        m_word.risk_set_end(wp);
                        append_lex_min_suffix(next, a);
                    }
                    else {
                        assert(m_iter.size() == 1);
                        assert(curr == initial_state);
                        reset1();
                        return false;
                    }
                }
                return true;
            }
            pos += zlen;
        }
        else {
            assert(pos <= key.size());
            if (terark_unlikely(key.size() == pos)) { // done
                assert(size_t(wp - m_word.data()) == pos);
                const size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
                ip->n_children = uint16_t(n_children);
                m_iter.risk_set_end(ip+1);
                if (p->meta.b_is_final) {
                    const size_t skip = MainPatricia::s_skip_slots[cnt_type];
                    m_value = (void*)(p[skip + n_children].bytes);
                    m_curr = curr;
                    *wp = '\0';
                    m_word.risk_set_end(wp);
                }
                else {
                    size_t next = trie->first_child(p, wp++);
                    if (terark_likely(nil_state != next)) {
                        prefetch(a+next);
                        m_word.risk_set_end(wp);
                        append_lex_min_suffix(next, a);
                    }
                    else {
                        assert(m_iter.size() == 1);
                        assert(curr == initial_state);
                        reset1();
                        return false;
                    }
                }
                return true;
            }
        }
        assert(pos < key.size());
        size_t ch = (byte_t)key.p[pos];
        assert(curr < trie->total_states());
        assert(ch <= 255);
  #undef  SetNth
  #define SetNth(Skip, Nth) curr = p[Skip+Nth].child; prefetch(a+curr); ip->nth_child = Nth
        switch (cnt_type) {
        default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
        case 0:
            assert(p->meta.b_is_final);
            goto RewindStackForNext;
        case 1:
            if (ch <= p->meta.c_label[0]) {
                curr = p[1].child; // SetNth(1, 0);
                prefetch(a+curr);
                ip->n_children = 1;
                if (ch == p->meta.c_label[0])
                    goto NextLoopL;
                ch = p->meta.c_label[0];
                goto IterNextL;
            }
            goto RewindStackForNext;
        case 2:
            if (ch <= p->meta.c_label[1]) {
                ip->n_children = 2;
                if (ch <= p->meta.c_label[0]) {
                    curr = p[1].child; // SetNth(1, 0);
                    prefetch(a+curr);
                    if (ch == p->meta.c_label[0])
                        goto NextLoopL;
                    ch = p->meta.c_label[0];
                }
                else {
                    SetNth(1, 1);
                    if (ch == p->meta.c_label[1])
                        goto NextLoopL;
                    ch = p->meta.c_label[1];
                }
                goto IterNextL;
            }
            goto RewindStackForNext;
        case 6:
        case 5:
        case 4:
        case 3:
            {
                auto label = p->meta.c_label;
                if (ch <= label[cnt_type-1]) {
                    size_t lo = size_t(-1);
                    do lo++; while (label[lo] < ch);
                    SetNth(2, lo);
                    ip->n_children = uint16_t(cnt_type);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
            }
            goto RewindStackForNext;
        case 7: // cnt in [ 7, 16 ]
            {
                size_t n_children = p->big.n_children;
                assert(n_children >=  7);
                assert(n_children <= 16);
                auto label = p->meta.c_label + 2; // do not use [0,1]
              #if defined(TERARK_PATRICIA_LINEAR_SEARCH_SMALL)
                if (ch <= label[n_children-1]) {
                    size_t lo = size_t(-1);
                    do lo++; while (label[lo] < ch);
                    SetNth(5, lo);
                    ip->n_children = uint16_t(n_children);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
              #else
                size_t lo = lower_bound_0(label, n_children, ch);
                if (lo < n_children) {
                    SetNth(5, lo);
                    ip->n_children = uint16_t(n_children);
                    if (label[lo] == ch)
                        goto NextLoopL;
                    ch = label[lo];
                    goto IterNextL;
                }
              #endif
            }
            goto RewindStackForNext;
        case 8: // cnt >= 17
            {
                size_t n_children = p->big.n_children;
                assert(n_children == popcount_rs_256(p[1].bytes));
                assert(n_children == p->big.n_children);
              //#define patricia_seek_lower_bound_readable
              #ifdef  patricia_seek_lower_bound_readable
                size_t lo = fast_search_byte_rs_idx(p[1].bytes, ch);
                if (lo < n_children) {
                    SetNth(10, lo);
                    ip->n_children = uint16_t(n_children);
                    if (terark_bit_test(&p[2].child, ch))
                        goto NextLoopL;
                    ch = rs_next_one_pos(&p[2].child, ch);
                    goto IterNextL;
                }
              #else
                size_t i = ch / TERARK_WORD_BITS;
                size_t j = ch % TERARK_WORD_BITS;
                size_t w = unaligned_load<size_t>(p+2, i);
                size_t lo = p[1].bytes[i] + fast_popcount_trail(w, j);
                if (lo < n_children) {
                    SetNth(10, lo);
                    ip->n_children = uint16_t(n_children);
                    w >>= j;
                    if (w & 1)
                        goto NextLoopL;
                    w >>= 1;
                    if (w) {
                        ch += 1 + fast_ctz64(w);
                    }
                    else {
                        do w = unaligned_load<uint64_t>(p+2, ++i); while (!w);
                        assert(i < 4);
                        ch = i * 64 + fast_ctz64(w);
                    }
                    goto IterNextL;
                }
              #endif
            }
            goto RewindStackForNext;
        case 15:
          {
          //assert(curr == initial_state); // now it need not be initial_state
            assert(256 == p->big.n_children);
            assert(0 == zlen);
            assert(m_word.capacity() >= 1);
            assert(m_iter.capacity() >= 1);
            ip->n_children = 256;
            curr = p[2 + ch].child;
            if (terark_likely(nil_state != curr)) {
                prefetch(a+curr);
                ip->nth_child = ch;
                goto ThisLoopDone;
            }
            else {
                while (++ch < 256) {
                    curr = p[2 + ch].child;
                    if (nil_state != curr) {
                        prefetch(a+curr);
                        ip->nth_child = ch;
                        goto IterNextNoZpathL;
                    }
                }
                if (p == a) {
                    reset1();
                    return false;
                }
                goto RewindStackForNext;
            }
          }
        }
        assert(false);
      IterNextL:
        assert(nil_state != curr); // now curr is child
        while (zlen) *wp++ = *zptr++, zlen--;
      IterNextNoZpathL:
        *wp = ch;
        m_iter.risk_set_end(ip+1);
        m_word.risk_set_end(wp+1);
        append_lex_min_suffix(curr, a);
        return true;
      NextLoopL:
        assert(nil_state != curr); // now curr is child
        while (zlen) *wp++ = *zptr++, zlen--;
      ThisLoopDone:
        *wp++ = ch;
        ip++;
    }
    TERARK_DIE("Infinite Loop should not goes here");
  RewindStackForNext:
    m_iter.risk_set_end(ip);
    m_word.risk_set_end(wp);
//  goto rewind_stack_for_next;
}

rewind_stack_for_next:
    while (!m_iter.empty()) {
        auto& top = m_iter.back();
        if (top.has_next()) {
            top.nth_child++;
          #if 0
            size_t curr = trie->nth_child(a+top.state, top.nth_child, &m_word.back());
            if (nil_state != curr) {
                append_lex_min_suffix(curr, a);
                return true;
            }
          #else
            size_t  curr = top.state;
            auto    p = a + curr;
            size_t  cnt_type = p->meta.n_cnt_type;
            size_t  nth = top.nth_child;
            byte_t* pch = &m_word.back();
            assert (p->meta.n_zpath_len == top.zpath_len);
            switch (cnt_type) {
            default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
            case 0:  TERARK_VERIFY(p->meta.b_is_final); break;
            case 1:
                assert(0 == nth);
                assert(1 == top.n_children);
                *pch = p->meta.c_label[0];
                curr = p[1].child;
                break;
            case 2:
                assert(nth < 2);
                assert(2 == top.n_children);
                *pch = p->meta.c_label[nth];
                curr = p[1+nth].child;
                break;
            case 3:
            case 4:
            case 5:
            case 6:
                assert(nth < cnt_type);
                assert(cnt_type == top.n_children);
                *pch = p->meta.c_label[nth];
                curr = p[2+nth].child;
                break;
            case 7:
                {
                    assert(p->big.n_children >=  7);
                    assert(p->big.n_children <= 16);
                    assert(p->big.n_children == top.n_children);
                    assert(nth < p->big.n_children);
                    *pch = p[1].bytes[nth];
                    curr = p[5+nth].child;
                }
                break;
            case 8:
                assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
                {
                    assert(p->big.n_children >= 17);
                    assert(p->big.n_children <= 256);
                    assert(p->big.n_children == top.n_children);
                    assert(nth < p->big.n_children);
                    auto ch1 = rs_next_one_pos(&p[2].child, *pch);
                #if !defined(NDEBUG)
                    auto ch2 = rs_select1(p[1].bytes, nth);
                    assert(ch1 == ch2);
                #endif
                    *pch = ch1;
                    curr = p[10 + nth].child;
                }
                break;
            case 15:
                assert(256 == p->big.n_children);
                assert(256 == top.n_children);
            //  assert(curr == initial_state);
            //  assert(nth < p[1].big.n_children);
            //  assert(*pch < nth); // nth is ignored in this case
                for (size_t ich = *pch + 1; ich < 256; ich++) {
                    uint32_t child = p[2+ich].child;
                    if (nil_state != child) {
                        *pch = byte_t(ich);
                        curr = child;
                        top.nth_child = byte_t(ich);
                        goto switch_done;
                    }
                }
                // now there is no next char
                if (m_iter.size() == 1) {
                    reset1();
                    return false;
                }
                goto StackPop;
            }
          switch_done:;
            append_lex_min_suffix(curr, a);
            return true;
          #endif
        }
        if (terark_unlikely(m_iter.size() == 1)) {
            assert(top.state == initial_state);
            reset1();
            return false;
        }
      StackPop:
        assert(m_word.size() >= top.zpath_len + 1u);
        m_word.pop_n(top.zpath_len + 1);
        m_iter.pop_back();
    }
    return false;
}
// end seek_lower_bound_impl

terark_flatten
bool MainPatricia::IterImpl::incr() {
    ensure_token_acquired();
    if (terark_unlikely(m_iter.empty())) {
        return false;
    }
#if 0
    if (m_live_verseq < m_min_age) {
        m_live_verseq = m_link.verseq;
        if (!seek_lower_bound_impl(m_word)) {
            return false;
        }
    }
#endif
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto a = reinterpret_cast<const PatriciaNode*>(trie->m_mempool.data());
    assert(calc_word_len() == m_word.size());
    assert(0 == m_iter.back().nth_child);
    assert(m_curr == m_iter.back().state);
    size_t curr = trie->first_child(a + m_curr, m_word.grow_no_init(1));
    if (terark_unlikely(nil_state == curr)) {
        m_word.back() = '\0';
        m_word.pop_back();
        assert(a[m_curr].meta.b_is_final);
        size_t top = m_iter.size();
        size_t len = m_word.size();
      LoopForType15:
        while (!m_iter[--top].has_next()) {
            if (terark_unlikely(0 == top)) {
                reset1();
                return false;
            }
            assert(len >= m_iter[top].zpath_len + 1u);
            len -= m_iter[top].zpath_len + 1;
        }
        m_word.risk_set_size(len);
        m_iter.risk_set_size(top + 1);
        m_iter[top].nth_child++;
        assert(m_iter[top].nth_child > 0);
        assert(m_iter[top].nth_child < m_iter[top].n_children);
        assert(calc_word_len() == len);
        curr = m_iter[top].state;
        size_t  ch = size_t(-1);
        auto    p = a + curr;
        size_t  cnt_type = p->meta.n_cnt_type;
        size_t  nth_child = m_iter[top].nth_child;
        assert (nth_child < m_iter[top].n_children);
        switch (cnt_type) {
        default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
        case 0:  TERARK_DIE("0 == cnt_type"); break;
        case 1:
            assert(nth_child < 1);
            assert(m_iter[top].n_children == 1);
            ch = p->meta.c_label[0];
            curr = p[1].child;
            break;
        case 2:
            assert(nth_child < 2);
            assert(m_iter[top].n_children == 2);
            ch = p->meta.c_label[nth_child];
            curr = p[1+nth_child].child;
            break;
        case 6:
        case 5:
        case 4:
        case 3:
            assert(nth_child < cnt_type);
            assert(m_iter[top].n_children == cnt_type);
            ch = p->meta.c_label[nth_child];
            curr = p[2+nth_child].child;
            break;
        case 7: // cnt in [ 7, 16 ]
            assert(nth_child < p->big.n_children);
            assert(m_iter[top].n_children == p->big.n_children);
            ch = p[1].bytes[nth_child];
            curr = p[5+nth_child].child;
            break;
        case 8: // cnt >= 17
            assert(m_iter[top].n_children == p->big.n_children);
            assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
            ch = m_word.data()[len]; // prev char in stack
            ch = rs_next_one_pos(&p[2].child, ch);
            curr = p[10 + nth_child].child;
            break;
        case 15:
        //  assert(0 == len);
        //  assert(0 == top);
            assert(256 == m_iter[top].n_children);
            assert(256 == p->big.n_children);
            assert(0 == p->meta.n_zpath_len);
        //  assert(curr == initial_state);
            ch = m_word.data()[len] + 1;
            for (; ch < 256; ch++) {
                if (nil_state != p[2+ch].child) {
                    m_iter[top].nth_child = ch;
                    curr = p[2+ch].child;
                    goto switch_done;
                }
            }
            if (0 == top) {
                assert(0 == len);
                assert(initial_state == curr);
                reset1();
                return false;
            }
            len--;
            goto LoopForType15;
        }
      switch_done:
        assert(calc_word_len() == m_word.size());
        m_word.unchecked_push_back(ch);
    }
    append_lex_min_suffix(curr, a);
    return true;
}

terark_flatten
bool MainPatricia::IterImpl::decr() {
    ensure_token_acquired();
    if (m_iter.empty()) {
        return false;
    }
#if 0
    if (m_live_verseq < m_min_age) {
        m_live_verseq = m_link.verseq;
        //std::string curr_key((const char*)m_word.data(), m_word.size());
        fstring curr_key((const char*)m_word.data(), m_word.size());
        if (!seek_lower_bound_impl(curr_key)) {
            return seek_end();
        }
    }
#endif
    assert(calc_word_len() == m_word.size());
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto a = reinterpret_cast<const PatriciaNode*>(trie->m_mempool.data());
    assert(m_curr == m_iter.back().state);
    assert(a[m_curr].meta.b_is_final);
    assert(0 == m_iter.back().nth_child);
    size_t top = m_iter.size();
    size_t len = m_word.size();
    if (terark_unlikely(0 == --top)) {
        reset1();
        return false;
    }
    assert(len >= m_iter[top].zpath_len + 1u);
    len -= m_iter[top].zpath_len + 1;
  LoopForType15:
    while (m_iter[--top].nth_child == 0) {
        size_t curr = m_iter[top].state;
        if (a[curr].meta.b_is_final) {
            m_iter.risk_set_size(top+1);
            m_word.risk_set_size(len);
            mark_word_end_zero_at(curr);
            m_value = trie->get_valptr(curr);
            return true;
        }
        if (terark_unlikely(0 == top)) {
            reset1();
            return false;
        }
        assert(len >= m_iter[top].zpath_len + 1u);
        len -= m_iter[top].zpath_len + 1;
    }
    assert(m_iter[top].n_children >= 2);
    assert(m_iter[top].nth_child > 0);
    assert(m_iter[top].nth_child < m_iter[top].n_children);
    m_iter[top].nth_child--;
    m_iter.risk_set_size(top + 1);
    m_word.risk_set_size(len);
    assert(calc_word_len() == len);
    size_t  curr = m_iter[top].state;
    size_t  ch = size_t(-1);
    auto    p = a + curr;
    size_t  cnt_type = p->meta.n_cnt_type;
    size_t  nth_child = m_iter[top].nth_child;
    switch (cnt_type) {
    default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
    case 0:
    case 1:
        assert(nth_child < cnt_type);
        assert(m_iter[top].n_children == cnt_type);
        TERARK_DIE("cnt_type must not be {0,1}"); break;
        break;
    case 2:
        assert(nth_child < 2);
        assert(m_iter[top].n_children == 2);
        ch = p->meta.c_label[nth_child];
        curr = p[1+nth_child].child;
        break;
    case 6:
    case 5:
    case 4:
    case 3:
        assert(nth_child < cnt_type);
        assert(m_iter[top].n_children == cnt_type);
        ch = p->meta.c_label[nth_child];
        curr = p[2+nth_child].child;
        break;
    case 7: // cnt in [ 7, 16 ]
        assert(nth_child < p->big.n_children);
        assert(m_iter[top].n_children == p->big.n_children);
        ch = p[1].bytes[nth_child];
        curr = p[5 + nth_child].child;
        break;
    case 8: // cnt >= 17
        assert(m_iter[top].n_children == p->big.n_children);
        assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
        ch = m_word.data()[len]; assert(ch > 0); // larger char
        ch = rs_prev_one_pos(&p[2].child, ch);
      #if !defined(NDEBUG)
        {
            size_t ch1 = rs_select1(p[1].bytes, nth_child);
            assert(ch == ch1);
        }
      #endif
        curr = p[10 + nth_child].child;
        break;
    case 15:
    //  assert(0 == top);
    //  assert(0 == len);
        assert(256 == m_iter[top].n_children);
        assert(256 == p->big.n_children);
    //  assert(curr == initial_state);
        ch = m_word.data()[len];
        while (ch) {
            ch--;
            if (nil_state != p[2+ch].child) {
                curr = p[2 + ch].child;
                m_iter[top].nth_child = ch;
                goto switch_done;
            }
        }
        m_word.data()[len] = '\0';
        if (p->meta.b_is_final) {
            m_iter[top].nth_child = 0;
            m_curr = curr;
            m_value = trie->get_valptr(curr);
            return true;
        }
        if (0 == top) {
            assert(curr == initial_state);
            reset1();
            return false;
        }
        len--;
        goto LoopForType15;
    }
  switch_done:
    assert(calc_word_len() == m_word.size());
    m_word.unchecked_push_back(ch);
    append_lex_max_suffix(curr, a);
    return true;
}

size_t MainPatricia::IterImpl::seek_max_prefix(fstring key) {
    ensure_token_acquired();
    reset1();
    auto trie = static_cast<MainPatricia*>(m_trie);
    auto a = reinterpret_cast<const PatriciaNode*>(trie->m_mempool.data());
    size_t last_stack_top = 0;
    size_t last_match_len = 0;
    size_t curr = initial_state;
    size_t pos = 0;
    for (;; ++pos) {
        const auto p = a + curr;
        const size_t zlen = p->meta.n_zpath_len;
        const size_t cnt_type = p->meta.n_cnt_type;
        const size_t skip = MainPatricia::s_skip_slots[cnt_type];
        const size_t n_children = cnt_type <= 6 ? cnt_type : p->big.n_children;
        Entry* e = m_iter.grow_no_init(1);
        e->state = curr;
        e->zpath_len = zlen;
        e->nth_child = 0;
        e->n_children = n_children;
        if (zlen) {
            size_t zkn = std::min(key.size() - pos, zlen);
            const byte_t* zptr = p[skip + n_children].bytes;
            const byte_t* pkey = key.udata() + pos;
            for (size_t j = 0; j < zkn; ++j) {
                if (pkey[j] != zptr[j]) { // OK, current word has max matching prefix
                    pos += j;
                    goto RestoreLastMatch;
                }
            }
            pos += zkn;
            if (zkn < zlen) { // OK, current word has max matching prefix
                goto RestoreLastMatch;
            }
        }
        assert(pos <= key.size());
        if (p->meta.b_is_final) {
            last_stack_top = m_iter.size();
            last_match_len = pos;
        }
        if (key.size() == pos) { // done
            goto RestoreLastMatch;
        }
        assert(n_children > 0);
      #define match_nth_char(skip, nth) \
              curr = p[skip+nth].child; \
              e->nth_child = nth;       \
              break
      //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        const auto ch = (byte_t)key[pos];
        switch (cnt_type) {
        default: TERARK_DIE("bad cnt_type = %zd", cnt_type); break;
        case 0:  TERARK_DIE("0 == cnt_type"); break;
        case 2: if (ch == p->meta.c_label[1]) { match_nth_char(1, 1); } no_break_fallthrough;
        case 1: if (ch == p->meta.c_label[0]) { match_nth_char(1, 0); }
                goto RestoreLastMatch;
        case 6: if (ch == p->meta.c_label[5]) { match_nth_char(2, 5); } no_break_fallthrough;
        case 5: if (ch == p->meta.c_label[4]) { match_nth_char(2, 4); } no_break_fallthrough;
        case 4: if (ch == p->meta.c_label[3]) { match_nth_char(2, 3); } no_break_fallthrough;
        case 3: if (ch == p->meta.c_label[2]) { match_nth_char(2, 2); }
                if (ch == p->meta.c_label[1]) { match_nth_char(2, 1); }
                if (ch == p->meta.c_label[0]) { match_nth_char(2, 0); }
                goto RestoreLastMatch;
        case 7: // cnt in [ 7, 16 ]
            assert(n_children == p->big.n_children);
            assert(n_children >=  7);
            assert(n_children <= 16);
            {
                auto label = p->meta.c_label + 2; // do not use [0,1]
                if (ch <= label[n_children-1]) {
                    size_t lo = size_t(-1);
                    do lo++; while (label[lo] < ch);
                    if (label[lo] == ch) {
                        match_nth_char(5, lo);
                    }
                }
                goto RestoreLastMatch;
            }
        case 8: // cnt >= 17
            assert(n_children == p->big.n_children);
            assert(popcount_rs_256(p[1].bytes) == p->big.n_children);
            if (terark_bit_test(&a[curr+1+1].child, ch)) {
                size_t lo = fast_search_byte_rs_idx(a[curr + 1].bytes, byte_t(ch));
                match_nth_char(10, lo);
            }
            goto RestoreLastMatch;
        case 15:
            assert(256 == p->big.n_children);
            if (nil_state != p[2 + ch].child) {
                match_nth_char(2, ch);
            }
            goto RestoreLastMatch;
        }
    }
  RestoreLastMatch:
    if (last_stack_top) {
        m_iter[last_stack_top - 1].nth_child = 0;
        m_curr = m_iter[last_stack_top - 1].state;
    }
    else {
        m_curr = size_t(-1);
    }
    m_word.ensure_capacity(last_match_len + 1);
    m_word.assign(key.udata(), last_match_len);
    m_word.end()[0] = '\0';
    m_iter.risk_set_size(last_stack_top);
    return pos; // max partial match len
}

#if defined(TERARK_PATRICIA_USE_CHEAP_ITERATOR)
ADFA_LexIterator* MainPatricia::adfa_make_iter(size_t) const {
    return new IterImpl(this);
}
ADFA_LexIterator16* MainPatricia::adfa_make_iter16(size_t) const {
    return NULL;
}
#endif

Patricia::Iterator* Patricia::new_iter(size_t root) const {
    auto iter = this->adfa_make_iter(root);
    return static_cast<Iterator*>(iter);
}

/// load & save

template<class DataIO>
void DataIO_loadObject(DataIO& dio, MainPatricia& dfa) {
    THROW_STD(logic_error, "Not supported");
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const MainPatricia& dfa) {
    THROW_STD(logic_error, "Not supported");
}

template class PatriciaMem<4>;
template class PatriciaMem<8>;
typedef MainPatricia DynamicPatriciaTrie;
TMPL_INST_DFA_CLASS (DynamicPatriciaTrie)

///////////////////////////////////////////////////////////////////////////////

Patricia*
Patricia::create(size_t valsize, size_t maxMem, ConcurrentLevel concurrentLevel) {
    return new MainPatricia(valsize, maxMem, concurrentLevel);
}

Patricia::MemStat Patricia::mem_get_stat() const {
    MemStat ms;
    mem_get_stat(&ms);
    return ms;
}

Patricia::Iterator::Iterator(Patricia* trie)
 : ADFA_LexIterator(valvec_no_init())
{
    ReaderToken::acquire(trie);
}

Patricia::Iterator::~Iterator() {
    TERARK_VERIFY(DisposeDone == m_flags.state);
}

void Patricia::Iterator::dispose() {
    if (AcquireDone == m_flags.state) {
        TERARK_VERIFY(ThisThreadID() == m_thread_id);
        release(); // auto release on dispose iterator
    }
    ReaderToken::dispose();
}

} // namespace terark

