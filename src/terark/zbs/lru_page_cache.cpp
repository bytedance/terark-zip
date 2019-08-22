#include "lru_page_cache.hpp"
#include <thread>
#include <terark/util/throw.hpp>
#include <terark/hash_common.hpp>
#include <terark/fstring.hpp>
//#include <terark/io/byte_swap.hpp>
#include <terark/util/byte_swap_impl.hpp>
#include <terark/util/function.hpp>
#include <terark/bitmap.hpp>
#include <terark/num_to_str.hpp>
#include <atomic>
#include <boost/preprocessor/cat.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <terark/thread/fiber_aio.hpp>

#if (defined(_WIN32) || defined(_WIN64)) && !defined(__CYGWIN__)
	#define WIN32_LEAN_AND_MEAN
	#define NOMINMAX
	#include <Windows.h>
#else
	#include <unistd.h> // for usleep
	#include <sys/mman.h>
#endif

#if defined(TERARK_WITH_TBB)
  #include <tbb/tbb.h>
  typedef tbb::queuing_mutex           MyMutex;
//typedef tbb::spin_mutex              MyMutex;
  typedef MyMutex::scoped_lock         ScopeLock;
  #define MY_MUTEX_PADDING             char BOOST_PP_CAT(padding,__LINE__)[64];
#else
  #include <mutex>
  typedef std::mutex                   MyMutex;
  typedef std::lock_guard<std::mutex>  ScopeLock;
  #define MY_MUTEX_PADDING
#endif

#undef PAGE_SIZE

namespace terark {

template<class T>
class SimplePermanentID {
	valvec<T> m_objects;
	febitvec  m_dropped;
	size_t    m_min_id;
	size_t    m_num_free;
public:
	explicit SimplePermanentID(size_t initial_cap = 0)
		: m_objects(align_up(initial_cap, 64), valvec_reserve())
		, m_dropped(align_up(initial_cap, 64), valvec_reserve())
		, m_min_id(0)
		, m_num_free(0)
	{}
	size_t push(const T& x) {
		size_t id = m_min_id + m_objects.size();
		m_objects.push_back(x);
		m_dropped.push_back(false);
		return id;
	}
	T& operator[](size_t id) {
		assert(id <  m_min_id + m_objects.size());
		assert(id >= m_min_id);
		size_t index = id - m_min_id;
		assert(m_dropped.is0(index));
		return m_objects[index];
	}
	const T& operator[](size_t id) const {
		assert(id <  m_min_id + m_objects.size());
		assert(id >= m_min_id);
		size_t index = id - m_min_id;
		assert(m_dropped.is0(index));
		return m_objects[index];
	}
	bool is_dropped(size_t id) const {
		assert(id <  m_min_id + m_objects.size());
		assert(id >= m_min_id);
		size_t index = id - m_min_id;
		return m_dropped.is1(index);
	}
	void drop(size_t id) {
		assert(id >= m_min_id);
		assert(id <  m_min_id + m_objects.size());
		assert(m_dropped.size() == m_objects.size());
		m_num_free++;
		bm_uint_t* dropped = m_dropped.bldata();
		size_t index = id - m_min_id;
		assert(!terark_bit_test(dropped, index));
		terark_bit_set1(dropped, index);
		if (terark_unlikely(bm_uint_t(-1) == dropped[0])) {
			assert(m_dropped.size() >= TERARK_WORD_BITS);
			size_t allones = 1;
			size_t n_words = m_dropped.size() / TERARK_WORD_BITS;
			while (allones < n_words && bm_uint_t(-1) == dropped[allones])
				 ++allones;
			n_words = ceiled_div(m_dropped.size(), TERARK_WORD_BITS);
			const bm_uint_t* src = dropped + allones;
			for (size_t i = 0, n = n_words - allones; i < n; ++i)
				dropped[i] = src[i]; // short loop is faster than memove
		//	memmove(dropped, src, sizeof(bm_uint_t)*(n_words-allones));
			size_t nDropped = allones * TERARK_WORD_BITS;
			m_dropped.risk_set_size(m_dropped.size() - nDropped);
			m_objects.erase_i(0, nDropped);
			assert(m_dropped.size() == m_objects.size());
			m_min_id += nDropped;
			m_num_free -= nDropped;
		}
	}
	size_t min_id() const { return m_min_id; }
	size_t max_id() const { return m_min_id + m_objects.size(); }
	size_t num_used() const { return m_objects.size() - m_num_free; }
	size_t num_free() const { return m_num_free; }
};

/// With sufficient capacity, time of all method are O(1)
template<class T>
class CircularPermanentID {
	AutoFree<T> m_objects;
	AutoFree<bm_uint_t> m_dropped;
//	size_t    m_head; // == m_min_id % (m_cap-1)
	size_t    m_cap;
	size_t    m_tail;
	size_t    m_min_id;
	size_t    m_num_free;
public:
	explicit CircularPermanentID(size_t initial_cap = 0)
	  : m_tail(0), m_min_id(0), m_num_free(0)
	{
		const size_t min_cap = TERARK_WORD_BITS;
		m_cap = std::max(initial_cap, min_cap);
		if (m_cap & (m_cap - 1)) { // not power of 2
			size_t bpos = terark_bsr_u32((uint32_t)m_cap);
			m_cap = size_t(1) << (bpos + 1);
		}
		m_dropped.resize(0, m_cap, 0);
		m_objects.resize(0, m_cap, T());
	}
	size_t push(const T& x) {
		size_t cap = m_cap;
		size_t tail = m_tail;
		size_t mask = cap - 1;
		size_t head = m_min_id & mask;
		size_t virt_tail = (tail - head) & mask;
		if (terark_unlikely(mask == virt_tail)) { // full
			if (terark_bit_test(m_dropped.p, head)) {
				assert(m_num_free > 0);
				m_min_id++;
				m_num_free--;
				head = m_min_id & mask;
				virt_tail = (tail - head) & mask;
				goto DoPush;
			}
			m_dropped.resize(2*cap);
			m_objects.resize(2*cap);
			bm_uint_t* dropped = m_dropped;
			T        * objects = m_objects;
			size_t cn = cap / TERARK_WORD_BITS;
			if (tail < head) {
				if (m_min_id & cap) {
					memmove(objects + head + cap,
							objects + head, sizeof(T)*(cap-head));
					size_t i = head / TERARK_WORD_BITS;
					for (; i < cn; ++i)
						dropped[i + cn] = dropped[i];
					head += cap;
					virt_tail = (tail - head) & (2*cap-1);
				}
				else {
					memmove(objects + cap, objects, sizeof(T)*(tail));
					size_t tn = ceiled_div(tail, TERARK_WORD_BITS);
					for (size_t i = 0; i < tn; ++i)
						dropped[i + cn] = dropped[i];
					tail += cap;
					virt_tail = tail - head;
				}
			}
			else {
				assert(0000 == head);
				assert(tail == mask);
				assert(tail == virt_tail);
				if (m_min_id & cap) {
					memmove(objects + cap, objects, sizeof(T)*(mask));
					memmove(dropped + cn , dropped, sizeof(bm_uint_t)*(cn));
					tail += cap;
					head += cap;
					virt_tail = tail;
				}
				else {
					assert(0000 == head);
				}
			}
			m_cap = cap = 2*cap;
			mask = cap - 1;
		}
	DoPush:
		m_objects[tail] = x;
		terark_bit_set0(m_dropped.p, tail);
		m_tail = (tail + 1) & mask;
		size_t id = m_min_id + virt_tail;
		return id;
	}
	inline T& operator[](size_t id) {
		assert(id >= m_min_id);
		size_t mask = m_cap - 1;
		size_t loca_id = id & mask;
#if !defined(NDEBUG)
		size_t head = m_min_id & mask;
		size_t virt_tail = (m_tail - head) & mask;
		size_t virt_id = id - m_min_id;
		assert(virt_id < virt_tail);
		assert(!terark_bit_test(m_dropped.p, loca_id));
#endif
		return m_objects[loca_id];
	}
	inline const T& operator[](size_t id) const {
		assert(id >= m_min_id);
		size_t mask = m_cap - 1;
		size_t loca_id = id & mask;
#if !defined(NDEBUG)
		size_t head = m_min_id & mask;
		size_t virt_tail = (m_tail - head) & mask;
		size_t virt_id = id - m_min_id;
		assert(virt_id < virt_tail);
		assert(!terark_bit_test(m_dropped.p, loca_id));
#endif
		return m_objects[loca_id];
	}
	bool is_dropped(size_t id) const {
		size_t mask = m_cap - 1;
		size_t loca_id = id & mask;
#if !defined(NDEBUG)
		size_t virt_tail = (m_tail - m_min_id) & mask;
		assert(id >= m_min_id);
		assert(id <  m_min_id + virt_tail);
		assert(!terark_bit_test(m_dropped.p, loca_id));
#endif
		return m_objects[loca_id];
	}
	/// time is always O(1)
	void drop(size_t id) {
		size_t mask = m_cap - 1;
#if !defined(NDEBUG)
		size_t head = m_min_id & mask;
		assert(head != m_tail); // not empty
		size_t virt_tail = (m_tail - head) & mask;
		assert(id >= m_min_id);
		assert(id <  m_min_id + virt_tail);
#endif
		bm_uint_t* dropped = m_dropped;
		size_t     loca_id = id & mask;
		assert(!terark_bit_test(dropped, loca_id));
		if (terark_likely(id != m_min_id)) {
			m_num_free++;
			terark_bit_set1(dropped, loca_id);
		} else {
			m_min_id++;
		}
	}
	size_t min_id() const { return m_min_id; }
	size_t max_id() const { return m_min_id + ((m_tail - m_min_id) & (m_cap - 1)); }
	size_t num_used() const { return ((m_tail - m_min_id) & (m_cap - 1)) - m_num_free; }
	size_t num_free() const { return m_num_free; }
};

#define ByPermanentID CircularPermanentID
//#define ByPermanentID SimplePermanentID

static const size_t HUGE_PAGE_SIZE = 2 << 20; // 2MB
static const size_t PAGE_SIZE = 4096;
static const size_t PAGE_BITS = 12;
static const uint32_t nillink = UINT32_MAX;
using std::memory_order_relaxed;

namespace lru_detail {
	struct File {
		intptr_t fd;
		uint32_t headpage = nillink;
		uint32_t pgcnt = 0;
		uint32_t next_fi = nillink;
		uint32_t prev_fi = nillink;
		bool     is_pending_drop = false;
		explicit File(intptr_t fd1 = -1) : fd(fd1) {}

		template<class FileVec>
		static size_t list_len(const FileVec& base, size_t head) {
			if (nillink == head) {
				return 0;
			}
			size_t p = head;
			size_t n = 0;
			do {
				n++;
				const File& fp = base[p];
				size_t q = fp.next_fi;
#if !defined(NDEBUG)
				const File& fq = base[q];
				assert(nillink != q);
				assert(fq.prev_fi == p);
#endif
				p = q;
			} while (p != head);
			return n;
		}
		template<class FileVec>
		inline static
		void insert_after_p(FileVec& base, uint32_t* pos, size_t x) {
			if (nillink == *pos) {
				*pos = x;
				base[x].next_fi = base[x].prev_fi = x;
			}
			else {
				insert_after(base, *pos, x);
			}
		}
		template<class FileVec>
		inline static
		void insert_after(FileVec& base, size_t pos, size_t x) {
			File& fp = base[pos];
			File& fx = base[x];
			size_t pnext = fp.next_fi;
			fx.next_fi = pnext;
			fx.prev_fi = pos;
			base[pnext].prev_fi = x;
			fp.next_fi = x;
		}
		template<class FileVec>
		inline static
		void remove_fi(FileVec& base, File& f, uint32_t curr, uint32_t* head) {
			assert(&base[curr] == &f);
			auto next = f.next_fi;
			auto prev = f.prev_fi;
			if (terark_likely(next != curr)) {
				base[next].prev_fi = prev;
				base[prev].next_fi = next;
				*head = next;
			}
			else {
				assert(next == prev);
				assert(next == *head);
				*head = nillink;
			}
		}
		template<class FileVec>
		inline static void remove(FileVec& base, size_t x) {
			File& f = base[x];
			auto next = f.next_fi;
			auto prev = f.prev_fi;
			base[next].prev_fi = prev;
			base[prev].next_fi = next;
		}
	};

	struct Node {
		uint64_t fi_offset;
		uint32_t fi_prev;
		uint32_t fi_next;
		uint32_t lru_prev;
		uint32_t lru_next;
		uint16_t ref_count;
		volatile uint08_t is_loaded;
		uint08_t reserved;
		uint32_t hash_link;

		uint32_t get_fi() const { return uint32_t(fi_offset >> 32); }

		static void fi_remove_head(Node* base, size_t x, uint32_t* head) {
			assert(x == *head);
			uint32_t x_next = base[x].fi_next;
			uint32_t x_prev = base[x].fi_prev;
			if (x_next != x) {
				base[x_next].fi_prev = x_prev;
				base[x_prev].fi_next = x_next;
				*head = x_next;
			}
			else {
				*head = nillink;
			}
		}
		static void fi_remove(Node* base, size_t x) {
			auto next = base[x].fi_next;
			auto prev = base[x].fi_prev;
			base[next].fi_prev = prev;
			base[prev].fi_next = next;
		}
		static void lru_remove(Node* base, size_t x) {
			auto next = base[x].lru_next;
			auto prev = base[x].lru_prev;
			base[next].lru_prev = prev;
			base[prev].lru_next = next;
		}

		static size_t fi_list_len(const Node* base, size_t head) {
			assert(nillink != head);
			size_t p = head;
			size_t n = 0;
			do {
				n++;
				size_t q = base[p].fi_next;
				assert(nillink != q);
				assert(base[q].fi_prev == p);
				p = q;
			} while (p != head);
			return n;
		}
		static void fi_insert_after_p(Node* base, uint32_t* pos, size_t x) {
			if (nillink == *pos) {
				*pos = x;
				base[x].fi_next = base[x].fi_prev = x;
			}
			else {
				fi_insert_after(base, *pos, x);
			}
		}
		///@param p insert after this position
		///@param x the inserted node
		static void fi_insert_after(Node* base, size_t p, size_t x) {
			auto n = base[p].fi_next;
			base[x].fi_next = n;
			base[x].fi_prev = p;
			base[n].fi_prev = x;
			base[p].fi_next = x;
		}
		///@param p insert after this position
		///@param x the inserted node
		static void lru_insert_after(Node* base, size_t p, size_t x) {
			auto n = base[p].lru_next;
			base[x].lru_next = n;
			base[x].lru_prev = p;
			base[n].lru_prev = x;
			base[p].lru_next = x;
		}
	};
}
using namespace lru_detail;

//#define SLOW_DEBUG

//#define INDIVIDUAL_FILE_VECTOR_LOCK
#ifdef INDIVIDUAL_FILE_VECTOR_LOCK
	#define LOCK_FILE_VECTOR_ELEM  ScopeLock lock_fd_fi(m_mutex_fd_fi)
	#define LOCK_FILE_VECTOR_FULL LOCK_FILE_VECTOR_ELEM
#else
	#define LOCK_FILE_VECTOR_ELEM
	#define LOCK_FILE_VECTOR_FULL  ScopeLock lock(m_mutex)
#endif

class SingleLruReadonlyCache final: public LruReadonlyCache {
public:
    bool                m_use_aio;
	valvec<size_t>      m_histogram;
	Node*               m_hash_nodes;
	uint32_t*           m_bucket;
	byte_t*             m_bufmem;
	size_t              m_page_num;
	size_t              m_bucket_size;
	ByPermanentID<File> m_fi_to_fd;
	uint32_t            m_fi_freelist;
	uint32_t            m_fi_busylist;
	uint32_t            m_busypage_num;
//	uint32_t            m_droppage_num;
	size_t   m_stat_cnt[6];
	MY_MUTEX_PADDING
	mutable MyMutex     m_mutex;
#ifdef INDIVIDUAL_FILE_VECTOR_LOCK
	MY_MUTEX_PADDING
	MyMutex          m_mutex_fd_fi;
#endif
	MY_MUTEX_PADDING
	SingleLruReadonlyCache(size_t capacityBytes, size_t maxFiles, bool aio);
	~SingleLruReadonlyCache();
	const byte_t* pread(intptr_t fi, size_t offset, size_t len, Buffer*) override;
	void discard_impl(const Buffer& b);
	intptr_t open(intptr_t fd) override;
	void close(intptr_t fi) override;
	bool safe_close(intptr_t fi) override;
	void print_stat_cnt(FILE*) const override;
	static void print_stat_cnt_impl(FILE*, const size_t cnt[6], const valvec<size_t>& histogram);
	valvec<size_t> get_histogram_snapshot() const;
private:
	uint32_t alloc_page(size_t hpos, uint64_t fi_offset_key, Buffer::CacheType*, intptr_t* fd);
	void remove_from_hash(size_t bucketIdx, size_t slot);
};

///
SingleLruReadonlyCache::
SingleLruReadonlyCache(size_t capacityBytes, size_t maxFiles, bool aio)
	: m_fi_to_fd(maxFiles)
{
    m_use_aio = aio;
	size_t pgNum = ceiled_div(capacityBytes, PAGE_SIZE);
	if (pgNum >= nillink-2) {
		THROW_STD(invalid_argument
			, "capacityBytes = %zd is too large, yield page num = %zd"
			, capacityBytes, pgNum);
	}
	m_bucket_size = __hsm_stl_next_prime(pgNum * 3 / 2);
	size_t node_bytes = sizeof(Node) * (pgNum + 1);
	size_t page_bytes = pgNum * PAGE_SIZE;
	size_t bucket_bytes = sizeof(uint32_t) * m_bucket_size;
	size_t bytes = page_bytes + node_bytes + bucket_bytes;
#if defined(_MSC_VER)
	byte_t* mem = (byte_t*)_aligned_malloc(bytes, PAGE_SIZE);
	if (NULL == mem) {
		THROW_STD(invalid_argument
			, "_aligned_malloc(align=%zd, size=%zd) = %s"
			, PAGE_SIZE, bytes, strerror(errno));
	}
#else
	byte_t* mem = NULL;
	int err = posix_memalign((void**)&mem, HUGE_PAGE_SIZE, bytes);
	if (err) {
		THROW_STD(invalid_argument
			, "posix_memalign(align=%zd, size=%zd) = %s"
			, PAGE_SIZE, bytes, strerror(err));
	}
  #ifdef MADV_HUGEPAGE
	if (madvise(mem, bytes, MADV_HUGEPAGE) < 0) {
		fprintf(stderr
			, "WARN: SingleLruReadonlyCache: madvise(HUGEPAGE) = %s\n"
			, strerror(errno));
	}
  #endif
	if (madvise(mem, bytes, MADV_WILLNEED) < 0) {
		fprintf(stderr
			, "WARN: SingleLruReadonlyCache: madvise(WILLNEED) = %s\n"
			, strerror(errno));
	}
#endif
	m_bufmem = mem;
	m_hash_nodes = (Node*)(mem + page_bytes);
	for (size_t i = 0; i < pgNum+1; ++i) {
		m_hash_nodes[i].fi_offset = uint64_t(-1);
		m_hash_nodes[i].ref_count = 0;
		m_hash_nodes[i].is_loaded = false;
		m_hash_nodes[i].reserved = 0;
		m_hash_nodes[i].hash_link = nillink;
		m_hash_nodes[i].fi_next = nillink; m_hash_nodes[i].lru_next = i+1;
		m_hash_nodes[i].fi_prev = nillink; m_hash_nodes[i].lru_prev = i-1;
	}
	m_hash_nodes[pgNum].lru_next = 0;
	m_hash_nodes[0].lru_prev = pgNum;
	m_bucket = (uint32_t*)(m_hash_nodes + pgNum + 1);
	std::fill_n(m_bucket, m_bucket_size, nillink);
	m_page_num = pgNum;
	m_fi_freelist = nillink;
	m_fi_busylist = nillink;
	m_busypage_num = 0;
	memset(m_stat_cnt, 0, sizeof(m_stat_cnt));
	m_histogram.reserve(128);
}

SingleLruReadonlyCache::~SingleLruReadonlyCache() {
	TERARK_IF_MSVC(_aligned_free, free)(m_bufmem);
}

static void
do_pread(intptr_t fd, void* buf, size_t offset, size_t minlen, size_t maxlen, bool aio) {
#if defined(_MSC_VER)
	OVERLAPPED ol; memset(&ol, 0, sizeof(ol));
	ol.Offset = (DWORD)offset;
	ol.OffsetHigh = (DWORD)(uint64_t(offset) >> 32);
	DWORD rdlen = 0;
	// fd is HANDLE, which opened by `CreateFile`, not `open`
	if (!ReadFile((HANDLE)(fd), buf, maxlen, &rdlen, &ol) || rdlen < minlen) {
		int err = GetLastError();
		THROW_STD(logic_error
			, "ReadFile(offset = %zd, len = %zd) = %u, LastError() = %d(0x%X)"
			, offset, maxlen, rdlen, err, err);
	}
#else
	ssize_t rdlen;
	if (aio)
	    rdlen = fiber_aio_read(fd, buf, maxlen, offset);
	else
	    rdlen = pread(fd, buf, maxlen, offset);

	if (rdlen < ssize_t(minlen)) {
		THROW_STD(logic_error
			, "pread(offset = %zd, len = %zd) = %zd (minlen = %zd), err = %s"
			, offset, maxlen, rdlen, minlen, strerror(errno));
	}
#endif
}

static inline uint64_t MyHash(uint64_t fi_page_id) {
	uint64_t hash1 = (fi_page_id << 3) | (fi_page_id >> 61);
	return byte_swap(hash1);
}

TERARK_DLL_EXPORT
void fdpread(intptr_t fd, void* buf, size_t len, size_t offset) {
	do_pread(fd, buf, offset, len, len, false);
}

// already in m_mutex lock
uint32_t
SingleLruReadonlyCache::alloc_page(size_t hpos, uint64_t fi_offset_key,
							 Buffer::CacheType* cache_type, intptr_t* fd) {
	uint32_t* bucket = m_bucket;
	Node*     nodes = m_hash_nodes;
	uint32_t  fi = uint32_t(fi_offset_key >> 32);
	uint32_t  p;
#if defined(NDEBUG) || !defined(SLOW_DEBUG)
	#define assert_list_len(file)
#else
	#define assert_list_len(file) do { \
		if (nillink == file.headpage) { \
			assert(file.pgcnt == 0); \
		} else { \
			assert(file.pgcnt > 0); \
			size_t list_len = Node::fi_list_len(nodes, file.headpage); \
			assert(list_len == file.pgcnt); \
		} \
	} while (0)
#endif
	if (nillink != m_fi_freelist) {
		// has dropped/closed files, try it first
		LOCK_FILE_VECTOR_ELEM;
		auto free_fi = m_fi_freelist;
		if (nillink != free_fi) { // double check after lock
			File*  free_fp = &m_fi_to_fd[free_fi];
			File*  curr_fp = &m_fi_to_fd[fi];
			*fd = curr_fp->fd;
			assert(free_fp->pgcnt > 0);
			assert(free_fp->is_pending_drop);
			assert(free_fp->headpage != nillink);
			p = free_fp->headpage;
			assert_list_len((*curr_fp));
			assert_list_len((*free_fp));
			if (0 == --free_fp->pgcnt) {
				File::remove_fi(m_fi_to_fd, *free_fp, free_fi, &m_fi_freelist);
				m_fi_to_fd.drop(free_fi);
				curr_fp = &m_fi_to_fd[fi]; // must reload after drop
			} else {
				auto next_freepg = nodes[p].fi_next;
				Node::fi_remove(nodes, p);
				free_fp->headpage = next_freepg;
			}
			Node::lru_remove(nodes, p);
			Node::fi_insert_after_p(nodes, &curr_fp->headpage, p);
			assert(nodes[p].ref_count == 0);
			size_t free_hpos = MyHash(nodes[p].fi_offset) % m_bucket_size;
			remove_from_hash(free_hpos, p);
			curr_fp->pgcnt++;
			m_busypage_num++;
			m_stat_cnt[Buffer::dropped_free]++;
			*cache_type = Buffer::dropped_free;
			assert_list_len((*curr_fp));
		}
		else { // very unlikely
			goto SwapOut;
		}
	}
	else {
	SwapOut:
		p = nodes[0].lru_prev; // lru tail
        if (0 == p) {
		    THROW_STD(logic_error
			    , "can not evict a page, busy pages = %zd, max pages = %zd"
			    , size_t(m_busypage_num), size_t(m_page_num));
        }
		if (uint64_t(-1) != nodes[p].fi_offset) {
			m_stat_cnt[Buffer::evicted_others]++;
			*cache_type = Buffer::evicted_others;
			size_t swap_hpos = MyHash(nodes[p].fi_offset) % m_bucket_size;
			size_t swap_fi = nodes[p].get_fi();
			LOCK_FILE_VECTOR_ELEM;
			File&  curr_fp = m_fi_to_fd[fi];
			File&  swap_fp = m_fi_to_fd[swap_fi];
			assert(swap_fp.pgcnt > 0);
			assert(swap_fp.headpage != nillink);
			assert(!swap_fp.is_pending_drop);
			assert_list_len(curr_fp);
			assert_list_len(swap_fp);
			*fd = curr_fp.fd;
			curr_fp.pgcnt++;
			if (--swap_fp.pgcnt) {
				if (swap_fp.headpage == p) {
					swap_fp.headpage = nodes[p].fi_next;
				}
				Node::fi_remove(nodes, p);
			} else {
				swap_fp.headpage = nillink;
			}
			Node::fi_insert_after_p(nodes, &curr_fp.headpage, p);
			assert_list_len(curr_fp);
			remove_from_hash(swap_hpos, p);
		}
		else {
			assert(nodes[p].ref_count == 0);
			m_stat_cnt[Buffer::initial_free]++;
			*cache_type = Buffer::initial_free;
			m_busypage_num++;
			LOCK_FILE_VECTOR_ELEM;
			File&  curr_fp = m_fi_to_fd[fi];
			assert(!curr_fp.is_pending_drop);
			assert_list_len(curr_fp);
			Node::fi_insert_after_p(nodes, &curr_fp.headpage, p);
			*fd = curr_fp.fd;
			curr_fp.pgcnt++;
			assert_list_len(curr_fp);
		}
		Node::lru_remove(nodes, p);
	}
	nodes[p].ref_count = 1;
	nodes[p].is_loaded = false;
	nodes[p].fi_offset = fi_offset_key;
	nodes[p].hash_link = bucket[hpos];
	bucket[hpos] = p; // insert to hash
	return p;
}

intptr_t SingleLruReadonlyCache::open(intptr_t fd) {
	if (fd < 0) {
		THROW_STD(invalid_argument, "invalid fd = %zd", fd);
	}
	LOCK_FILE_VECTOR_FULL;
	uint32_t fi = (uint32_t)m_fi_to_fd.push(File(fd));
	File::insert_after_p(m_fi_to_fd, &m_fi_busylist, fi);
#if !defined(NDEBUG) && defined(SLOW_DEBUG)
	const File& f = m_fi_to_fd[fi];
	assert(0 == f.pgcnt);
	assert(nillink == f.headpage);
	size_t busylen = File::list_len(m_fi_to_fd, m_fi_busylist);
	size_t freelen = File::list_len(m_fi_to_fd, m_fi_freelist);
	size_t usednum = m_fi_to_fd.num_used();
	assert(usednum == busylen + freelen);
#endif
	return fi;
}

struct MyPageEntry {
	size_t    hpos;
	uint32_t  page_id;
	bool      alloc_by_me;
};

const byte_t*
SingleLruReadonlyCache::pread(intptr_t fi, size_t offset, size_t len, Buffer* b) {
	if (terark_unlikely(fi < 0)) {
		THROW_STD(invalid_argument, "invalid fi = %zd", fi);
	}
	size_t pg_offset = offset % PAGE_SIZE;
	uint32_t* bucket = m_bucket;
	Node*     nodes = m_hash_nodes;
	intptr_t  fd = -1;
    assert(nullptr != b->rdbuf);
    b->cache_type = Buffer::hit; // hit is very likely
    b->owner = this;
	if (pg_offset + len <= PAGE_SIZE) {
		uint64_t fi_offset_key = (fi << 32) | (offset >> PAGE_BITS);
		size_t   hpos = MyHash(fi_offset_key) % m_bucket_size;
		uint32_t p = bucket[hpos]; // to preload
		{
			size_t conflict_len = 0;
			ScopeLock lock(m_mutex);
			p = bucket[hpos]; assert(p > 0); // real load
			for (; nillink != p; p = nodes[p].hash_link) {
				assert(p <= m_page_num);
				if (fi_offset_key == nodes[p].fi_offset) {
					m_histogram.ensure_get(conflict_len)++;
                    if (nodes[p].ref_count++ == 0) {
    					Node::lru_remove(nodes, p);
                    }
					if (terark_likely(nodes[p].is_loaded)) {
						m_stat_cnt[Buffer::hit]++;
						byte_t* bufptr = m_bufmem + PAGE_SIZE*(p-1) + pg_offset;
                        b->index = p;
                        assert(p > 0);
						return bufptr;
					} else { // very unlikely
						goto OnHitOthersLoad; // go out of scope to unlock
					}
				}
				conflict_len++;
			}
			p = alloc_page(hpos, fi_offset_key, &b->cache_type, &fd);
		}
		if (0) {
	OnHitOthersLoad:
			while (!nodes[p].is_loaded) {
				// waiting for other threads to load the page
				std::this_thread::yield();
			}
			byte_t* bufptr = m_bufmem + PAGE_SIZE*(p-1) + pg_offset;
			ScopeLock lock(m_mutex);
			assert(nodes[p].fi_offset == fi_offset_key);
			m_stat_cnt[Buffer::hit_others_load]++;
            b->cache_type = Buffer::hit_others_load;
            b->index = p;
            assert(p > 0);
            return bufptr;
		}
		byte_t* bufptr = m_bufmem + PAGE_SIZE*(p-1);
		bool    isOK = false;
		TERARK_SCOPE_EXIT(if (!isOK) nodes[p].ref_count--);
		do_pread(fd, bufptr
				   , align_down(offset, PAGE_SIZE)
				   , pg_offset + len, PAGE_SIZE, m_use_aio);
		nodes[p].is_loaded = true;
		isOK = true;
        b->index = p;
        assert(p > 0);
        return bufptr + pg_offset;
	}
	else {
        assert(nullptr != b->rdbuf);
        valvec<byte_t>* unibuf = b->rdbuf;
		unibuf->erase_all();
		unibuf->ensure_capacity(len);
		size_t first_page =  offset >> PAGE_BITS;
		size_t plast_page = (offset + len + PAGE_SIZE - 1) >> PAGE_BITS;
		assert(first_page + 2 <= plast_page);
		static thread_local valvec<valvec<MyPageEntry> > tss;
		valvec<MyPageEntry> pgvec_obj;
		if (tss.size()) {
		    tss.back().swap(pgvec_obj);
		    tss.pop_back();
		}
		pgvec_obj.resize_no_init(plast_page - first_page);
		auto pgvec = pgvec_obj.data();
		for (size_t pg = first_page; pg < plast_page; ++pg) {
			uint64_t fi_offset_key = (fi << 32) | pg;
			// MyHash(fi_offset_key) % m_bucket_size is slow
			// compute it no lock
			// and prefetch memory into cache
			size_t hpos = MyHash(fi_offset_key) % m_bucket_size;
			size_t p = bucket[hpos];
			for (size_t i = 0; i < 3 && nillink != p; ++i) {
				_mm_prefetch((const char*)(nodes + p), _MM_HINT_T0);
				p = nodes[p].hash_link;
			}
			pgvec[pg - first_page].hpos = hpos;
		}
		size_t missed_cnt = 0;
		{
			ScopeLock lock(m_mutex);
			for (size_t pg = first_page; pg < plast_page; ++pg) {
				size_t hpos = pgvec[pg - first_page].hpos;
				uint64_t fi_offset_key = (fi << 32) | pg;
				auto p = bucket[hpos];
				assert(p > 0);
				size_t conflict_len = 0;
				for (; nillink != p; p = nodes[p].hash_link) {
					assert(p <= m_page_num);
					if (fi_offset_key == nodes[p].fi_offset) {
						if (nodes[p].ref_count++ == 0) {
						    Node::lru_remove(nodes, p);
                        }
						m_stat_cnt[Buffer::hit]++;
						pgvec[pg - first_page].alloc_by_me = false;
						m_histogram.ensure_get(conflict_len)++;
						goto CrossPageNext;
					}
					conflict_len++;
				}
				p = alloc_page(hpos, fi_offset_key, &b->cache_type, &fd);
				missed_cnt++;
				pgvec[pg - first_page].alloc_by_me = true;
			CrossPageNext:
				pgvec[pg - first_page].page_id = p;
			}
		}
		// read data no lock...
		auto readpage = [this,first_page,fd,nodes,pgvec,unibuf,fi]
		(size_t fpg, size_t minlen, size_t pg_offset) {
			auto p = pgvec[fpg - first_page].page_id;
			byte_t* bufptr = this->m_bufmem + PAGE_SIZE*(p-1);
			if (!nodes[p].is_loaded) {
				if (pgvec[fpg - first_page].alloc_by_me) {
					assert(fd >= 0);
					do_pread(fd, bufptr, fpg*PAGE_SIZE, minlen, PAGE_SIZE, m_use_aio);
					nodes[p].is_loaded = true;
				} else {
					while (!nodes[p].is_loaded) {
					    if (m_use_aio) {
                            boost::this_fiber::yield();
                            if (nodes[p].is_loaded)
                                break;
                        }
						std::this_thread::yield();
					}
					ScopeLock lock(m_mutex);
					this->m_stat_cnt[Buffer::hit_others_load]++;
				}
			}
			assert(((fi << 32) | fpg) == nodes[p].fi_offset);
			size_t  len0 = minlen - pg_offset;
			unibuf->append(bufptr + pg_offset, len0);
		};
		TERARK_SCOPE_EXIT(
			auto pgvec_p = pgvec;
			auto nodes_p = nodes;
			size_t  last = plast_page;
            m_mutex.lock();
			for (size_t fpg = first_page; fpg < last; ++fpg) {
				auto  p = pgvec_p[fpg - first_page].page_id;
				if (0 == --nodes_p[p].ref_count)
                    Node::lru_insert_after(nodes, 0, p);
			}
            m_mutex.unlock();
		);
		if (missed_cnt > 0) {
		    readpage(first_page, PAGE_SIZE, pg_offset);
		    size_t pg = first_page + 1;
		    for (; pg < plast_page - 1; ++pg) {
			    readpage(pg, PAGE_SIZE, 0);
		    }
		    readpage(pg, (offset + len - 1) % PAGE_SIZE + 1, 0);
		    assert(unibuf->size() == len);
		    if (missed_cnt > 1) {
			    b->cache_type = Buffer::mix;
            }
		}
        b->index = 0;
		if (pgvec_obj.capacity()) {
		    tss.emplace_back(std::move(pgvec_obj));
		}
		return unibuf->data();
	}
}

// m_mutex is locked before calling this function
void SingleLruReadonlyCache::remove_from_hash(size_t bucketIdx, size_t slot) {
	assert(bucketIdx < m_bucket_size);
	uint32_t* pCurr = &m_bucket[bucketIdx];
	Node* nodes = m_hash_nodes;
	while (nillink != *pCurr) {
		uint32_t* pNext = &nodes[*pCurr].hash_link;
		if (*pCurr == slot) {
			*pCurr = *pNext;
			return;
		}
		pCurr = pNext;
	}
	abort(); // should not goes here
}

void SingleLruReadonlyCache::discard_impl(const Buffer& b) {
	assert(0 != b.index);
	size_t p = b.index;
    Node* nodes = m_hash_nodes;
	ScopeLock lock(m_mutex);
#if !defined(NDEBUG)
	assert(m_hash_nodes[p].ref_count > 0);
	size_t fi = m_hash_nodes[p].get_fi();
	LOCK_FILE_VECTOR_ELEM;
	File& f = m_fi_to_fd[fi];
	assert(!f.is_pending_drop);
	assert(nillink != f.headpage);
	assert(f.headpage <= m_page_num);
	assert(f.pgcnt > 0);
#endif
	if (0 == --nodes[p].ref_count) {
        Node::lru_insert_after(nodes, 0, p);
    }
}

void LruReadonlyCache::Buffer::discard_impl() {
    assert(0 != index);
    assert(nullptr != owner);
    owner->discard_impl(*this);
    index = 0;
}

void SingleLruReadonlyCache::close(intptr_t fi) {
	if (fi < 0) {
		THROW_STD(invalid_argument, "invalid fi = %zd", fi);
	}
	LOCK_FILE_VECTOR_FULL;
	assert(size_t(fi) >= m_fi_to_fd.min_id());
	assert(size_t(fi) <  m_fi_to_fd.max_id());
	File& f = m_fi_to_fd[fi];
	assert(f.fd >= 0);
	assert(!f.is_pending_drop);
	f.is_pending_drop = true;
	File::remove_fi(m_fi_to_fd, f, fi, &m_fi_busylist);
	if (nillink == f.headpage) {
		assert(0 == f.pgcnt);
		m_fi_to_fd.drop(fi);
	} else {
		assert(f.pgcnt > 0);
#if !defined(NDEBUG) && defined(SLOW_DEBUG)
		const Node* nodes = m_hash_nodes;
		assert_list_len(f);
#endif
		m_busypage_num -= f.pgcnt;
		File::insert_after_p(m_fi_to_fd, &m_fi_freelist, fi);
	}
}

bool SingleLruReadonlyCache::safe_close(intptr_t fi) {
	if (fi < 0) {
		return false;
	}
	LOCK_FILE_VECTOR_FULL;
	if (size_t(fi) < m_fi_to_fd.min_id()) {
		return false;
	}
	if (size_t(fi) >= m_fi_to_fd.max_id()) {
		return false;
	}
	File& f = m_fi_to_fd[fi];
	if (f.is_pending_drop) {
		return false;
	}
	f.is_pending_drop = true;
	File::remove_fi(m_fi_to_fd, f, fi, &m_fi_busylist);
	if (nillink == f.headpage) {
		assert(0 == f.pgcnt);
		m_fi_to_fd.drop(fi);
	} else {
		assert(f.pgcnt > 0);
#if !defined(NDEBUG) && defined(SLOW_DEBUG)
		const Node* nodes = m_hash_nodes;
		assert_list_len(f);
#endif
		m_busypage_num -= f.pgcnt;
		File::insert_after_p(m_fi_to_fd, &m_fi_freelist, fi);
	}
	return true;
}

valvec<size_t> SingleLruReadonlyCache::get_histogram_snapshot() const {
	valvec<size_t> histogram(m_histogram.capacity() + 10, valvec_reserve());
	ScopeLock lock(m_mutex);
	histogram.assign(m_histogram);
	return histogram;
}

void SingleLruReadonlyCache::print_stat_cnt(FILE* fp) const {
	print_stat_cnt_impl(fp, m_stat_cnt, get_histogram_snapshot());
}

void SingleLruReadonlyCache::print_stat_cnt_impl(FILE* fp, const size_t cnt[6], const valvec<size_t>& histogram) {
	size_t sum = 0;
	for (size_t i = 0; i < 6; ++i) sum += cnt[i];
#define PrintEnum(Enum) \
  fprintf(fp, "%-15s : %12zd, %7.3f\n", #Enum, cnt[Buffer::Enum], cnt[Buffer::Enum]/double(sum))
	PrintEnum(hit);
	PrintEnum(evicted_others);
	PrintEnum(initial_free);
	PrintEnum(dropped_free);
	PrintEnum(hit_others_load);
	fprintf(fp, "----\n");
	fprintf(fp, "| hash conflict len | freq | ratio |\n");
	fprintf(fp, "| ----------------- | ---- | -----:|\n");
	double total = 0;
	for(size_t x : histogram) total += x;
	for(size_t i = 0; i < histogram.size(); ++i) {
		size_t x = histogram[i];
		fprintf(fp, "| %4zd | %4zd | %6.3f |\n", i + 1, x, x/total);
	}
}

/////////////////////////////////////////////////////////////////////////////
class MultiLruReadonlyCache final : public LruReadonlyCache {
public:
	valvec<std::unique_ptr<SingleLruReadonlyCache> > m_shards;
	//typedef boost::fibers::mutex MutexType;
	typedef std::mutex MutexType;
	typedef std::lock_guard<MutexType> MutexGuard;
	MutexType m_mutex;

	explicit MultiLruReadonlyCache(size_t capacityBytes, size_t shards, size_t maxFiles, bool aio) {
		m_shards.reserve(shards);
		size_t cap_all = align_up(capacityBytes, shards*PAGE_SIZE);
		size_t cap_one = cap_all / shards;
		for (size_t i = 0; i < shards; ++i) {
			m_shards.emplace_back(new SingleLruReadonlyCache(cap_one, maxFiles, aio));
		}
	}
	~MultiLruReadonlyCache() {
	}
	static inline
    size_t get_shard_id(uint64_t fi_page_id, uint32_t n_shards) {
		uint64_t hash1 = (fi_page_id << 3) | (fi_page_id >> 61);
		uint64_t hash2 = byte_swap(hash1);
		return size_t(hash2 % n_shards);
	}
	const byte_t*
    pread(intptr_t fi, size_t offset, size_t len, Buffer* b) override {
		const uint64_t fi_at_hi32 = (uint64_t(fi) << 32);
        const uint32_t n_shards = uint32_t(m_shards.size());
    	size_t shard = get_shard_id(fi_at_hi32|(offset>>PAGE_BITS), n_shards);
        if ((offset & (PAGE_SIZE - 1)) + len <= PAGE_SIZE) {
    		return m_shards[shard]->pread(fi, offset, len, b);
        }
        valvec<byte_t>* unibuf = b->rdbuf;
        unibuf->ensure_capacity(len);
        unibuf->erase_all();
        {
            size_t len1 = PAGE_SIZE - (offset % PAGE_SIZE);
            auto data = m_shards[shard]->pread(fi, offset, len1, b);
            assert(0 != b->index);
            unibuf->append(data, len1);
            b->discard_impl();
            len -= len1;
            offset += len1;
        }
        while (len >= PAGE_SIZE) {
    	    shard = get_shard_id(fi_at_hi32|(offset>>PAGE_BITS), n_shards);
            auto data = m_shards[shard]->pread(fi, offset, PAGE_SIZE, b);
            assert(0 != b->index);
            unibuf->append(data, PAGE_SIZE);
            b->discard_impl();
            len -= PAGE_SIZE;
            offset += PAGE_SIZE;
        }
        if (len) {
    	    shard = get_shard_id(fi_at_hi32|(offset>>PAGE_BITS), n_shards);
            auto data = m_shards[shard]->pread(fi, offset, len, b);
            assert(0 != b->index);
            unibuf->append(data, len);
            b->discard_impl();
        }
		return unibuf->data();
	}
	intptr_t open(intptr_t fd) override {
	    MutexGuard lock(m_mutex);
		intptr_t fi = m_shards[0]->open(fd);
		for (size_t i = 1; i < m_shards.size(); ++i) {
			intptr_t fii = m_shards[i]->open(fd);
			TERARK_RT_assert(fi == fii, std::logic_error);
		}
		return fi;
	}
	void close(intptr_t fi) override {
	    MutexGuard lock(m_mutex);
		for (auto& p : m_shards) {
			p->close(fi);
		}
	}
	bool safe_close(intptr_t fi) override {
	    MutexGuard lock(m_mutex);
		bool bRet = false;
		for (auto& p : m_shards) {
			bRet = p->safe_close(fi);
		}
		return bRet;
	}
	void print_stat_cnt(FILE* fp) const override {
		size_t cnt[6];
		memset(cnt, 0, sizeof(cnt));
		for (auto& p : m_shards) {
			for (size_t i = 0; i < 6; ++i) {
				cnt[i] += p->m_stat_cnt[i];
			}
		}
		valvec<size_t> histogram(128, valvec_reserve());
		for (auto& p : m_shards) {
			auto hist1 = p->get_histogram_snapshot();
			if (histogram.size() < hist1.size()) {
				histogram.resize(hist1.size());
			}
			for (size_t i = 0; i < hist1.size(); ++i) {
				histogram[i] += hist1[i];
			}
		}
		SingleLruReadonlyCache::print_stat_cnt_impl(fp, cnt, histogram);
	}
};

LruReadonlyCache*
LruReadonlyCache::create(size_t totalcapacityBytes, size_t shards, size_t maxFiles, bool aio) {
	if (shards <= 1) {
		return new SingleLruReadonlyCache(totalcapacityBytes, maxFiles, aio);
	}
	if (shards >= 500) {
		THROW_STD(invalid_argument, "too large shard num = %zd", shards);
	}
	return new MultiLruReadonlyCache(totalcapacityBytes, shards, maxFiles, aio);
}

} // namespace terark

