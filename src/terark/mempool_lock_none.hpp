#pragma once
#include "valvec.hpp"
#include <boost/integer/static_log2.hpp>
#include <boost/mpl/if.hpp>

namespace terark {

/// mempool which alloc mem block identified by
/// integer offset(relative address), not pointers(absolute address)
/// integer offset could be 32bit even in 64bit hardware.
///
/// the returned offset is aligned to align_size, this allows 32bit
/// integer could address up to 4G*align_size memory
///
/// when memory exhausted, valvec can realloc memory without memcpy
/// @see valvec
#define MemPool_ThisType MemPool_LockNone
template<int AlignSize>
class MemPool_ThisType : private valvec<unsigned char> {
    BOOST_STATIC_ASSERT((AlignSize & (AlignSize-1)) == 0);
    BOOST_STATIC_ASSERT(AlignSize >= 4);
    typedef valvec<unsigned char> mem;
    typedef typename boost::mpl::if_c<AlignSize == 4, uint32_t, uint64_t>::type link_size_t;

    static const size_t skip_list_level_max = 8;    // data io depend on this, don't modify this value
    static const size_t list_tail = ~link_size_t(0);
    static const size_t offset_shift = AlignSize == 4 ? boost::static_log2<AlignSize>::value : 0;

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
    size_t  fragment_size; // for compatible with MemPool_Lock(Free|None|Mutex)
    size_t  huge_size_sum;
    size_t  huge_node_cnt;
    huge_link_t huge_list; // huge_list.size is max height of skiplist
    head_t* free_list_arr;
    size_t  free_list_len;

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

public:
    // low level method
    void destroy_and_clean() {
        mem::clear();
        clear_free_list();
    }

    // low level method
    void clear_free_list() {
        if (free_list_arr) {
            free(free_list_arr);
            free_list_arr = NULL;
        }
        free_list_len = 0;
        fragment_size = 0;
        huge_size_sum = 0;
        huge_node_cnt = 0;
        huge_list.size = 0;
        for(auto& next : huge_list.next)
            next = list_tail;
    }

public:
          mem& get_data_byte_vec()       { return *this; }
    const mem& get_data_byte_vec() const { return *this; }

    static size_t align_to(size_t len) {
        return (len + align_size - 1) & ~size_t(align_size - 1);
    }
    enum { align_size = AlignSize };

    explicit MemPool_ThisType(size_t maxBlockSize) {
        assert(maxBlockSize >= align_size);
        assert(maxBlockSize >= sizeof(huge_link_t));
        maxBlockSize = align_to(maxBlockSize);
        free_list_len = maxBlockSize / align_size;
        free_list_arr = (head_t*)malloc(sizeof(head_t) * free_list_len);
        if (NULL == free_list_arr) {
            throw std::bad_alloc();
        }
        fragment_size = 0;
        huge_size_sum = 0;
        huge_node_cnt = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, head_t());
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
    }
    MemPool_ThisType(const MemPool_ThisType& y) : mem(y) {
        free_list_len = y.free_list_len;
        free_list_arr = (head_t*)malloc(sizeof(head_t) * free_list_len);
        if (NULL == free_list_arr) {
            throw std::bad_alloc();
        }
        fragment_size = y.fragment_size;
        huge_size_sum = y.huge_size_sum;
        huge_node_cnt = y.huge_node_cnt;
        memcpy(free_list_arr, y.free_list_arr, sizeof(head_t) * free_list_len);
        huge_list = y.huge_list;
    }
    MemPool_ThisType& operator=(const MemPool_ThisType& y) {
        if (&y == this)
            return *this;
        destroy_and_clean();
        MemPool_ThisType(y).swap(*this);
        return *this;
    }
    ~MemPool_ThisType() {
        if (free_list_arr) {
            free(free_list_arr);
            free_list_arr = NULL;
        }
    }

#ifdef HSM_HAS_MOVE
    MemPool_ThisType(MemPool_ThisType&& y) noexcept : mem(y) {
        assert(y.data() == NULL);
        assert(y.size() == 0);
        free_list_len = y.free_list_len;
        free_list_arr = y.free_list_arr;
        fragment_size = y.fragment_size;
        huge_size_sum = y.huge_size_sum;
        huge_node_cnt = y.huge_node_cnt;
        huge_list = y.huge_list;
        y.free_list_len = 0;
        y.free_list_arr = NULL;
        y.fragment_size = 0;
        y.huge_size_sum = 0;
        y.huge_node_cnt = 0;
        y.huge_list.size = 0;
        for(auto& next : y.huge_list.next) next = list_tail;
    }
    MemPool_ThisType& operator=(MemPool_ThisType&& y) noexcept {
        if (&y == this)
            return *this;
        this->~MemPool_ThisType();
        ::new(this) MemPool_ThisType(y);
        return *this;
    }
#endif

    void get_fastbin(valvec<size_t>* fast) const {
        fast->resize_fill(free_list_len, 0);
        size_t* ptr = fast->data();
        head_t* arr = free_list_arr;
        size_t  len = free_list_len;
        for (size_t i = 0; i < len; ++i)
            ptr[i] = arr[i].cnt;
    }

    size_t get_huge_stat(size_t* huge_memsize) const {
        *huge_memsize = huge_size_sum;
        return huge_node_cnt;
    }

    using mem::data;
    using mem::size; // bring to public...
//  using mem::shrink_to_fit;
    using mem::reserve;
    using mem::capacity;
    using mem::risk_release_ownership;

    void risk_set_data(const void* data, size_t len) {
        assert(NULL == mem::p);
        assert(0 == mem::n);
        assert(0 == mem::c);
        mem::risk_set_data((unsigned char*)data, len);
        clear_free_list();
    }

    unsigned char byte_at(size_t pos) const {
        assert(pos < n);
        return p[pos];
    }

    // keep free_list_arr
    void clear() {
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
        fragment_size = 0;
        huge_size_sum = 0;
        huge_node_cnt = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, head_t());
        mem::clear();
    }

    void erase_all() {
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
        fragment_size = 0;
        huge_size_sum = 0;
        huge_node_cnt = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, head_t());
        mem::erase_all();
    }

    void resize_no_init(size_t newsize) {
        assert(newsize % align_size == 0);
        assert(newsize >= mem::size());
        mem::resize_no_init(newsize);
    }

    void shrink_to_fit() {
        mem::shrink_to_fit();
    }

    template<class U> const U& at(size_t pos) const {
        assert(pos < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + pos);
    }
    template<class U> U& at(size_t pos) {
        assert(pos < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + pos);
    }

    // param request must be aligned by align_size
    size_t alloc(size_t request) {
        assert(request > 0);
        request = align_up(request, AlignSize);
        if (AlignSize < sizeof(link_t)) // const expression
            request = std::max(sizeof(link_t), request);
        if (request <= free_list_len * align_size) {
            size_t idx = request / align_size - 1;
            auto& list = free_list_arr[idx];
            if (list_tail != list.head) {
                assert(fragment_size >= request);
                size_t pos = size_t(list.head) << offset_shift;
                assert(pos + request <= this->n);
                list.head = at<link_size_t>(pos);
                list.cnt--;
                fragment_size -= request;
                return pos;
            }
            else {
                size_t pos = n;
                size_t End = pos + request;
                ensure_capacity(End);
                n = End;
                return pos;
            }
        }
        else { // find in freelist, use first match
            size_t res = list_tail;
            assert(request >= sizeof(huge_link_t));
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2 = nullptr;
            size_t k = huge_list.size;
            while (k-- > 0) {
                while (n1->next[k] != list_tail && (n2 = &at<huge_link_t>(size_t(n1->next[k]) << offset_shift))->size < request)
                    n1 = n2;
                update[k] = n1;
            }
            if (n2 != nullptr && n2->size >= request) {
                assert((byte*)n2 >= p);
                size_t remain = n2->size - request;
                res = size_t((byte*)n2 - p);
                size_t res_shift = res >> offset_shift;
                for (k = 0; k < huge_list.size; ++k)
                    if ((n1 = update[k])->next[k] == res_shift)
                        n1->next[k] = n2->next[k];
                while (huge_list.next[huge_list.size - 1] == list_tail && --huge_list.size > 0)
                    ;
                if (remain)
                    sfree(res + request, remain);
                fragment_size -= request;
                huge_size_sum -= request;
                huge_node_cnt--;
            }
            if (list_tail == res) {
                res = n;
                size_t End = res + request;
                ensure_capacity(End);
                n = End;
            }
            return res;
        }
    }

    size_t alloc3(size_t pos, size_t oldlen, size_t newlen) {
        assert(newlen > 0); newlen = align_up(newlen, AlignSize);
        assert(oldlen > 0); oldlen = align_up(oldlen, AlignSize);
        if (AlignSize < sizeof(link_t)) { // const expression
            oldlen = std::max(sizeof(link_t), oldlen);
            newlen = std::max(sizeof(link_t), newlen);
        }
        assert(pos < n);
        assert(pos + oldlen <= n);
        if (pos + oldlen == n) {
            size_t End = pos + newlen;
            ensure_capacity(End);
            n = End;
            return pos;
        }
        if (newlen < oldlen) {
            assert(oldlen - newlen >= sizeof(link_t));
            assert(oldlen - newlen >= align_size);
            sfree(pos + newlen, oldlen - newlen);
            return pos;
        }
        else if (newlen == oldlen) {
            // do nothing
            return pos;
        }
        else {
            size_t newpos = alloc(newlen);
            memcpy(p + newpos, p + pos, std::min(oldlen, newlen));
            sfree(pos, oldlen);
            return newpos;
        }
    }

    void sfree(size_t pos, size_t len) {
        assert(len > 0);
        assert(pos < n);
        assert(pos % AlignSize == 0);
        len = align_up(len, AlignSize);
        if (AlignSize < sizeof(link_t)) // const expression
            len = std::max(sizeof(link_t), len);
        assert(pos + len <= n);
        if (pos + len == n) {
            n = pos;
			return;
        }
        if (len <= free_list_len * align_size) {
            size_t idx = len / align_size - 1;
            auto& list = free_list_arr[idx];
            at<link_t>(pos) = list.head;
            TERARK_IF_DEBUG(memset(&at<link_t>(pos) + 1, 0xDD, len-sizeof(link_t)),);
            list.head = link_size_t(pos >> offset_shift);
            list.cnt++;
        }
        else {
            assert(len >= sizeof(huge_link_t));
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2;
            size_t k = huge_list.size;
            while (k-- > 0) {
                while (n1->next[k] != list_tail && (n2 = &at<huge_link_t>(size_t(n1->next[k]) << offset_shift))->size < len)
                    n1 = n2;
                update[k] = n1;
            }
            k = random_level();
            if (k >= huge_list.size) {
                k = huge_list.size++;
                update[k] = &huge_list;
            };
            n2 = &at<huge_link_t>(pos);
            size_t pos_shift = pos >> offset_shift;
            do {
                n1 = update[k];
                n2->next[k] = n1->next[k];
                n1->next[k] = pos_shift;
            } while(k-- > 0);
            n2->size = len;
            huge_size_sum += len;
            huge_node_cnt++;
        }
        fragment_size += len;
    }

    size_t frag_size() const { return fragment_size; }

    void swap(MemPool_ThisType& y) {
        mem::swap(y);
        std::swap(free_list_arr, y.free_list_arr);
        std::swap(free_list_len, y.free_list_len);
        std::swap(fragment_size, y.fragment_size);
        std::swap(huge_size_sum, y.huge_size_sum);
        std::swap(huge_node_cnt, y.huge_node_cnt);
        std::swap(huge_list, y.huge_list);
    }

    template<class DataIO>
    friend void DataIO_loadObject(DataIO& dio, MemPool_ThisType& self) {
        typename DataIO::my_var_size_t var;
        self.clear();
        if (self.free_list_arr)
            ::free(self.free_list_arr);
        self.free_list_arr = NULL;
        self.free_list_len = 0;
        self.fragment_size = 0;
        self.huge_size_sum = 0;
        self.huge_node_cnt = 0;
        self.huge_list.size = 0;
        for (auto& next : self.huge_list.next) next = list_tail;
        dio >> var; self.huge_list.size = var.t;
        for (auto& next : self.huge_list.next) {
            dio >> var;
            next = var.t;
        }
        dio >> var;  self.fragment_size = var.t;
        dio >> var;  self.huge_size_sum = var.t;
        dio >> var;  self.huge_node_cnt = var.t;
        dio >> var;  self.free_list_len = var.t;
        self.free_list_arr = (head_t*)malloc(sizeof(head_t) * self.free_list_len);
        if (NULL == self.free_list_arr) {
            self.free_list_arr = NULL;
            self.free_list_len = 0;
            self.fragment_size = 0;
            self.huge_list.size = 0;
            for (auto& next : self.huge_list.next) next = list_tail;
            throw std::bad_alloc();
        }
		size_t  fastnum = self.free_list_len;
		head_t* fastbin = self.free_list_arr;
        for (size_t i = 0; i < fastnum; ++i) {
            dio >> var;
            fastbin[i].head = link_size_t(var.t);
            dio >> var;
            fastbin[i].cnt = link_size_t(var.t);
        }
        dio >> static_cast<mem&>(self);
    }

    template<class DataIO>
    friend void DataIO_saveObject(DataIO& dio, const MemPool_ThisType& self) {
        const size_t  fastnum = self.free_list_len;
        const head_t* fastbin = self.free_list_arr;
        typename DataIO::my_var_size_t var;
        dio << typename DataIO::my_var_size_t(self.huge_list.size);
        for (auto& next : self.huge_list.next)
            dio << typename DataIO::my_var_size_t(next);
        dio << typename DataIO::my_var_size_t(self.fragment_size);
        dio << typename DataIO::my_var_size_t(self.huge_size_sum);
        dio << typename DataIO::my_var_size_t(self.huge_node_cnt);
        dio << typename DataIO::my_var_size_t(fastnum);
        for (size_t i = 0; i < fastnum; ++i) {
            dio << typename DataIO::my_var_size_t(fastbin[i].head);
            dio << typename DataIO::my_var_size_t(fastbin[i].cnt);
        }
        dio << static_cast<const mem&>(self);
    }
};
#undef MemPool_ThisType

} // namespace terark


