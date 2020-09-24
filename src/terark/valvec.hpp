#ifndef __penglei_valvec_hpp__
#define __penglei_valvec_hpp__

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <utility>

#include <boost/type_traits.hpp>
#include <boost/static_assert.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/mpl/if.hpp>
#include <boost/utility.hpp>

#include <terark/util/autofree.hpp>
#include <terark/util/function.hpp>
#include "config.hpp"
#include "stdtypes.hpp"

#if defined(TERARK_HAS_WEAK_SYMBOL) && 0
  #define TERARK_VALVEC_HAS_WEAK_SYMBOL 1
#endif

#if TERARK_VALVEC_HAS_WEAK_SYMBOL
extern "C" {
    // jemalloc specific
    size_t TERARK_WEAK_SYMBOL xallocx(void*, size_t, size_t, int flags);
  #if defined(__GLIBC__) && defined(__THROW)
    size_t TERARK_WEAK_SYMBOL malloc_usable_size(void*) __THROW;
  #else
    size_t TERARK_WEAK_SYMBOL malloc_usable_size(void*);
  #endif
};
#endif

#if defined(__GNUC__) && __GNUC_MINOR__ + 1000 * __GNUC__ > 9000
  #pragma GCC diagnostic push
  //#pragma GCC diagnostic ignored "-Wno-class-memaccess" // which version support?
#endif

namespace terark {

    enum class MemType {
        Malloc,
        Mmap,
        User,
    };
    // defined in util/mmap.cpp
    TERARK_DLL_EXPORT void mmap_close(void* base, size_t size);

    template<class T>
    struct ParamPassType {
        static const bool is_pass_by_value =
                sizeof(T) <= sizeof(void*)
            && (sizeof(T) & (sizeof(T)-1 )) == 0 // power of 2
            && boost::has_trivial_copy<T>::value
            && boost::has_trivial_copy_constructor<T>::value
            && boost::has_trivial_destructor<T>::value
        ;
        typedef typename boost::mpl::if_c<is_pass_by_value,
            const T, const T&>::type type;
    };

#if (defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L || \
	 defined(_MSC_VER) && _MSC_VER >= 1700) && 0
	template <typename T>
	class is_iterator_impl {
	  static char test(...);
	  template <typename U,
		typename=typename std::iterator_traits<U>::difference_type,
		typename=typename std::iterator_traits<U>::pointer,
		typename=typename std::iterator_traits<U>::reference,
		typename=typename std::iterator_traits<U>::value_type,
		typename=typename std::iterator_traits<U>::iterator_category
	  > static long test(U*);
	public:
	  static const bool value = sizeof(test((T*)(NULL))) == sizeof(long);
	};
#else
	template <typename T>
	class is_iterator_impl {
		static T makeT();
		static char test(...); // Common case
		template<class R> static typename R::iterator_category* test(R);
		template<class R> static void* test(R*); // Pointer
	public:
		static const bool value = sizeof(test(makeT())) == sizeof(void*);
	};
#endif
	template <typename T>
	struct is_iterator :
		public boost::mpl::bool_<is_iterator_impl<T>::value> {};

#if defined(_MSC_VER) || defined(_LIBCPP_VERSION)
	template<class T>
	void STDEXT_destroy_range_aux(T*, T*, boost::mpl::true_) {}
	template<class T>
	void STDEXT_destroy_range_aux(T* p, T* q, boost::mpl::false_) {
		for (; p < q; ++p) p->~T();
	}
	template<class T>
	void STDEXT_destroy_range(T* p, T* q) {
		STDEXT_destroy_range_aux(p, q, boost::has_trivial_destructor<T>());
	}
#else
	#define STDEXT_destroy_range std::_Destroy
#endif

template<class ForwardIt, class Size>
ForwardIt
always_uninitialized_default_construct_n_aux(ForwardIt first, Size n,
                                             std::false_type /*nothrow*/) {
    using T = typename std::iterator_traits<ForwardIt>::value_type;
    ForwardIt current = first;
    try {
        for (; n > 0 ; (void) ++current, --n) {
            ::new (static_cast<void*>(std::addressof(*current))) T ();
			// ------------------------------- init primitives  ---^^
			// std::uninitialized_default_construct_n will not init primitives
        }
        return current;
    }  catch (...) {
        STDEXT_destroy_range(first, current);
        throw;
    }
}

template<class ForwardIt, class Size>
ForwardIt
always_uninitialized_default_construct_n_aux(ForwardIt first, Size n,
                                             std::true_type /*nothrow*/) {
    using T = typename std::iterator_traits<ForwardIt>::value_type;
    ForwardIt current = first;
    for (; n > 0 ; (void) ++current, --n) {
        ::new (static_cast<void*>(std::addressof(*current))) T ();
        // ------------------------------- init primitives  ---^^
        // std::uninitialized_default_construct_n will not init primitives
    }
    return current;
}

template<class ForwardIt, class Size>
ForwardIt
always_uninitialized_default_construct_n(ForwardIt first, Size n) {
    using T = typename std::iterator_traits<ForwardIt>::value_type;
    return always_uninitialized_default_construct_n_aux(
            first, n, std::is_nothrow_default_constructible<T>());
}

template<class T>
inline bool is_object_overlap(const T* x, const T* y) {
	assert(NULL != x);
	assert(NULL != y);
	if (x+1 <= y || y+1 <= x)
		return false;
	else
		return true;
}

struct valvec_no_init {
	template<class Vec>
	void operator()(Vec& v, size_t n) const { v.resize_no_init(n); }
};
struct valvec_reserve {
	template<class Vec>
	void operator()(Vec& v, size_t n) const { v.reserve(n); }
};

inline size_t larger_capacity(size_t oldcap) {
    // use ull for 32 bit platform
    typedef unsigned long long ull;
    return size_t(ull(oldcap) * 103 / 64); // 103/64 = 1.609375 <~ 1.618
}

/// similary with std::vector, but:
///   1. use realloc to enlarge/shrink memory, this has avoid memcpy when
///      realloc is implemented by mremap for large chunk of memory;
///      mainstream realloc are implemented in this way
///   2. valvec also avoid calling copy-cons/move-cons when enlarge the valvec
///@Note
///  1. T must be memmove-able, std::list,map,set... are not memmove-able,
///     some std::string with short string optimization is not memmove-able,
///     a such known std::string is g++-5.3's implementation
///  2. std::vector, ... are memmove-able, they could be the T, I'm not 100% sure,
///     since the g++-5.3's std::string has trapped me once
template<class T>
class valvec {
protected:
	struct AutoMemory { // for exception-safe
		T* p;
		explicit AutoMemory(size_t n) {
			p = (T*)malloc(sizeof(T) * n);
			if (NULL == p) throw std::bad_alloc();
		}
	//	explicit AutoMemory(T* q) {
	//		p = q;
	//		if (NULL == q) throw std::bad_alloc();
	//	}
	//	AutoMemory() { p = NULL; }
		~AutoMemory() { if (p) free(p); }
	};

    T*     p;
    size_t n;
    size_t c; // capacity

	template<class InputIter>
	void construct(InputIter first, ptrdiff_t count) {
		assert(count >= 0);
		p = NULL;
		n = c = 0;
		if (count) { // for exception-safe
			AutoMemory tmp(count);
			STDEXT_uninitialized_copy_n(first, count, tmp.p);
			p = tmp.p;
			n = c = count;
			tmp.p = NULL;
		}
	}

	template<class> struct void_ { typedef void type; };

	template<class InputIter>
	void construct(InputIter first, InputIter last, std::input_iterator_tag) {
		p = NULL;
		n = c = 0;
		for (; first != last; ++first)
			this->push_back(*first);
	}
	template<class ForwardIter>
	void construct(ForwardIter first, ForwardIter last, std::forward_iterator_tag) {
		ptrdiff_t count = std::distance(first, last);
		assert(count >= 0);
		construct(first, count);
	}

	template<class InputIter>
	void assign_aux(InputIter first, InputIter last, std::input_iterator_tag) {
		resize(0);
		for (; first != last; ++first)
			this->push_back(*first);
	}
	template<class ForwardIter>
	void assign_aux(ForwardIter first, ForwardIter last, std::forward_iterator_tag) {
		ptrdiff_t count = std::distance(first, last);
		assign(first, count);
	}

public:
    typedef T  value_type;
    typedef T* iterator;
    typedef T& reference;
    typedef const T* const_iterator;
    typedef const T& const_reference;
    typedef typename ParamPassType<T>::type param_type;

    typedef std::reverse_iterator<T*> reverse_iterator;
    typedef std::reverse_iterator<const T*> const_reverse_iterator;

    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    T* begin() { return p; }
    T* end()   { return p + n; }

    const T* begin() const { return p; }
    const T* end()   const { return p + n; }

    reverse_iterator rbegin() { return reverse_iterator(p + n); }
    reverse_iterator rend()   { return reverse_iterator(p); }

    const_reverse_iterator rbegin() const { return const_reverse_iterator(p + n); }
    const_reverse_iterator rend()   const { return const_reverse_iterator(p); }

    const T* cbegin() const { return p; }
    const T* cend()   const { return p + n; }

    const_reverse_iterator crbegin() const { return const_reverse_iterator(p + n); }
    const_reverse_iterator crend()   const { return const_reverse_iterator(p); }

    valvec() {
        p = NULL;
        n = c = 0;
    }

    valvec(size_t sz, param_type val) {
        if (sz) {
			AutoMemory tmp(sz);
            std::uninitialized_fill_n(tmp.p, sz, val);
			p = tmp.p;
			n = c = sz;
			tmp.p = NULL;
        } else {
            p = NULL;
            n = c = 0;
        }
    }
    explicit valvec(size_t sz) {
        if (sz) {
			AutoMemory tmp(sz);
            always_uninitialized_default_construct_n(tmp.p, sz);
			p = tmp.p;
			n = c = sz;
			tmp.p = NULL;
        } else {
            p = NULL;
            n = c = 0;
        }
    }

	// size = capacity = sz, memory is not initialized/constructed
	terark_no_inline
	valvec(size_t sz, valvec_no_init) {
		p = NULL;
		n = c = 0;
		if (sz) {
			p = (T*)malloc(sizeof(T) * sz);
			if (NULL == p) {
				throw std::bad_alloc();
			}
			n = c = sz;
		}
	}
	// size = 0, capacity = sz
	terark_no_inline
	valvec(size_t sz, valvec_reserve) {
		p = NULL;
		n = c = 0;
		if (sz) {
			p = (T*)malloc(sizeof(T) * sz);
			if (NULL == p) {
				throw std::bad_alloc();
			}
			c = sz;
		}
	}

	template<class AnyIter>
	explicit
	valvec(const std::pair<AnyIter, AnyIter>& rng
		 , typename boost::enable_if<is_iterator<AnyIter>,int>::type = 1) {
		construct(rng.first, rng.second, typename std::iterator_traits<AnyIter>::iterator_category());
	}
	explicit
	valvec(const std::pair<const T*, const T*>& rng) {
		construct(rng.first, rng.second - rng.first);
	}
	valvec(const T* first, const T* last) { construct(first, last-first); }
	valvec(const T* first, ptrdiff_t len) { construct(first, len); }
	template<class AnyIter>
	valvec(AnyIter first, AnyIter last
		, typename boost::enable_if<is_iterator<AnyIter>, int>::type = 1) {
		construct(first, last, typename std::iterator_traits<AnyIter>::iterator_category());
	}
	template<class InputIter>
	valvec(InputIter first, ptrdiff_t count
		, typename boost::enable_if<is_iterator<InputIter>, int>::type = 1) 	{
		assert(count >= 0);
		construct(first, count);
	}

	valvec(const valvec& y) {
        assert(this != &y);
		assert(!is_object_overlap(this, &y));
		construct(y.p, y.n);
    }

    valvec& operator=(const valvec& y) {
        if (&y == this)
            return *this;
		assert(!is_object_overlap(this, &y));
		assign(y.p, y.n);
        return *this;
    }

#ifdef HSM_HAS_MOVE
    valvec(valvec&& y) noexcept {
        assert(this != &y);
		assert(!is_object_overlap(this, &y));
		p = y.p;
		n = y.n;
		c = y.c;
		y.p = NULL;
		y.n = 0;
		y.c = 0;
	}

	valvec& operator=(valvec&& y) noexcept {
        assert(this != &y);
        if (&y == this)
            return *this;
		assert(!is_object_overlap(this, &y));
		this->clear();
		new(this)valvec(y);
		return *this;
	}
#endif // HSM_HAS_MOVE

    ~valvec() { clear(); }

    void fill(param_type x) {
        std::fill_n(p, n, x);
    }

    void fill(size_t pos, size_t cnt, param_type x) {
    	assert(pos <= n);
    	assert(pos + cnt <= n);
    	std::fill_n(p + pos, cnt, x);
    }

    template<class AnyIter>
	typename boost::enable_if<is_iterator<AnyIter>, void>::type
    assign(const std::pair<AnyIter, AnyIter>& rng) {
		assign_aux(rng.first, rng.second, typename std::iterator_traits<AnyIter>::iterator_category());
    }
    template<class AnyIter>
	typename boost::enable_if<is_iterator<AnyIter>, void>::type
    assign(AnyIter first, AnyIter last) {
		assign_aux(first, last, typename std::iterator_traits<AnyIter>::iterator_category());
    }
    template<class InputIter>
	typename boost::enable_if<is_iterator<InputIter>, void>::type
    assign(InputIter first, ptrdiff_t len) {
		assert(len >= 0);
		erase_all();
        reserve(len);
		STDEXT_uninitialized_copy_n(first, len, p);
		n = len;
    }
    void assign(const std::pair<const T*, const T*>& rng) {
		assign(rng.first, rng.second);
    }
    void assign(const T* first, const T* last) {
		assign(first, last - first);
    }
    void assign(const T* first, ptrdiff_t len) {
		assert(len >= 0);
		erase_all();
        reserve(len);
		STDEXT_uninitialized_copy_n(first, len, p);
		n = len;
    }
	template<class Container>
	typename void_<typename Container::const_iterator>::type
	assign(const Container& cont) { assign(cont.begin(), cont.size()); }

    void clear() {
        if (p) {
			STDEXT_destroy_range(p, p + n);
            free(p);
        }
        p = NULL;
        n = c = 0;
    }

	const T* data() const { return p; }
	      T* data()       { return p; }

	const T* finish() const { return p + c; }
	      T* finish()       { return p + c; }

    bool  empty() const { return 0 == n; }
    bool   full() const { return c == n; }
    size_t size() const { return n; }
    size_t capacity() const { return c; }
	size_t unused() const { return c - n; }

	size_t used_mem_size() const { return sizeof(T) * n; }
	size_t full_mem_size() const { return sizeof(T) * c; }
	size_t free_mem_size() const { return sizeof(T) * (c - n); }

    void reserve(size_t newcap) {
        if (newcap <= c)
            return; // nothing to do
        reserve_slow(newcap);
    }

private:
    terark_no_inline
    void reserve_slow(size_t newcap) {
        assert(newcap > c);
        T* q = (T*)realloc(p, sizeof(T) * newcap);
        if (NULL == q) throw std::bad_alloc();
        p = q;
        c = newcap;
    }

public:
    T* ensure_unused(size_t unused_cap) {
        size_t oldsize = n;
        ensure_capacity(oldsize + unused_cap);
        return p + oldsize;
    }
    void ensure_capacity(size_t min_cap) {
        if (terark_likely(min_cap <= c)) {
            // nothing to do
            return;
        }
        ensure_capacity_slow(min_cap);
    }
private:
    terark_no_inline
    void ensure_capacity_slow(size_t min_cap) {
        assert(min_cap > c);
        size_t new_cap = std::max(larger_capacity(c), min_cap);
#if defined(TERARK_VALVEC_HAS_WEAK_SYMBOL)
        if (xallocx) {
            size_t extra = sizeof(T) * (new_cap - min_cap);
            size_t minsz = sizeof(T) * min_cap;
            size_t usesz = xallocx(p, minsz, extra, 0);
            if (usesz >= minsz) {
                c = usesz / sizeof(T); // done
                return;
            }
        }
        else if (malloc_usable_size) {
            size_t usesz = malloc_usable_size(p);
            if (usesz >= sizeof(T) * min_cap) {
                c = usesz / sizeof(T); // done
                return;
            }
        }
#endif
        T* q = (T*)realloc(p, sizeof(T) * new_cap);
        if (NULL == q) throw std::bad_alloc();
        p = q;
        c = new_cap;
    }

public:
    void try_capacity(size_t min_cap, size_t max_cap) {
		if (terark_likely(min_cap <= c)) {
			// nothing to do
			return;
		}
        try_capacity_slow(min_cap, max_cap);
    }
private:
    terark_no_inline
    void try_capacity_slow(size_t min_cap, size_t max_cap) {
        assert(min_cap > c);
        if (max_cap < min_cap) {
            max_cap = min_cap;
        }
        size_t cur_cap = max_cap;
        for (;;) {
            T* q = (T*)realloc(p, sizeof(T) * cur_cap);
            if (q) {
                p = q;
                c = cur_cap;
                return;
            }
            if (cur_cap == min_cap) {
                throw std::bad_alloc();
            }
            cur_cap = min_cap + (cur_cap - min_cap) / 2;
        }
    }

public:
    T& ensure_get(size_t i) {
		if (i < n)
			return p[i];
		else
			return ensure_get_slow(i);
    }
	T& ensure_set(size_t i, param_type x) {
		if (i < n)
			return p[i] = x;
		else
			return ensure_set_slow(i, x);
	}
private:
	terark_no_inline
	T& ensure_get_slow(size_t i) {
		assert(n <= i);
		resize_slow(i+1);
		return p[i];
	}
	terark_no_inline
	T& ensure_set_slow(size_t i, param_type x) {
		assert(n <= i);
		resize_slow(i+1);
		T* beg = p; // load p into register
		beg[i] = x;
		return beg[i];
	}
public:

    terark_no_inline
    void shrink_to_fit() {
        assert(n <= c);
        if (n == c)
            return;
        if (n) {
			if (T* q = (T*)realloc(p, sizeof(T) * n)) {
				p = q;
				c = n;
			}
        } else {
            if (p)
                free(p);
            p = NULL;
            c = n = 0;
        }
    }

    // may do nothing
    void shrink_to_fit_inplace() {
#if defined(TERARK_VALVEC_HAS_WEAK_SYMBOL)
        if (xallocx) {
            size_t usesz = xallocx(p, sizeof(T) * n, 0, 0);
            c = usesz / sizeof(T); // done
        }
#endif
    }

	// expect this function will reduce memory fragment
	terark_no_inline
	void shrink_to_fit_malloc_free() {
		if (0 == c) return;
		assert(NULL != p);
		if (0 == n) {
			free(p);
			p = NULL;
			c = 0;
			return;
		}
		if (n == c)
			return;
		// malloc new and free old even if n==c
		// because this may trigger the memory compaction
		// of the malloc implementation and reduce memory fragment
		if (T* q = (T*)malloc(sizeof(T) * n)) {
			memcpy(q, p, sizeof(T) * n);
			free(p);
			c = n;
			p = q;
		} else {
			shrink_to_fit();
		}
	}

    void resize(size_t newsize, param_type val) {
        size_t oldsize = n;
        if (boost::has_trivial_destructor<T>::value) {
            if (newsize <= oldsize) {
                n = newsize;
                return;
            }
        }
        else {
            if (newsize == oldsize)
                return;
        }
        resize_slow(newsize, val);
    }
private:
    terark_no_inline
    void resize_slow(size_t newsize, param_type val) {
        size_t oldsize = n;
        assert(oldsize != newsize);
        if (!boost::has_trivial_destructor<T>::value && newsize <= oldsize) {
			STDEXT_destroy_range(p + newsize, p + oldsize);
        }
        else {
            assert(oldsize < newsize);
            ensure_capacity(newsize);
            std::uninitialized_fill_n(p+oldsize, newsize-oldsize, val);
        }
        n = newsize;
    }

public:
    void resize(size_t newsize) {
        size_t oldsize = n;
        if (boost::has_trivial_destructor<T>::value) {
            if (newsize <= oldsize) {
                n = newsize;
                return;
            }
        }
        else {
            if (newsize == oldsize)
                return;
        }
        resize_slow(newsize);
    }
private:
    terark_no_inline
    void resize_slow(size_t newsize) {
        size_t oldsize = n;
        assert(oldsize != newsize);
        if (!boost::has_trivial_destructor<T>::value && newsize <= oldsize) {
            STDEXT_destroy_range(p + newsize, p + oldsize);
        }
        else {
            assert(oldsize < newsize);
            ensure_capacity(newsize);
            always_uninitialized_default_construct_n(p + oldsize, newsize - oldsize);
        }
        n = newsize;
    }

public:
	terark_no_inline
	void resize_fill(size_t newsize) {
		ensure_capacity(newsize);
		STDEXT_destroy_range(p, p + n);
		if (!std::is_nothrow_default_constructible<T>::value) {
			n = 0;
		}
		always_uninitialized_default_construct_n(p, newsize);
		n = newsize;
	}

	terark_no_inline
	void resize_fill(size_t newsize, param_type val) {
		ensure_capacity(newsize);
		STDEXT_destroy_range(p, p + n);
		if (!std::is_nothrow_copy_constructible<T>::value) {
			n = 0;
		}
		std::uninitialized_fill_n(p, newsize, val);
		n = newsize;
	}

	void erase_all() {
		if (!boost::has_trivial_destructor<T>::value)
			STDEXT_destroy_range(p, p + n);
		n = 0;
	}

    // client code should pay the risk for performance gain
    void resize_no_init(size_t newsize) {
    //  assert(boost::has_trivial_constructor<T>::value);
        ensure_capacity(newsize);
        n = newsize;
    }
    // client code should pay the risk for performance gain
	void resize0() { n = 0; }

	///  trim [from, end)
	void trim(T* from) {
		assert(from >= p);
		assert(from <= p + n);
		if (!boost::has_trivial_destructor<T>::value)
    		STDEXT_destroy_range(from, p + n);
		n = from - p;
	}

	/// trim [from, size)
	/// @param from is the new size
	/// when trim(0) is ambiguous, use vec.erase_all(), or:
	/// vec.trim(size_t(0));
	void trim(size_t from) {
		assert(from <= n);
		if (!boost::has_trivial_destructor<T>::value)
    		STDEXT_destroy_range(p + from, p + n);
		n = from;
	}

	void insert(const T* pos, param_type x) {
		assert(pos <= p + n);
		assert(pos >= p);
		insert(pos-p, x);
	}

	terark_no_inline
	void insert(size_t pos, param_type x) {
		assert(pos <= n);
		if (pos > n) {
			throw std::out_of_range("valvec::insert");
		}
        ensure_capacity(n+1);
	//	for (ptrdiff_t i = n; i > pos; --i) memcpy(p+i, p+i-1, sizeof T);
		memmove(p+pos+1, p+pos, sizeof(T)*(n-pos));
		new(p+pos)T(x);
		++n;
	}

	template<class InputIter>
	terark_no_inline
	void insert(size_t pos, InputIter iter, size_t count) {
		assert(pos <= n);
		if (pos > n) {
			throw std::out_of_range("valvec::insert");
		}
        ensure_capacity(n+count);
	//	for (ptrdiff_t i = n; i > pos; --i) memcpy(p+i, p+i-1, sizeof T);
		memmove(p+pos+count, p+pos, sizeof(T)*(n-pos));
		STDEXT_uninitialized_copy_n(iter, count, p+pos);
		n += count;
	}

    void push_back() {
        size_t oldsize = n;
        assert(oldsize <= c);
        if (terark_unlikely(oldsize < c)) {
            new(p + oldsize)T(); // default cons
            n = oldsize + 1;
        } else {
            push_back_slow();
        }
    }
private:
    terark_no_inline
    void push_back_slow() {
        size_t oldsize = n;
        ensure_capacity(oldsize + 1);
        new(p + oldsize)T(); // default cons
        n = oldsize + 1;
    }

public:
	void push_back(param_type x) {
        size_t oldsize = n;
		if (terark_likely(oldsize < c)) {
			new(p+oldsize)T(x); // copy cons
			n = oldsize + 1;
		} else {
			push_back_slow(x);
		}
	}
private:
    terark_no_inline
    void push_back_slow(param_type x) {
        size_t oldsize = n;
        if (!ParamPassType<T>::is_pass_by_value &&
                &x >= p && &x < p + oldsize) {
            size_t idx = &x - p;
            ensure_capacity(oldsize + 1);
            new(p + oldsize)T(p[idx]); // copy cons
        }
        else {
            ensure_capacity(oldsize + 1);
            new(p + oldsize)T(x); // copy cons
        }
        n = oldsize + 1;
    }

public:
	void append(param_type x) { push_back(x); } // alias for push_back
	template<class Iterator>
	void append(Iterator first, ptrdiff_t len) {
		assert(len >= 0);
        size_t newsize = n + len;
        ensure_capacity(newsize);
        STDEXT_uninitialized_copy_n(first, len, p+n);
        n = newsize;
	}
	template<class Iterator>
	void append(Iterator first, Iterator last) {
		ptrdiff_t len = std::distance(first, last);
        size_t newsize = n + len;
        ensure_capacity(newsize);
        std::uninitialized_copy(first, last, p+n);
        n = newsize;
	}
	template<class Iterator>
	void append(const std::pair<Iterator, Iterator>& rng) {
		append(rng.first, rng.second);
	}
	template<class Container>
	typename void_<typename Container::const_iterator>::type
	append(const Container& cont) {
		append(cont.begin(), cont.end());
	}

    terark_no_inline
    void push_n(size_t cnt, param_type val) {
        if (boost::has_trivial_copy<T>::value) {
            std::uninitialized_fill_n(grow_no_init(cnt), cnt, val);
        }
        else {
            ensure_capacity(n + cnt);
            for (size_t i = 0; i < cnt; ++i) {
                unchecked_push_back(val);
            }
        }
    }

	T* grow_capacity(size_t cnt) {
		size_t oldsize = n;
		ensure_capacity(oldsize + cnt);
		return p + oldsize;
	}
	T* grow_no_init(size_t cnt) {
		size_t oldsize = n;
		resize_no_init(oldsize + cnt);
		return p + oldsize;
	}

	T* grow(size_t cnt, param_type x) {
		size_t oldsize = n;
		resize(oldsize + cnt, x);
		return p + oldsize;
	}

	T* grow(size_t cnt) {
		size_t oldsize = n;
		resize(oldsize + cnt);
		return p + oldsize;
	}

    void unchecked_push_back() {
		size_t oldsize = n;
		assert(oldsize < c);
        new(p+oldsize)T(); // default cons
        n = oldsize + 1;
    }
    void unchecked_push_back(param_type x) {
		size_t oldsize = n;
		assert(oldsize < c);
        new(p+oldsize)T(x); // copy cons
        n = oldsize + 1;
    }

    void pop_back() {
		assert(n > 0);
        size_t newsize = n-1;
        p[newsize].~T();
        n = newsize;
    }

// use valvec as stack ...
//
	void pop_n(size_t num) {
		assert(num <= this->n);
		STDEXT_destroy_range(this->p + this->n - num, this->p + this->n);
		this->n -= num;
	}
    void pop() { pop_back(); }
    void push() { push_back(); } // alias for push_back
    void push(param_type x) { push_back(x); } // alias for push_back

	param_type top() const {
        if (0 == n)
            throw std::logic_error("valvec::top() const, valec is empty");
		return p[n-1];
	}
	T& top() {
        if (0 == n)
            throw std::logic_error("valvec::top(), valec is empty");
		return p[n-1];
	}

	T pop_val() {
		assert(n > 0);
#ifdef HSM_HAS_MOVE
		T x(std::move(p[n-1]));
#else
		T x(p[n-1]);
        p[n-1].~T();
#endif
		--n;
		return x;
	}

    void unchecked_push() { unchecked_push_back(); }
    void unchecked_push(param_type x) { unchecked_push_back(x); }

// end use valvec as stack

    param_type operator[](size_t i) const {
        assert(i < n);
        return p[i];
    }

    T& operator[](size_t i) {
        assert(i < n);
        return p[i];
    }

    param_type at(size_t i) const {
        if (i >= n) throw std::out_of_range("valvec::at");
        return p[i];
    }

    T& at(size_t i) {
        if (i >= n) throw std::out_of_range("valvec::at");
        return p[i];
    }

    T& ref(size_t i) {
        assert(i < n);
        return p[i];
    }
    const T& ref(size_t i) const {
        assert(i < n);
        return p[i];
    }
    const T& cref(size_t i) const {
        assert(i < n);
        return p[i];
    }
    T* ptr(size_t i) {
        assert(i < n);
        return p + i;
    }
    const T* ptr(size_t i) const {
        assert(i < n);
        return p + i;
    }
    const T* cptr(size_t i) const {
        assert(i < n);
        return p + i;
    }

	void set(size_t i, param_type val) {
		assert(i < n);
		p[i] = val;
	}

    param_type front() const {
        assert(n);
        assert(p);
        return p[0];
    }
    T& front() {
        assert(n);
        assert(p);
        return p[0];
    }

    param_type back() const {
        assert(n);
        assert(p);
        return p[n-1];
    }
    T& back() {
        assert(n);
        assert(p);
        return p[n-1];
    }

    T& ende(size_t d) {
        assert(d <= n);
        return p[n-d];
    }
    param_type ende(size_t d) const {
        assert(d <= n);
        return p[n-d];
    }

    void operator+=(param_type x) {
        push_back(x);
    }

    void operator+=(const valvec& y) {
        size_t newsize = n + y.size();
        ensure_capacity(newsize);
        std::uninitialized_copy(y.p, y.p + y.n, p+n);
        n = newsize;
    }

    void swap(valvec& y) {
        std::swap(p, y.p);
        std::swap(n, y.n);
        std::swap(c, y.c);
    }

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103 \
 || defined(_MSC_VER) && _MSC_VER >= 1700

	// same as push_back()
	void emplace_back() { push_back(); }

#if defined(_MSC_VER) && _MSC_VER <= 1800
// C++: internal compiler error: variadic perfect forwarding
// https://connect.microsoft.com/VisualStudio/feedback/details/806017/c-internal-compiler-error-variadic-perfect-forwarding-to-base-class
// Can not call std::forward multiple times, even in different branch
// this emplace_back is buggy for vec.emplace_back(vec[0]);
	template<class... Args>
	void emplace_back(Args&&... args) {
		ensure_capacity(n + 1);
		new(p+n)T(std::forward<Args>(args)...);
		++n;
	}
#else
	template<class... Args>
	void emplace_back(Args&&... args) {
		size_t oldsize = n;
		assert(oldsize <= c);
        if (oldsize < c) {
			new(p+oldsize)T(std::forward<Args>(args)...);
			n = oldsize + 1;
		} else {
			emplace_back_slow(std::forward<Args>(args)...);
		}
	}
private:
	template<class... Args>
    terark_no_inline
	void emplace_back_slow(Args&&... args) {
        assert(n == c);
		T val(std::forward<Args>(args)...);
        ensure_capacity_slow(n+1);
		new(p+n)T(std::move(val));
		++n;
	}
public:
	template<class... Args>
	void unchecked_emplace_back(Args&&... args) {
		size_t oldsize = n;
		assert(oldsize < c);
		new(p+oldsize)T(std::forward<Args>(args)...);
		n = oldsize + 1;
	}
#endif

	explicit valvec(std::initializer_list<T> list) {
		construct(list.begin(), list.size());
	}

#else

	template<class A1>
	void emplace_back(const A1& a1) {
        if (n < c) {
			new(p+n)T(a1);
			++n;
		} else {
			emplace_back_slow(a1);
		}
	}
	template<class A1, class A2>
	void emplace_back(const A1& a1, const A2& a2) {
        if (n < c) {
			new(p+n)T(a1, a2);
			++n;
		} else {
			emplace_back_slow(a1, a2);
		}
	}
	template<class A1, class A2, class A3>
	void emplace_back(const A1& a1, const A2& a2, const A3& a3) {
        if (n < c) {
			new(p+n)T(a1, a2, a3);
			++n;
		} else {
			emplace_back_slow(a1, a2, a3);
		}
	}
	template<class A1, class A2, class A3, class A4>
	void emplace_back(const A1& a1, const A2& a2, const A3& a3, const A4& a4) {
        if (n < c) {
			new(p+n)T(a1, a2, a3, a4);
			++n;
		} else {
			emplace_back_slow(a1, a2, a3, a4);
		}
	}

	template<class A1>
	void emplace_back_slow(const A1& a1) {
		T val(a1);
        ensure_capacity(n+1);
		new(p+n)T(val);
		++n;
	}
	template<class A1, class A2>
	void emplace_back_slow(const A1& a1, const A2& a2) {
		T val(a1, a2);
        ensure_capacity(n+1);
		new(p+n)T(val);
		++n;
	}
	template<class A1, class A2, class A3>
	void emplace_back_slow(const A1& a1, const A2& a2, const A3& a3) {
		T val(a1, a2, a3);
        ensure_capacity(n+1);
		new(p+n)T(val);
		++n;
	}
	template<class A1, class A2, class A3, class A4>
	void emplace_back_slow(const A1& a1, const A2& a2, const A3& a3, const A4& a4) {
		T val(a1, a2, a3, a4);
        ensure_capacity(n+1);
		new(p+n)T(val);
		++n;
	}

	template<class A1>
	void unchecked_emplace_back(const A1& a1) {
		assert(n < c);
		new(p+n)T(a1);
		++n;
	}
	template<class A1, class A2>
	void unchecked_emplace_back(const A1& a1, const A2& a2) {
		assert(n < c);
		new(p+n)T(a1, a2);
		++n;
	}
	template<class A1, class A2, class A3>
	void unchecked_emplace_back(const A1& a1, const A2& a2, const A3& a3) {
		assert(n < c);
		new(p+n)T(a1, a2, a3);
		++n;
	}
	template<class A1, class A2, class A3, class A4>
	void unchecked_emplace_back(const A1& a1, const A2& a2, const A3& a3, const A4& a4) {
		assert(n < c);
		new(p+n)T(a1, a2, a3, a4);
		++n;
	}

#endif

	terark_no_inline
	size_t erase_i(size_t pos, size_t cnt) {
		assert(cnt <= this->n);
		assert(pos <= this->n);
		assert(pos + cnt <= this->n);
		STDEXT_destroy_range(p + pos, p + pos + cnt);
		memmove(p + pos, p + pos + cnt, sizeof(T) * (n - cnt - pos));
		n -= cnt;
		return pos;
	}

	std::pair<T*, T*> range() { return std::make_pair(p, p+n); }
	std::pair<const T*, const T*> range() const { return std::make_pair(p, p+n); }

	param_type get_2d(size_t colsize, size_t row, size_t col) const {
		size_t idx = row * colsize + col;
		assert(idx < n);
		return p[idx];
	}
	T& get_2d(size_t colsize, size_t row, size_t col) {
		size_t idx = row * colsize + col;
		assert(idx < n);
		return p[idx];
	}

	void risk_set_data(T* Data, size_t Size) {
		p = Data;
		n = Size;
		c = Size;
	}
	void risk_set_data(T* data) { p = data; }
	void risk_set_size(size_t size) { this->n = size; }
	void risk_set_capacity(size_t capa) { this->c = capa; }
    void risk_set_end(T* endp) {
        assert(size_t(endp - p) <= c);
        assert(size_t(endp - p) >= 0);
        n = endp - p;
    }

	T* risk_release_ownership() {
	//	BOOST_STATIC_ASSERT(boost::has_trivial_destructor<T>::value);
		T* q = p;
		p = NULL;
		n = c = 0;
		return q;
	}

	void risk_destroy(MemType mt) {
		switch (mt) {
			default:
				TERARK_DIE("invalid MemType = %d", int(mt));
				break;
			case MemType::Malloc:
				clear();
				break;
			case MemType::Mmap:
				assert(n <= c);
				TERARK_VERIFY(size_t(p) % 4096 == 0);
				mmap_close(p, sizeof(T) * c);
				no_break_fallthrough; // fall through
			case MemType::User:
				risk_release_ownership();
				break;
		}
	}

/*
 * Now serialization valvec<T> has been builtin DataIO
 *
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const valvec& x) {
		typename DataIO::my_var_uint64_t size(x.n);
		dio << size;
		// complex object has not implemented yet!
		BOOST_STATIC_ASSERT(boost::has_trivial_destructor<T>::value);
		dio.ensureWrite(x.p, sizeof(T)*x.n);
	}

   	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, valvec& x) {
		typename DataIO::my_var_uint64_t size;
		dio >> size;
		x.resize_no_init(size.t);
		// complex object has not implemented yet!
		BOOST_STATIC_ASSERT(boost::has_trivial_destructor<T>::value);
		dio.ensureRead(x.p, sizeof(T)*x.n);
	}
*/

    template<class TLess, class TEqual>
    static bool lessThan(const valvec& x, const valvec& y, TLess le, TEqual eq) {
        size_t n = std::min(x.n, y.n);
        for (size_t i = 0; i < n; ++i) {
            if (!eq(x.p[i], y.p[i]))
                return le(x.p[i], y.p[i]);
        }
        return x.n < y.n;
    }
    template<class TEqual>
    static bool equalTo(const valvec& x, const valvec& y, TEqual eq) {
        if (x.n != y.n)
            return false;
        for (size_t i = 0, n = x.n; i < n; ++i) {
            if (!eq(x.p[i], y.p[i]))
                return false;
        }
        return true;
    }
    template<class TLess = std::less<T>, class TEqual = std::equal_to<T> >
    struct less : TLess, TEqual {
        bool operator()(const valvec& x, const valvec& y) const {
            return lessThan<TLess, TEqual>(x, y, *this, *this);
        }
        less() {}
        less(TLess le, TEqual eq) : TLess(le), TEqual(eq) {}
    };
    template<class TEqual = std::equal_to<T> >
    struct equal : TEqual {
        bool operator()(const valvec& x, const valvec& y) const {
            return equalTo<TEqual>(x, y, *this);
        }
        equal() {}
        equal(TEqual eq) : TEqual(eq) {}
    };
};

template<class T>
bool valvec_lessThan(const valvec<T>& x, const valvec<T>& y) {
	return valvec<T>::lessThan(x, y, std::less<T>(), std::equal_to<T>());
}
template<class T>
bool valvec_equalTo(const valvec<T>& x, const valvec<T>& y) {
	return valvec<T>::equalTo(x, y, std::equal_to<T>());
}

template<class T>
bool operator==(const valvec<T>& x, const valvec<T>& y) {
	return valvec<T>::equalTo(x, y, std::equal_to<T>());
}
template<class T>
bool operator!=(const valvec<T>& x, const valvec<T>& y) {
	return !valvec<T>::equalTo(x, y, std::equal_to<T>());
}
template<class T>
bool operator<(const valvec<T>& x, const valvec<T>& y) {
	return valvec<T>::lessThan(x, y, std::less<T>(), std::equal_to<T>());
}
template<class T>
bool operator>(const valvec<T>& x, const valvec<T>& y) {
	return valvec<T>::lessThan(y, x, std::less<T>(), std::equal_to<T>());
}
template<class T>
bool operator<=(const valvec<T>& x, const valvec<T>& y) {
	return !valvec<T>::lessThan(y, x, std::less<T>(), std::equal_to<T>());
}
template<class T>
bool operator>=(const valvec<T>& x, const valvec<T>& y) {
	return !valvec<T>::lessThan(x, y, std::less<T>(), std::equal_to<T>());
}

/// STL like algorithm with array/RanIt and size_t param

template<class RanIt, class Key>
size_t lower_bound_n(RanIt a, size_t low, size_t upp, const Key& key) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (a[mid] < key)
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}
template<class RanIt, class Key, class Comp>
size_t lower_bound_n(RanIt a, size_t low, size_t upp, const Key& key, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(a[mid], key))
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}

template<class RanIt, class Key>
size_t upper_bound_n(RanIt a, size_t low, size_t upp, const Key& key) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (key < a[mid])
			j = mid;
		else
			i = mid + 1;
	}
	return i;
}

template<class RanIt, class Key, class Comp>
size_t upper_bound_n(RanIt a, size_t low, size_t upp, const Key& key, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(key, a[mid]))
			j = mid;
		else
			i = mid + 1;
	}
	return i;
}

template<class RanIt, class Key>
std::pair<size_t, size_t>
equal_range_n(RanIt a, size_t low, size_t upp, const Key& key) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (a[mid] < key)
			i = mid + 1;
		else if (key < a[mid])
			j = mid;
		else
			return std::pair<size_t, size_t>(
				lower_bound_n<RanIt, Key>(a, i, mid, key),
				upper_bound_n<RanIt, Key>(a, mid + 1, upp, key));
	}
	return std::pair<size_t, size_t>(i, i);
}

template<class RanIt, class Key, class Comp>
std::pair<size_t, size_t>
equal_range_n(RanIt a, size_t low, size_t upp, const Key& key, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(a[mid], key))
			i = mid + 1;
		else if (comp(key, a[mid]))
			j = mid;
		else
			return std::pair<size_t, size_t>(
				lower_bound_n<RanIt, Key, Comp>(a, i, mid, key, comp),
				upper_bound_n<RanIt, Key, Comp>(a, mid+1, j, key, comp));
	}
	return std::pair<size_t, size_t>(i, i);
}

template<class RanIt, class Key>
bool
binary_search_n(RanIt a, size_t low, size_t upp, const Key& key) {
	assert(low <= upp);
	size_t f = lower_bound_n<RanIt, Key>(a, low, upp, key);
	return f < upp && !(key < a[f]);
}
template<class RanIt, class Key, class Comp>
bool
binary_search_n(RanIt a, size_t low, size_t upp, const Key& key, Comp comp) {
	assert(low <= upp);
	size_t f = lower_bound_n<RanIt, Key, Comp>(a, low, upp, key, comp);
	return f < upp && !comp(key, a[f]);
}

template<class RanIt>
void sort_n(RanIt a, size_t low, size_t upp) {
	assert(low <= upp);
	std::sort<RanIt>(a + low, a + upp);
}
template<class RanIt, class Comp>
void sort_n(RanIt a, size_t low, size_t upp, Comp comp) {
	assert(low <= upp);
	std::sort<RanIt, Comp>(a + low, a + upp, comp);
}

template<class RanIt, class KeyExtractor>
void sort_ex(RanIt first, RanIt last, KeyExtractor keyEx) {
	assert(first <= last);
	std::sort(first, last, ExtractorLess(keyEx));
}
template<class RanIt, class KeyExtractor, class Comp>
void sort_ex(RanIt first, RanIt last, KeyExtractor keyEx, Comp cmp) {
	assert(first <= last);
	std::sort(first, last, ExtractorComparator(keyEx,cmp));
}

template<class RanIt, class KeyExtractor>
void sort_ex_n(RanIt a, size_t low, size_t upp, KeyExtractor keyEx) {
	assert(low <= upp);
	std::sort(a+low, a+upp, ExtractorLess(keyEx));
}
template<class RanIt, class KeyExtractor, class Comp>
void sort_ex_n(RanIt a, size_t low, size_t upp, KeyExtractor keyEx, Comp cmp) {
	assert(low <= upp);
	std::sort(a+low, a+upp, ExtractorComparator(keyEx,cmp));
}

template<class Container, class KeyExtractor>
void sort_ex_a(Container& a, KeyExtractor keyEx) {
	std::sort(std::begin(a), std::end(a), ExtractorLess(keyEx));
}
template<class Container, class KeyExtractor, class Comp>
void sort_ex_a(Container& a, KeyExtractor keyEx, Comp cmp) {
	std::sort(std::begin(a), std::end(a), ExtractorComparator(keyEx,cmp));
}

template<class RanIt, class Key>
size_t lower_bound_0(RanIt a, size_t n, const Key& key) {
	return lower_bound_n<RanIt, Key>(a, 0, n, key);
}
template<class RanIt, class Key, class Comp>
size_t lower_bound_0(RanIt a, size_t n, const Key& key, Comp comp) {
	return lower_bound_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template<class Container, class Key>
size_t lower_bound_a(const Container& a, const Key& key) {
	typedef typename Container::const_iterator RanIt;
	return lower_bound_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template<class Container, class Key, class Comp>
size_t lower_bound_a(const Container& a, const Key& key, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return lower_bound_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template<class RanIt, class Key>
size_t upper_bound_0(RanIt a, size_t n, const Key& key) {
	return upper_bound_n<RanIt, Key>(a, 0, n, key);
}
template<class RanIt, class Key, class Comp>
size_t upper_bound_0(RanIt a, size_t n, const Key& key, Comp comp) {
	return upper_bound_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template<class Container, class Key>
size_t upper_bound_a(const Container& a, const Key& key) {
	typedef typename Container::const_iterator RanIt;
	return upper_bound_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template<class Container, class Key, class Comp>
size_t upper_bound_a(const Container& a, const Key& key, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return upper_bound_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template<class RanIt, class Key>
std::pair<size_t, size_t>
equal_range_0(RanIt a, size_t n, const Key& key) {
	return equal_range_n<RanIt, Key>(a, 0, n, key);
}
template<class RanIt, class Key, class Comp>
std::pair<size_t, size_t>
equal_range_0(RanIt a, size_t n, const Key& key, Comp comp) {
	return equal_range_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template<class Container, class Key>
std::pair<size_t, size_t>
equal_range_a(const Container& a, const Key& key) {
	typedef typename Container::const_iterator RanIt;
	return equal_range_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template<class Container, class Key, class Comp>
std::pair<size_t, size_t>
equal_range_a(const Container& a, const Key& key, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return equal_range_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template<class RanIt, class Key>
bool
binary_search_0(RanIt a, size_t n, const Key& key) {
	return binary_search_n<RanIt, Key>(a, 0, n, key);
}
template<class RanIt, class Key, class Comp>
bool
binary_search_0(RanIt a, size_t n, const Key& key, Comp comp) {
	return binary_search_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}

template<class Range, class Key>
bool
binary_search_a(const Range& a, const Key& key) {
	return binary_search_n(a.begin(), 0, a.size(), key);
}
template<class Range, class Key, class Comp>
bool
binary_search_a(const Range& a, const Key& key, Comp comp) {
	return binary_search_n(a.begin(), 0, a.size(), key, comp);
}

/////////////////////////////////////////////////////////////////////////////
template<class RanIt, class Key, class KeyExtractor>
size_t
lower_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (keyEx(a[mid]) < key)
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
size_t
lower_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(keyEx(a[mid]), key))
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}

template<class RanIt, class Key, class KeyExtractor>
size_t
upper_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (key < keyEx(a[mid]))
			j = mid;
		else
			i = mid + 1;
	}
	return i;
}

template<class RanIt, class Key, class KeyExtractor, class Comp>
size_t
upper_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(key, keyEx(a[mid])))
			j = mid;
		else
			i = mid + 1;
	}
	return i;
}

template<class RanIt, class Key, class KeyExtractor>
std::pair<size_t, size_t>
equal_range_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (keyEx(a[mid]) < key)
			i = mid + 1;
		else if (key < keyEx(a[mid]))
			j = mid;
		else
			return std::pair<size_t, size_t>(
				lower_bound_ex_n<RanIt, Key, KeyExtractor>(a, i, mid, key, keyEx),
				upper_bound_ex_n<RanIt, Key, KeyExtractor>(a, mid+1, upp, key, keyEx));
	}
	return std::pair<size_t, size_t>(i, i);
}

template<class RanIt, class Key, class KeyExtractor, class Comp>
std::pair<size_t, size_t>
equal_range_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(low <= upp);
	size_t i = low, j = upp;
	while (i < j) {
		size_t mid = (i + j) / 2;
		if (comp(keyEx(a[mid]), key))
			i = mid + 1;
		else if (comp(key, keyEx(a[mid])))
			j = mid;
		else
			return std::pair<size_t, size_t>(
				lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, i, mid, key, keyEx, comp),
				upper_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, mid+1, j, key, keyEx, comp));
	}
	return std::pair<size_t, size_t>(i, i);
}

template<class RanIt, class Key, class KeyExtractor>
bool
binary_search_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx) {
	assert(low <= upp);
	size_t f = lower_bound_ex_n<RanIt, Key, KeyExtractor>(a, low, upp, key, keyEx);
	return f < upp && !(key < keyEx(a[f]));
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
bool
binary_search_ex_n(RanIt a, size_t low, size_t upp, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(low <= upp);
	size_t f = lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, low, upp, key, keyEx, comp);
	return f < upp && !comp(key, keyEx(a[f]));
}

template<class RanIt, class Key, class KeyExtractor>
size_t
lower_bound_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return lower_bound_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
size_t
lower_bound_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class Container, class Key, class KeyExtractor>
size_t
lower_bound_ex_a(const Container& a, const Key& key, KeyExtractor keyEx) {
	typedef typename Container::const_iterator RanIt;
	return lower_bound_ex_n<RanIt, Key, KeyExtractor>(a.begin(), 0, a.size(), key, keyEx);
}
template<class Container, class Key, class KeyExtractor, class Comp>
size_t
lower_bound_ex_a(const Container& a, const Key& key, KeyExtractor keyEx, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a.begin(), 0, a.size(), key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor>
RanIt
lower_bound_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return a + lower_bound_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor>
RanIt
lower_bound_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx) {
	assert(!(end < beg));
	return beg + lower_bound_ex_n<RanIt, Key, KeyExtractor>(beg, 0, end - beg, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
RanIt
lower_bound_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return a + lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
RanIt
lower_bound_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(!(end < beg));
	return beg + lower_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(beg, 0, end - beg, key, keyEx, comp);
}

template<class RanIt, class Key, class KeyExtractor>
size_t
upper_bound_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return upper_bound_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
size_t
upper_bound_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return upper_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class Container, class Key, class KeyExtractor>
size_t
upper_bound_ex_a(const Container& a, const Key& key, KeyExtractor keyEx) {
	typedef typename Container::const_iterator RanIt;
	return upper_bound_ex_n<RanIt, Key, KeyExtractor>(a.begin(), 0, a.size(), key, keyEx);
}
template<class Container, class Key, class KeyExtractor, class Comp>
size_t
upper_bound_ex_a(const Container& a, const Key& key, KeyExtractor keyEx, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return upper_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a.begin(), 0, a.size(), key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor>
RanIt
upper_bound_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return a + upper_bound_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor>
RanIt
upper_bound_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx) {
	assert(!(end < beg));
	return beg + upper_bound_ex_n<RanIt, Key, KeyExtractor>(beg, 0, end - beg, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
RanIt
upper_bound_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return a + upper_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
RanIt
upper_bound_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(!(end < beg));
	return beg + upper_bound_ex_n<RanIt, Key, KeyExtractor, Comp>(beg, 0, end - beg, key, keyEx, comp);
}

template<class RanIt, class Key, class KeyExtractor>
std::pair<size_t, size_t>
equal_range_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return equal_range_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
std::pair<size_t, size_t>
equal_range_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return equal_range_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class Container, class Key, class KeyExtractor>
std::pair<size_t, size_t>
equal_range_ex_a(const Container& a, const Key& key, KeyExtractor keyEx) {
	typedef typename Container::const_iterator RanIt;
	return equal_range_ex_n<RanIt, Key, KeyExtractor>(a.begin(), 0, a.size(), key, keyEx);
}
template<class Container, class Key, class KeyExtractor, class Comp>
std::pair<size_t, size_t>
equal_range_ex_a(const Container& a, const Key& key, KeyExtractor keyEx, Comp comp) {
	typedef typename Container::const_iterator RanIt;
	return equal_range_ex_n<RanIt, Key, KeyExtractor, Comp>(a.begin(), 0, a.size(), key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor>
std::pair<RanIt, RanIt>
equal_range_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	std::pair<size_t, size_t>
	r = equal_range_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
	return std::pair<RanIt, RanIt>(a + r.first, a + r.second);
}
template<class RanIt, class Key, class KeyExtractor>
std::pair<RanIt, RanIt>
equal_range_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx) {
	assert(!(end < beg));
	std::pair<size_t, size_t>
	r = equal_range_ex_n<RanIt, Key, KeyExtractor>(beg, 0, end - beg, key, keyEx);
	return std::pair<RanIt, RanIt>(beg + r.first, beg + r.second);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
std::pair<RanIt, RanIt>
equal_range_ex(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	std::pair<size_t, size_t>
	r = equal_range_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
	return std::pair<RanIt, RanIt>(a + r.first, a + r.second);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
std::pair<RanIt, RanIt>
equal_range_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(!(end < beg));
	std::pair<size_t, size_t>
	r = equal_range_ex_n<RanIt, Key, KeyExtractor, Comp>(beg, 0, end - beg, key, keyEx, comp);
	return std::pair<RanIt, RanIt>(beg + r.first, beg + r.second);
}

template<class RanIt, class Key, class KeyExtractor>
bool
binary_search_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx) {
	return binary_search_ex_n<RanIt, Key, KeyExtractor>(a, 0, n, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
bool
binary_search_ex_0(RanIt a, size_t n, const Key& key, KeyExtractor keyEx, Comp comp) {
	return binary_search_ex_n<RanIt, Key, KeyExtractor, Comp>(a, 0, n, key, keyEx, comp);
}
template<class Range, class Key, class KeyExtractor>
bool
binary_search_ex_a(const Range& a, const Key& key, KeyExtractor keyEx) {
	return binary_search_ex_n(a.begin(), 0, a.size(), key, keyEx);
}
template<class Range, class Key, class KeyExtractor, class Comp>
bool
binary_search_ex_a(const Range& a, const Key& key, KeyExtractor keyEx, Comp comp) {
	return binary_search_ex_n(a.begin(), 0, a.size(), key, keyEx, comp);
}
template<class RanIt, class Key, class KeyExtractor>
bool
binary_search_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx) {
	assert(!(end < beg));
	return binary_search_ex_n<RanIt, Key, KeyExtractor>(beg, 0, end - beg, key, keyEx);
}
template<class RanIt, class Key, class KeyExtractor, class Comp>
bool
binary_search_ex(RanIt beg, RanIt end, const Key& key, KeyExtractor keyEx, Comp comp) {
	assert(!(end < beg));
	return binary_search_ex_n<RanIt, Key, KeyExtractor, Comp>(beg, 0, end - beg, key, keyEx, comp);
}

/////////////////////////////////////////////////////////////////////////////

template<class RanIt>
void sort_0(RanIt a, size_t n) {
	sort_n<RanIt>(a, 0, n);
}
template<class RanIt, class Comp>
void sort_0(RanIt a, size_t n, Comp comp) {
	sort_n<RanIt, Comp>(a, 0, n, comp);
}
template<class Container>
void sort_a(Container& a) {
	std::sort(std::begin(a), std::end(a));
}
template<class Container, class Comp>
void sort_a(Container& a, Comp comp) {
	std::sort(std::begin(a), std::end(a), comp);
}

template<class RanIt>
void reverse_n(RanIt a, size_t low, size_t upp) {
	std::reverse<RanIt>(a + low, a + upp);
}
template<class RanIt>
void reverse_0(RanIt a, size_t n) {
	std::reverse<RanIt>(a + 0, a + n);
}
template<class Container>
void reverse_a(Container& a) {
	std::reverse(a.begin(), a.end());
}
template<class Container>
void reverse_a(Container& a, size_t low, size_t upp) {
	assert(low <= upp);
	assert(upp <= a.size());
	std::reverse(a.begin() + low, a.begin() + upp);
}

template<class RanIt>
size_t unique_n(RanIt a, size_t low, size_t upp) {
	assert(low <= upp);
	return std::unique<RanIt>(a + low, a + upp) - a;
}
template<class RanIt>
size_t unique_0(RanIt a, size_t n) {
	return std::unique<RanIt>(a + 0, a + n) - a;
}
template<class Container>
size_t unique_a(Container& a) {
	return std::unique(a.begin(), a.end()) - a.begin();
}
template<class Container, class Equal>
size_t unique_a(Container& a, Equal eq) {
	return std::unique(a.begin(), a.end(), eq) - a.begin();
}
template<class Container>
size_t unique_a(Container& a, size_t low, size_t upp) {
	assert(low <= upp);
	assert(upp <= a.size());
	return std::unique(a.begin() + low, a.begin() + upp) - low - a.begin();
}
template<class Container, class Equal>
size_t unique_a(Container& a, Equal eq, size_t low, size_t upp) {
	assert(low <= upp);
	assert(upp <= a.size());
	return std::unique(a.begin() + low, a.begin() + upp, eq) - low - a.begin();
}

template<class Container, class KeyExtractor>
size_t unique_ex_a(Container& a, KeyExtractor ex) {
	return std::unique(a.begin(), a.end(), ExtractorEqual(ex)) - a.begin();
}
template<class Container, class KeyExtractor, class Equal>
size_t unique_ex_a(Container& a, KeyExtractor ex, Equal eq) {
	return std::unique(a.begin(), a.end(), ExtractorComparator(ex, eq)) - a.begin();
}

} // namespace terark

namespace std {
	template<class T>
	void swap(terark::valvec<T>& x, terark::valvec<T>& y) { x.swap(y); }
}

#if defined(__GNUC__) && __GNUC_MINOR__ + 1000 * __GNUC__ > 9000
  #pragma GCC diagnostic pop
#endif

#endif

