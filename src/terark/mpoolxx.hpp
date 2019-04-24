/* vim: set tabstop=4 : */
#ifndef __terark_mpool_cxx_h__
#define __terark_mpool_cxx_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "c/mpool.h"
#include <string.h>
#include <new>
#include <assert.h>

namespace terark {

	template<class T> class mpoolxx
	{
// can not ensure the order of construction, so do not use static data member
//		static struct mpool* s_globalpool;
	public:
		typedef T value_type;
		typedef T *pointer;
		typedef T &reference;
		typedef const T *const_pointer;
		typedef const T &const_reference;
		typedef size_t    size_type;
		typedef ptrdiff_t difference_type;

		template<class Other>
		struct rebind
		{	// convert an mpoolxx<T> to an mpoolxx <Other>
			typedef mpoolxx<Other> other;
		};

		pointer address(reference _Val) const
		{	// return address of mutable _Val
			return (&_Val);
		}

		const_pointer address(const_reference _Val) const
		{	// return address of nonmutable _Val
			return (&_Val);
		}

		mpoolxx() throw()
		{	// construct default mpoolxx (do nothing)
		}

		mpoolxx(const mpoolxx<T>&) throw()
		{	// construct by copying (do nothing)
		}

		template<class Other>
			mpoolxx(const mpoolxx<Other>&) throw()
			{	// construct from a related mpoolxx (do nothing)
			}

		template<class Other>
			mpoolxx<T>& operator=(const mpoolxx<Other>&)
			{	// assign from a related mpoolxx (do nothing)
				return (*this);
			}

		void deallocate(pointer _Ptr, size_type _Size)
		{	// deallocate object at _Ptr
		//	mpool_sfree(s_globalpool, _Ptr, _Size);
			gsfree(_Ptr, _Size);
		}

		pointer allocate(size_type _Count)
		{	// allocate array of _Count elements
			if (terark_unlikely(_Count <= 0))
				_Count = 0;
			else if (terark_unlikely(((size_t)(-1) / _Count) < sizeof (T)))
				throw std::bad_alloc();

			// allocate storage for _Count elements of type T
		//	T* p = (T*)mpool_salloc(s_globalpool, _Count * sizeof(T));
			T* p = (T*)gsalloc(_Count * sizeof(T));
			if (terark_unlikely(NULL == p))
				throw std::bad_alloc();
			return p;
		}

		pointer allocate(size_type _Count, const void  *)
		{	// allocate array of _Count elements, ignore hint
			return (allocate(_Count));
		}

		void construct(pointer _Ptr, const T& _Val)
		{	// construct object at _Ptr with value _Val
			::new (_Ptr) T(_Val);
		}

		void destroy(pointer _Ptr)
		{	// destroy object at _Ptr
			_Ptr->~T();
		}

		size_t max_size() const throw()
		{	// estimate maximum array size
			size_t _Count = (size_t)(-1) / sizeof (T);
			return (0 < _Count ? _Count : 1);
		}

		static struct sallocator* get_vtab() { return (sallocator*)mpool_get_global(); }
	};
// can not ensure the order of construction, so do not use static data member
//	template<class T>
//	mpool* mpoolxx<T>::s_globalpool = mpool_get_global();

	// mpoolxx TEMPLATE OPERATORS
	template<class T,
		class Other> inline
			bool operator==(const mpoolxx<T>&, const mpoolxx<Other>&) throw()
			{	// test for mpoolxx equality (always true)
				return (true);
			}

	template<class T,
		class Other> inline
			bool operator!=(const mpoolxx<T>&, const mpoolxx<Other>&) throw()
			{	// test for mpoolxx inequality (always false)
				return (false);
			}
////////////////////////////////////////////////////////////////////////////////////////////////

	template<int Size>
	struct fixed_mpool_wrapper : sfixed_mpool
	{
		fixed_mpool_wrapper() {
			memset(this, 0, sizeof(*this));
			fmp.cell_size = Size;
			fmp.chunk_size = Size * 512;
			fmp.nChunks = 16;
			sfixed_mpool_init(this);
		}
		~fixed_mpool_wrapper() {
			sfixed_mpool_destroy(this);
		}

		void* allocate(size_t _Count) {
/*			if (_Count == 1) {
				T* p = (T*)fixed_mpool_alloc(&s_globalpool);
				if (NULL == p)
					throw std::bad_alloc();
				return p;
			} else if (_Count <= 0)
				return ::malloc(0);
			else if (((size_t)(-1) / _Count) < sizeof (T))
				throw std::bad_alloc();
			else {
			// allocate storage for _Count elements of type T
				T* p = (T*)::malloc(sizeof(T) * _Count);
				if (NULL == p)
					throw std::bad_alloc();
				return p;
			}
*/
			assert(1 == _Count);
			void* p = fixed_mpool_alloc(&fmp);
			if (terark_unlikely(NULL == p))
				throw std::bad_alloc();
			return p;
		}

		void deallocate(void* _Ptr, size_t _Count) {
/*			if (1 != _Count) {
				return ::free(_Ptr);
			} else
				fixed_mpool_free(&fmp, _Ptr);
*/
			assert(1 == _Count);
			fixed_mpool_free(&fmp, _Ptr);
		}

		static fixed_mpool_wrapper* get_fmp() {
			static fixed_mpool_wrapper theInstance;
			return &theInstance;
		}
		static struct sallocator* get_vtab() {
			return (struct sallocator*)get_fmp();
		}
	};

	template<class T> class fixed_mpoolxx
	{
	public:
		typedef T value_type;
		typedef T *pointer;
		typedef T &reference;
		typedef const T *const_pointer;
		typedef const T &const_reference;
		typedef size_t    size_type;
		typedef ptrdiff_t difference_type;

		template<class Other>
		struct rebind
		{	// convert an fixed_mpoolxx<T> to an fixed_mpoolxx <Other>
			typedef fixed_mpoolxx<Other> other;
		};

		pointer address(reference _Val) const { return (&_Val); }
		const_pointer address(const_reference _Val) const { return (&_Val); }

		fixed_mpoolxx() throw() {}

		fixed_mpoolxx(const fixed_mpoolxx<T>&) throw() {}

		template<class Other>
			fixed_mpoolxx(const fixed_mpoolxx<Other>&) throw()
			{	// construct from a related fixed_mpoolxx (do nothing)
			}

		template<class Other>
			fixed_mpoolxx<T>& operator=(const fixed_mpoolxx<Other>&)
			{	// assign from a related fixed_mpoolxx (do nothing)
				return (*this);
			}

		void deallocate(pointer _Ptr, size_type _Count) {
			fixed_mpool_wrapper<sizeof(T)>::get_fmp()->deallocate(_Ptr, _Count);
		}

		pointer allocate(size_type _Count) {
			return (T*)fixed_mpool_wrapper<sizeof(T)>::get_fmp()->allocate(_Count);
		}

		pointer allocate(size_type _Count, const void  *)
		{	// allocate array of _Count elements, ignore hint
			return (allocate(_Count));
		}

		void construct(pointer _Ptr, const T& _Val)
		{	// construct object at _Ptr with value _Val
			::new (_Ptr) T(_Val);
		}

		void destroy(pointer _Ptr) { _Ptr->~T(); }

		size_t max_size() const throw()
		{	// estimate maximum array size
			size_t _Count = (size_t)(-1) / sizeof (T);
			return (0 < _Count ? _Count : 1);
		}

		static struct sallocator* get_vtab() { return fixed_mpool_wrapper<sizeof(T)>::get_vtab(); }
	};

	// fixed_mpoolxx TEMPLATE OPERATORS
	template<class T,
		class Other> inline
			bool operator==(const fixed_mpoolxx<T>&, const fixed_mpoolxx<Other>&) throw()
			{	// test for fixed_mpoolxx equality (always true)
				return (true);
			}

	template<class T,
		class Other> inline
			bool operator!=(const fixed_mpoolxx<T>&, const fixed_mpoolxx<Other>&) throw()
			{	// test for fixed_mpoolxx inequality (always false)
				return (false);
			}
//////////////////////////////////////////////////////////////////////////////////////////////

	template<class Alloc, int ElemSize, bool IsAllocEmpty>
	class alloc_to_mpool_bridge_base : public Alloc
	{
		typedef typename Alloc::template rebind<unsigned char>::other byte_alloc;
		static  void* my_salloc(struct sallocator*  sa, size_t size) {
			return ((byte_alloc*)0)->allocate(size);
		}
		static void my_sfree(struct sallocator*  sa, void*  ptr, size_t size) {
			((byte_alloc*)0)->deallocate((unsigned char*)ptr, size);
		}
	public:
		struct sallocator* get_vtab() {
			static struct sallocator s_vtab = {
				&alloc_to_mpool_bridge_base::my_salloc,
				&alloc_to_mpool_bridge_base::my_sfree,
				&default_srealloc
			};
			return &s_vtab;
	   	}
	};

	template<class Alloc, int ElemSize>
	class alloc_to_mpool_bridge_base<Alloc, ElemSize, false> : public sallocator
	{
		typedef typename Alloc::template rebind<unsigned char>::other byte_alloc;
		typedef alloc_to_mpool_bridge_base<Alloc, ElemSize, false> self_t;
		byte_alloc al;
		static  void* my_salloc(struct sallocator*  sa, size_t size) {
			self_t* self = static_cast<self_t*>(sa);
			return self->al.allocate(size);
		}
		static void my_sfree(struct sallocator*  sa, void*  ptr, size_t size) {
			self_t* self = static_cast<self_t*>(sa);
			self->al.deallocate(ptr, size);
		}
	public:
		struct sallocator* get_vtab() { return this; }
		alloc_to_mpool_bridge_base() {
			this->salloc = &self_t::my_salloc;
			this->sfree  = &self_t::my_sfree;
			this->srealloc = &default_srealloc;
		}
	};

	template<class T>
	struct mpoolxx_test_is_empty_helper : T {
		long dummy[16];
	};
	template<class T>
	struct mpoolxx_test_is_empty {
		enum {value = sizeof(mpoolxx_test_is_empty_helper<T>) == 16*sizeof(long)};
	};

	template<class Alloc, int ElemSize>
	class alloc_to_mpool_bridge :
	   	public alloc_to_mpool_bridge_base<Alloc, ElemSize, mpoolxx_test_is_empty<Alloc>::value>
	{};

	template<class T, int ElemSize>
	class alloc_to_mpool_bridge<mpoolxx<T>, ElemSize>
	{
		struct MemBlock { unsigned char dummy[ElemSize]; };
	public:
		struct sallocator* get_vtab() { return mpoolxx<MemBlock>::get_vtab(); }
	};
	template<class T, int ElemSize>
	class alloc_to_mpool_bridge<fixed_mpoolxx<T>, ElemSize>
	{
	public:
		struct sallocator* get_vtab() { return fixed_mpool_wrapper<ElemSize>::get_vtab(); }
	};

} // name space terark

#endif // __terark_mpool_cxx_h__
