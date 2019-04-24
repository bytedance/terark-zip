#ifndef __terark_util_deepcopy_ptr_hpp__
#define __terark_util_deepcopy_ptr_hpp__

#include <boost/static_assert.hpp>
#include <assert.h>
#include <stdlib.h>
#include <stdexcept>

namespace terark {

	template<class T>
	class DeepCopyPtr {
	public:
		T* p;

		T* get() const { return p; }

		~DeepCopyPtr() {
			delete p;
		}
		DeepCopyPtr() : p(NULL) {}
		explicit DeepCopyPtr(T* q) {
			if (q) {
				p = new T(*q);
			} else {
				p = NULL;
			}
		}
		DeepCopyPtr(const DeepCopyPtr& q) {
			if (q.p) {
				p = new T(*q.p);
			} else {
				p = NULL;
			}
		}
		DeepCopyPtr& operator=(const DeepCopyPtr& q) {
			DeepCopyPtr(q).swap(*this);
			return *this;
		}
		DeepCopyPtr(DeepCopyPtr&& q) {
			p = q.p;
			q.p = NULL;
		}
		DeepCopyPtr& operator=(DeepCopyPtr&& q) {
			p = q.p;
			q.p = NULL;
		}
		DeepCopyPtr& operator=(T* q) {
			DeepCopyPtr(q).swap(*this);
			return *this;
		}

		T* release_and_set(T* newptr) {
			T* oldptr = p;
			p = newptr;
			return oldptr;
		}
		T* release() {
			T* q = p;
			p = NULL;
			return q;
		}

		void reset(T* q) {
			DeepCopyPtr(q).swap(*this);
		}

		void swap(DeepCopyPtr& y) { T* tmp = p; p = y.p; y.p = tmp; }

		operator T*  () const { return  p; }
		T* operator->() const { return  p; } // ? direct, simple and stupid ?
		T& operator* () const { return *p; } // ? direct, simple and stupid ?
	};

} // namespace terark

namespace std {
	template<class T>
	void swap(terark::DeepCopyPtr<T>& x, terark::DeepCopyPtr<T>& y) { x.swap(y); }
}

#endif


