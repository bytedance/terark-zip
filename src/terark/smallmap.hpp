#ifndef __terark_smallmap_hpp__
#define __terark_smallmap_hpp__

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <memory>
#include <stdexcept>
#include <terark/valvec.hpp>

namespace terark {

template<class Mapped>
class smallmap {
public:
    explicit smallmap(size_t indexSize) {
        index = (short*)malloc(sizeof(short) * indexSize);
        if (NULL == index) throw std::bad_alloc();
        p = (Mapped*)malloc(sizeof(Mapped) * indexSize);
        if (NULL == p) {
			free(index);
			throw std::bad_alloc();
		}
	//	std::uninitialized_fill_n(p, indexSize, Mapped());
        for (size_t i = 0; i < indexSize; ++i)
            new(p+i)Mapped();
        n = 0;
        c = indexSize;
        memset(index, -1, sizeof(short) * c);
    }
    ~smallmap() {
        STDEXT_destroy_range(p, p + c);
        free(p);
        free(index);
    }
    Mapped& bykey(size_t key) {
        assert(key < c);
        assert(n <= c);
        if (-1 == index[key]) {
            assert(n < c);
            index[key] = (short)n;
		//	assert(isprint(key) || isspace(key));
            p[n].ch = key;
            return p[n++];
        }
        return p[index[key]];
    }
    void resize0() {
		if (n <= 16) {
			for (size_t i = 0; i < n; ++i) {
				Mapped& v = p[i];
				assert(-1 != v.ch);
				assert(-1 != index[v.ch]);
				index[v.ch] = -1;
				v.resize0();
			}
		} else {
			memset(index, -1, sizeof(short) * c);
			for (size_t i = 0; i < n; ++i)
				p[i].resize0();
		}
        n = 0;
    }
    bool exists(size_t key) const {
        assert(key < c);
        return -1 != index[key];
    }
	Mapped& byidx(size_t idx) {
		assert(idx < n);
		return p[idx];
	}
    Mapped* begin() { return p; }
    Mapped* end()   { return p + n; }
    size_t size() const { return n; }
private:
    short*  index;
    Mapped* p;
    size_t  n, c;
};

} // namespace terark

#endif // __terark_smallmap_hpp__


