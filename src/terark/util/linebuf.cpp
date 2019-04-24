#include "linebuf.hpp"
#include "throw.hpp"
#include "autoclose.hpp"

#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <stdexcept>

#ifdef _MSC_VER
#include <io.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

#include <terark/util/stat.hpp>
#include <terark/valvec.hpp>

namespace terark {

LineBuf::LineBuf()
   	: capacity(0), n(0), p(NULL)
{}

LineBuf::~LineBuf() {
   	if (p)
	   	free(p);
}

void LineBuf::clear() {
	if (p) {
		free(p);
		p = NULL;
		n = capacity = 0;
	}
	else {
		assert(0 == n);
		assert(0 == capacity);
	}
}

ptrdiff_t LineBuf::getline(FILE* f) {
	assert(NULL != f);
#if defined(__USE_GNU) || defined(__CYGWIN__) || defined(__CYGWIN32__)
       	// has ::getline
	return n = ::getline(&p, &capacity, f);
#else
//	#error only _GNU_SOURCE is supported
	if (NULL == p) {
		capacity = BUFSIZ;
		p = (char*)malloc(BUFSIZ);
		if (NULL == p)
		   	THROW_STD(runtime_error, "malloc(BUFSIZ=%d) failed", BUFSIZ);
	}
	n = 0;
	p[0] = '\0';
	for (;;) {
		assert(n < capacity);
		char*  ret = ::fgets(p + n, capacity - n, f);
		size_t len = ::strlen(p + n);
		if (0 == len && (feof(f) || ferror(f)))
			return -1;
		n += len;
		if (ret) {
			if (capacity-1 == n && p[n-1] != '\n') {
				size_t newcap = capacity * 2;
				ret = (char*)realloc(p, newcap);
				if (NULL == ret)
					THROW_STD(runtime_error, "realloc(newcap=%zd)", newcap);
				p = ret;
				capacity = newcap;
			}
			else {
				return ptrdiff_t(n);
			}
		}
		else if (feof(f))
			return ptrdiff_t(n);
		else
			return -1;
	}
#endif
}
#ifdef _MSC_VER
  #define fread_unlocked _fread_nolock
#endif
#ifdef __APPLE__
  #define fread_unlocked fread
#endif

ptrdiff_t LineBuf::getbson(FILE* f) {
    uint32_t len;
	size_t nl = fread_unlocked(&len, 1, 4, f);
    if (0 == nl) {
        return -1;
    }
    if (4 != nl) {
        THROW_STD(runtime_error, "fread_nolock(4) = %zd", nl);
    }
    if (len < 4) { // len includes length of 'len' self
        THROW_STD(runtime_error, "len = %zd, must >= 4", size_t(len));
    }
    len -= 4;
    if (capacity < len + 1) {
        size_t newcap = (len+1)*103/64; // 103/64 = 1.609375 <~ 1.618
        char* q = (char*)realloc(p, newcap);
        if (NULL == q)
            THROW_STD(runtime_error, "realloc(newcap=%zd)", newcap);
        capacity = newcap;
        p = q;
    }
    n = nl = fread_unlocked(p, 1, len, f);
    if (nl != size_t(len))
        THROW_STD(runtime_error, "fread_nolock(%zd) = %zd", size_t(len), nl);
    p[nl] = '\0';
    return nl;
}

size_t LineBuf::trim() {
	assert(NULL != p);
	size_t n0 = n;
	while (n > 0 && isspace((unsigned char)p[n-1])) p[--n] = 0;
	return n0 - n;
}

size_t LineBuf::chomp() {
	assert(NULL != p);
	size_t n0 = n;
	while (n > 0 && strchr("\r\n", p[n-1])) p[--n] = 0;
	return n0 - n;
}

void LineBuf::push_back_slow_path(char ch) {
	assert(n + 1 == capacity || 0 == capacity);
	// 13/8 = 1.625 ~ 1.618
	size_t newcap = std::max<size_t>(32, align_up(capacity*13/8, 16));
	char* q = (char*)realloc(p, capacity);
	if (NULL == q) {
		throw std::bad_alloc();
	}
	capacity = newcap;
	p = q;
	p[n++] = ch;
	p[n] = '\0';
}

bool LineBuf::read_binary_tuple(int32_t* offsets, size_t arity, FILE* f) {
	assert(NULL != offsets);
	offsets[0] = 0;
	size_t n_read = fread(offsets+1, 1, sizeof(int32_t) * arity, f);
	if (n_read != sizeof(int32_t) * arity) {
		return false;
	}
	for (size_t i = 0; i < arity; ++i) {
		assert(offsets[i+1] >= 0);
		offsets[i+1] += offsets[i];
	}
	size_t len = offsets[arity];
	if (this->capacity < len) {
		char* q = (char*)realloc(this->p, len);
		if (NULL == q) {
			THROW_STD(invalid_argument
				, "Out of memory when reading record[size=%zd(0x%zX)]"
				, len, len
				);
		}
		this->p = q;
		this->capacity = len;
	}
	n_read = fread(this->p, 1, len, f);
	if (n_read != len) {
		THROW_STD(invalid_argument
			, "fread record data failed: request=%zd, readed=%zd\n"
			, len, n_read
			);
	}
	this->n = len;
	return true; // len can be 0
}

LineBuf& LineBuf::read_all(FILE* fp, size_t align) {
	int fd = fileno(fp);
	struct ll_stat st;
	if (::ll_fstat(fd, &st) < 0) {
		THROW_STD(runtime_error, "fstat failed");
	}
    if (!S_ISREG(st.st_mode)) {
        valvec<char> vec;
        while (this->getline(fp) >= 0) {
            vec.append(p, n);
        }
        vec.grow_capacity(1)[0] = '\0';
        free(p);
        p = vec.data();
        n = vec.size();
        capacity = vec.capacity();
	//	fprintf(stderr, "LineBuf.read_all().n = %zd, p = %s\n", n, p);
        vec.risk_release_ownership();
        return *this;
    }
	if (p) free(p);
	size_t cap;
	if (align) {
		if (align & (align-1)) {
			THROW_STD(invalid_argument
				, "invalid(align = %zd), must be power of 2", align);
		}
		cap = align_up(st.st_size + 1, align);
	#if defined(_MSC_VER)
		p = (char*)_aligned_malloc(cap, align);
	#else
		int err = posix_memalign((void**)&p, align, cap);
		if (err) {
            p = NULL;
			THROW_STD(invalid_argument
				, "posix_memalign(align=%zd, size=%lld) = %s"
				, align, llong(st.st_size), strerror(err));
		}
	  #if defined(MADV_HUGEPAGE)
        if (align >= (2 << 20)) {
            if (madvise(p, cap, MADV_HUGEPAGE) < 0) {
                fprintf(stderr
                    , "WARN: LineBuf::read_all: madvise(ptr=%p, len=%zdM, HUGEPAGE) = %s\n"
                    , p, cap>>20, strerror(errno));
            }
        }
	  #endif
	#endif
	}
	else {
		cap = st.st_size + 1;
		p = (char*)malloc(cap);
	}
	if (NULL == p) {
		n = 0;
		capacity = 0;
		THROW_STD(invalid_argument,
			"file too large(size=%lld)", llong(st.st_size));
	}
	capacity = cap;
	n = fread(p, 1, st.st_size, fp);
	p[n] = '\0';
	return *this;
}

LineBuf& LineBuf::read_all(fstring fname, size_t align) {
	Auto_fclose f(fopen(fname.c_str(), "r"));
	if (!f) {
		THROW_STD(invalid_argument,
			"ERROR: fopen(%.*s, r) = %s",
			fname.ilen(), fname.data(), strerror(errno));
	}
	read_all(f, align);
	return *this;
}

} // namespace terark

