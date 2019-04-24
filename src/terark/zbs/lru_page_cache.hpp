#pragma once

#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <boost/noncopyable.hpp>

namespace terark {

class SingleLruReadonlyCache;
class  MultiLruReadonlyCache;
class TERARK_DLL_EXPORT LruReadonlyCache : public RefCounter {
public:
	class Buffer : private boost::noncopyable {
        friend class SingleLruReadonlyCache;
        friend class  MultiLruReadonlyCache;
		enum CacheType : unsigned char {
			hit,
			evicted_others,
			initial_free,
			dropped_free,
			hit_others_load,
			mix, // for multi page only
		};
        SingleLruReadonlyCache* owner;
        valvec<byte_t>*         rdbuf;
		uint32_t  index; // index == 0 indicate not ref any page
		CacheType cache_type;
	//	byte_t    reserved;
	//	uint16_t  missed_pages;
        void discard_impl();
    public:
        explicit
         Buffer(valvec<byte_t>* rb) : rdbuf(rb), index(0) { assert(rb); }
        ~Buffer() { discard(); }
        void discard() { if (index) discard_impl(); }
	};
	static LruReadonlyCache*
	create(size_t totalcapacityBytes, size_t shards, size_t maxFiles = 512);

	virtual const byte_t* pread(intptr_t fi, size_t offset, size_t len, Buffer*) = 0;
	virtual intptr_t open(intptr_t fd) = 0;
	virtual void close(intptr_t fi) = 0;
	virtual bool safe_close(intptr_t fi) = 0;
	virtual void print_stat_cnt(FILE*) const = 0;
};

TERARK_DLL_EXPORT
void fdpread(intptr_t fd, void* buf, size_t len, size_t offset);

} // namespace terark
