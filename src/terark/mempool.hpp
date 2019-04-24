#pragma once
#include "valvec.hpp"
#include <terark/util/atomic.hpp>
#include <terark/util/throw.hpp>
#include <terark/thread/instance_tls_owner.hpp>
#include <stdexcept>
#include <boost/integer/static_log2.hpp>
#include <boost/mpl/if.hpp>
#include <mutex>
#include "mempool_lock_free.hpp"
#include "mempool_lock_none.hpp"
#include "mempool_fixed_cap.hpp"
#include "mempool_lock_mutex.hpp"
#include "mempool_thread_cache.hpp"

namespace terark {

template<int AlignSize>
class MemPool_CompileX : protected valvec<unsigned char> {
protected:
    size_t  fragment_size; // for compatible with MemPool_Lock(Free|None|Mutex)
    typedef valvec<unsigned char> mem;
public:
    using mem::data;
    using mem::size; // bring to public...
    using mem::reserve;
    using mem::capacity;
    using mem::risk_set_data;
    using mem::risk_set_size;
    using mem::risk_set_capacity;
    using mem::risk_release_ownership;
    size_t frag_size() const { return fragment_size; }
//  void   sfree(size_t,size_t) { assert(false); }
//  size_t alloc(size_t) { assert(false); return 0; }
//  size_t alloc3(size_t,size_t,size_t) { assert(false); return 0; }
    valvec<unsigned char>* get_valvec() { return this; }
};

} // namespace terark

