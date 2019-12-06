#pragma once

#pragma error Never Try to add such a file StdvecWriter

#include <stddef.h> // for size_t

namespace terark {

template<class Stdvec>
class StdvecWriter : public Stdvec {
public:
    typedef typename Stdvec::value_type value_type;
    static_assert(sizeof(value_type) == 1, "value_type must be 1 byte");

    using Stdvec::Stdvec;
    void ensureWrite(const void* buf, size_t len) {
        this->insert(this->end(), (const value_type*)(buf), len);
    }

    void writeByte(unsigned char b) {
        this->push_back(b);
    }
};

} // namespace terark
