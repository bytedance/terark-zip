#pragma once
#ifndef __terark_io_FileMemIO__
#define __terark_io_FileMemIO__

#include <stddef.h>
#include <string>
#include <exception>
#include <terark/stdtypes.hpp>
#include <boost/static_assert.hpp>
#include <terark/util/throw.hpp>
//#include <boost/enable_shared_from_this.hpp>
#include <boost/core/enable_if.hpp>
#include "MemStream.hpp"

namespace terark {

class FileMemIO : protected AutoGrownMemIO {
    using Base = AutoGrownMemIO;
    size_t m_len;

public:
    FileMemIO() { m_len = 0; }
    void setRangeLen(size_t len) { m_len = len; }
    bool eof() const { return Base::tell() == m_len; }
    int  getByte() {
        if (terark_unlikely(Base::tell() == m_len))
            return -1;
        return Base::uncheckedReadByte();
    }
    byte readByte() {
        assert(Base::tell() < m_len);
        return Base::readByte();
    }
    void writeByte(byte b) {
        Base::writeByte(b);
        m_len = std::max(m_len, Base::tell());
    }

    template<class ByteArray>
    void readAll(ByteArray& ba) {
        BOOST_STATIC_ASSERT(sizeof(ba[0]) == 1);
        assert(Base::tell() <= m_len);
        size_t n = m_len - Base::tell();
        ba.resize(n);
        if (n) {
            // must be a continuous memory block
            assert(&*ba.begin() + m_len == &*(ba.end() - 1) + 1);
            Base::ensureRead(&*ba.begin(), ba.size());
        }
    }

    void ensureRead(void* vbuf, size_t length) {
        if (terark_likely(Base::tell() + length < Base::size())) {
            Base::ensureRead(vbuf, length);
        }
        else
            throw_EndOfFile(BOOST_CURRENT_FUNCTION, length);
    }
    void ensureWrite(const void* data, size_t length) {
        Base::ensureWrite(data, length);
        m_len = std::max(m_len, Base::tell());
    }

    size_t read(void* vbuf, size_t length) {
        assert(Base::tell() <= m_len);
        size_t n = m_len - Base::tell();
        if (terark_unlikely(n < length)) {
            Base::ensureRead(vbuf, n);
            return n;
        }
        else {
            Base::ensureRead(vbuf, length);
            return length;
        }
    }
    size_t write(const void* data, size_t length) {
        size_t ret = Base::write(data, length);
        m_len = std::max(m_len, Base::tell());
        return ret;
    }

    byte* skip(ptrdiff_t diff) {
        ptrdiff_t n = ptrdiff_t(m_len) - ptrdiff_t(Base::tell());
        if (terark_unlikely(diff > n)) {
            THROW_STD(out_of_range
                , "diff=%ld, end-pos=%ld"
                , long(diff), long(n));
        }
        return Base::skip(diff);
    }

    void seek(ptrdiff_t newPos) {
        if (newPos < 0 || newPos > ptrdiff_t(m_len)) {
            THROW_STD(out_of_range
                , "seek offset=%ld, invalid"
                , long(newPos));
        }
        Base::seek(newPos);
    }
    void seek(ptrdiff_t offset, int origin) {
        ptrdiff_t pos;
        switch (origin) {
        case 0: pos = offset; break;
        case 1: pos = Base::tell(); break;
        case 2: pos = ptrdiff_t(m_len) + offset; break;
        default: pos = -1; break;
        }
        if (pos < 0 || pos > ptrdiff_t(m_len)) {
            THROW_STD(out_of_range
                , "seek offset=%ld, origin=%ld invalid"
                , long(offset), long(origin));
        }
        Base::seek(pos);
    }

    size_t remain() const noexcept { return m_len - Base::tell(); }
    using Base::tell;
    size_t size() const noexcept { return m_len; }
    using Base::begin;
    byte* end() const noexcept { return Base::begin() + m_len; }
    using Base::flush;

    void read_string(std::string& s) {
        size_t len = TERARK_IF_WORD_BITS_64(read_var_uint64, read_var_uint32)();
        s.resize(len);
        if (len)
            this->ensureRead(&*s.begin(), len);
    }

    size_t pread(stream_position_t pos, void* vbuf, size_t length) {
        if (terark_unlikely(Base::tell() + length > m_len)) {
            if (pos >= m_len) return 0;
            length = Base::tell() - pos;
        }
        memcpy(vbuf, Base::begin() + pos, length);
        return length;
    }
    size_t pwrite(stream_position_t pos, const void* data, size_t length) noexcept {
        if (terark_unlikely(Base::tell() + length > m_len)) {
            resize(std::max<size_t>(pos + length, 64u));
            if (pos > m_len) memset(Base::begin() + m_len, 0, pos - m_len);
        }
        memcpy(Base::begin() + pos, data, length);
        return length;
    }

    void resize(size_t size) {
        if (size > Base::size())
            Base::resize(size);
        if (size > m_len) memset(Base::begin() + m_len, 0, size - m_len);
        m_len = size;
    }

    template<class InputStream>
    void from_input(InputStream& input, size_t length) {
        Base::from_input(input, length);
        m_len = std::max(m_len, Base::tell());
    }
    template<class OutputStream>
    void to_output(OutputStream& output, size_t length) {
        assert(Base::tell() <= m_len);
        size_t n = m_len - Base::tell();
        output.ensureWrite(skip(n), n);
    }

    void clear() {
        Base::clear();
        m_len = 0;
    }
    void swap(FileMemIO& that) {
        Base::swap(that);
        std::swap(m_len, that.m_len);
    }
    void shrink_to_fit() {
        size_t pos = Base::tell();
        Base::seek(m_len);
        Base::shrink_to_fit();
        Base::seek(pos);
    }
};

} // namespace terark

#endif // __terark_io_stream_range_hpp__
