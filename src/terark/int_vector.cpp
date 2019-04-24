#include "int_vector.hpp"
#include <string>
#include "io/FileStream.hpp"
#include "io/StreamBuffer.hpp"
#include "io/DataOutput.hpp"
#include "fstring.hpp"

namespace terark {

UintVecMin0Base::Builder::~Builder() {
}

class UintVecMin0Builder : public UintVecMin0Base::Builder {
protected:
    boost::intrusive_ptr<OutputBuffer> m_writer;
    UintVecMin0Base m_buffer;
    size_t m_count;
    size_t m_flush_count;
    size_t m_output_size;

    static const size_t offset_flush_size = 128;
public:
    UintVecMin0Builder(size_t uintbits, OutputBuffer* buffer)
        : m_writer(buffer)
        , m_count(0)
        , m_flush_count(0)
        , m_output_size(0) {
        m_buffer.resize_with_uintbits(offset_flush_size, uintbits);
    }

    ~UintVecMin0Builder() {
    }

    void push_back(size_t value) override {
        m_buffer.set_wire(m_count++, value);
        if (m_count == offset_flush_size) {
            size_t byte_count = m_buffer.uintbits() * offset_flush_size / 8;
            m_writer->ensureWrite(m_buffer.data(), byte_count);
            m_flush_count += offset_flush_size;
            m_output_size += byte_count;
            m_count = 0;
        }
    }
    BuildResult finish() override {
        if (m_count > 0) {
            m_buffer.resize(m_count);
            m_buffer.shrink_to_fit();
            m_writer->ensureWrite(m_buffer.data(), m_buffer.mem_size());
            m_output_size += m_buffer.mem_size();
        }
        else {
            size_t remain_size = UintVecMin0Base::compute_mem_size(m_buffer.uintbits(), 0);
            m_output_size += remain_size;
            byte zero = 0;
            while (remain_size-- > 0) {
                m_writer->ensureWrite(&zero, 1);
            }
        }
        m_writer->flush_buffer();
        return {
            m_buffer.uintbits(),
            m_flush_count + m_count,
            m_output_size,
        };
    }
};

class UintVecMin0FileBuilder : public UintVecMin0Builder {
protected:
    FileStream m_file;
public:
    UintVecMin0FileBuilder(size_t uintbits, const char* fpath)
        : UintVecMin0Builder(uintbits, new OutputBuffer())
        , m_file(fpath, "wb+") {
        m_file.disbuf();
        UintVecMin0Builder::m_writer->attach(&m_file);
    }
    BuildResult finish() override {
        auto result = UintVecMin0Builder::finish();
        m_file.close();
        return result;
    }
    ~UintVecMin0FileBuilder() {
        UintVecMin0Builder::m_writer.reset();
    }
};


UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_uintbits(size_t uintbits, const char* fpath) {
    return new UintVecMin0FileBuilder(uintbits, fpath);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_max_value(size_t max_val, const char* fpath) {
    return new UintVecMin0FileBuilder(UintVecMin0::compute_uintbits(max_val), fpath);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_uintbits(size_t uintbits, OutputBuffer* buffer) {
    return new UintVecMin0Builder(uintbits, buffer);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_max_value(size_t max_val, OutputBuffer* buffer) {
    return new UintVecMin0Builder(UintVecMin0::compute_uintbits(max_val), buffer);
}

} // namespace terark

