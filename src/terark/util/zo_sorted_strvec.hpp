#pragma once
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/util/sorted_uint_vec.hpp>

namespace terark {

class TERARK_DLL_EXPORT ZoSortedStrVec {
public:
    class TERARK_DLL_EXPORT Builder {
        class Impl; Impl* impl;
    public:
        ~Builder();
        Builder();
        explicit Builder(size_t blockUnits);
        operator Impl*() const { return impl; }
        void init(size_t blockUnits = 128);
        void reserve_strpool(size_t cap);
        void push_back(fstring str);
        void finish(ZoSortedStrVec* strVec);
    };
    SortedUintVec  m_offsets;
    valvec<byte_t> m_strpool;

    explicit ZoSortedStrVec();
    ~ZoSortedStrVec();

    double avg_size() const { return m_strpool.size() / double(m_offsets.size()-1); }
    size_t mem_cap () const { return m_offsets.mem_size() + m_strpool.capacity(); }
    size_t mem_size() const { return m_offsets.mem_size() + m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    size_t size() const {
        assert(m_offsets.size() >= 1);
        return m_offsets.size()-1;
    }
    fstring operator[](size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return fstring(m_strpool.data() + BegEnd[0], BegEnd[1] - BegEnd[0]);
    }
    byte_t* mutable_nth_data(size_t idx) { return m_strpool.data() + m_offsets[idx]; }
    const
    byte_t* nth_data(size_t idx) const { return m_strpool.data() + m_offsets[idx]; }
    size_t  nth_size(size_t idx) const {
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return BegEnd[1] - BegEnd[0];
    }
    size_t  nth_offset(size_t idx) const { return m_offsets[idx]; }
    size_t  nth_seq_id(size_t idx) const { return idx; }
    size_t  nth_endpos(size_t idx) const {
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return BegEnd[1];
    }
    fstring back() const { return (*this)[m_offsets.size()-1]; }
    void swap(ZoSortedStrVec&);
    void sort();
    void clear();
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring) const;
    size_t upper_bound(fstring) const;
    size_t max_strlen() const;
};

class TERARK_DLL_EXPORT ZoSortedStrVecWithBuilder : public ZoSortedStrVec {
    ZoSortedStrVec::Builder m_builder;
public:
    bool has_builder() const { return NULL != m_builder; }
    void init(size_t blockUnits);
    void reserve(size_t strNum, size_t maxStrPool);
    void finish();
    void push_back(fstring);
};

} // namespace terark
