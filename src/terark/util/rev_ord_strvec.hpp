#pragma once
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/int_vector.hpp>

namespace terark {

class TERARK_DLL_EXPORT FixedRevStrVec {
    size_t (*m_lower_bound_fixed)(const FixedRevStrVec*, size_t, size_t, const void*);
    size_t (*m_upper_bound_fixed)(const FixedRevStrVec*, size_t, size_t, const void*);
public:
    size_t m_fixlen;
    size_t m_size;
    MemType m_strpool_mem_type;
    valvec<byte_t> m_strpool;

    explicit FixedRevStrVec(size_t fixlen = 0);
    ~FixedRevStrVec();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();
    void risk_release_ownership();

    double avg_size() const { return m_fixlen; }
    size_t mem_cap () const { return m_strpool.capacity(); }
    size_t mem_size() const { return m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    size_t size() const { return m_size; }
    fstring operator[](size_t idx) const {
        assert(idx < m_size);
        assert(m_fixlen * m_size == m_strpool.size());
        size_t fixlen = m_fixlen;
        size_t offset = fixlen * (m_size - 1 - idx);
        return fstring(m_strpool.data() + offset, fixlen);
    }
    const byte_t* data() const { return m_strpool.data(); }
    byte_t* data() { return m_strpool.data(); }
    byte_t* mutable_nth_data(size_t idx) {
        TERARK_ASSERT_LT(idx, m_size, "%zd %zd");
        idx = m_size - 1 - idx;
        return m_strpool.data() + m_fixlen * idx;
    }
    const byte_t* nth_data(size_t idx) const {
        TERARK_ASSERT_LT(idx, m_size, "%zd %zd");
        idx = m_size - 1 - idx;
        return m_strpool.data() + m_fixlen * idx;
    }
    size_t  nth_size(size_t /*idx*/) const { return m_fixlen; }
    size_t  nth_offset(size_t idx) const {
        TERARK_ASSERT_LT(idx, m_size, "%zd %zd");
        idx = m_size - 1 - idx;
        return m_fixlen * idx;
    }
    size_t  nth_seq_id(size_t idx) const { return m_size - 1 - idx; }
    size_t  nth_endpos(size_t idx) const {
        TERARK_ASSERT_LT(idx, m_size, "%zd %zd");
        idx = m_size - 1 - idx;
        return m_fixlen * (idx + 1);
    }
    fstring back() const { return (*this)[m_size-1]; }
    void swap(FixedRevStrVec&);
    void push_back(fstring str);
    void pop_back();
    void reverse_keys();
    void sort();
    static void sort_raw(void* base, size_t num, size_t fixlen);
    void clear();
    void optimize_func(); // optimize (lower|upper)_bound_fixed
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring k) const { return lower_bound(0, m_size, k); }
    size_t upper_bound(fstring k) const { return upper_bound(0, m_size, k); }
    size_t lower_bound(size_t lo, size_t hi, fstring) const;
    size_t upper_bound(size_t lo, size_t hi, fstring) const;
    ///@{ user should ensure k len is same as m_fixlen
    size_t lower_bound_fixed(const void* k) const { return m_lower_bound_fixed(this, 0, m_size, k); }
    size_t upper_bound_fixed(const void* k) const { return m_upper_bound_fixed(this, 0, m_size, k); }
    size_t lower_bound_fixed(size_t lo, size_t hi, const void* k) const { return m_lower_bound_fixed(this, lo, hi, k); }
    size_t upper_bound_fixed(size_t lo, size_t hi, const void* k) const { return m_upper_bound_fixed(this, lo, hi, k); }
    ///@}
    size_t max_strlen() const { return m_fixlen; }
};

// object layout is compatible to SortedStrVec
class TERARK_DLL_EXPORT RevOrdStrVec {
public:
    UintVecMin0    m_offsets;
    valvec<byte_t> m_strpool;
    MemType m_offsets_mem_type;
    MemType m_strpool_mem_type;

    explicit RevOrdStrVec();
    ~RevOrdStrVec();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();

    double avg_size() const { return m_strpool.size() / double(m_offsets.size()-1); }
    size_t mem_cap () const { return m_offsets.mem_size() + m_strpool.capacity(); }
    size_t mem_size() const { return m_offsets.mem_size() + m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    inline size_t size() const {
        assert(m_offsets.size() >= 1);
        return m_offsets.size()-1;
    }
    inline fstring operator[](size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return fstring(m_strpool.data() + BegEnd[0], BegEnd[1] - BegEnd[0]);
    }
    inline byte_t* mutable_nth_data(size_t idx) {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        return m_strpool.data() + m_offsets[idx];
    }
    inline const byte_t* nth_data(size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        return m_strpool.data() + m_offsets[idx];
    }
    inline size_t nth_size(size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return BegEnd[1] - BegEnd[0];
    }
    inline size_t nth_offset(size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        return m_offsets[idx];
    }
    inline size_t nth_seq_id(size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        return idx;
    }
    inline size_t nth_endpos(size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        //idx = size() - idx - 1;
        idx = m_offsets.size() - 2;
        return m_offsets[idx+1];
    }
//  fstring back() const { return (*this)[m_offsets.size()-1]; }
    void swap(RevOrdStrVec&);
    void push_back(fstring str);
    void pop_back();
    void back_append(fstring str);
    void back_shrink(size_t nShrink);
    void back_grow_no_init(size_t nGrow);
    void reverse_keys();
    void sort();
    void clear();
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring k) const { return lower_bound(0, size(), k); }
    size_t upper_bound(fstring k) const { return upper_bound(0, size(), k); }
    size_t lower_bound(size_t start, size_t end, fstring) const;
    size_t upper_bound(size_t start, size_t end, fstring) const;
    size_t max_strlen() const;
};

// object layout is compatible to SortedStrVecUintTpl
template<class UintXX>
class TERARK_DLL_EXPORT RevOrdStrVecUintTpl {
public:
    valvec<UintXX> m_offsets;
    valvec<byte_t> m_strpool;
    uint32_t       m_delim_len;
    MemType m_offsets_mem_type;
    MemType m_strpool_mem_type;

    explicit RevOrdStrVecUintTpl(size_t delim_len = 0);
    ~RevOrdStrVecUintTpl();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();

    double avg_size() const { return m_strpool.size() / double(m_offsets.size()-1) - m_delim_len; }
    size_t mem_cap () const { return m_offsets.full_mem_size() + m_strpool.capacity(); }
    size_t mem_size() const { return m_offsets.full_mem_size() + m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    size_t size() const {
        assert(m_offsets.size() >= 1);
        return m_offsets.size()-1;
    }
    fstring operator[](size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        size_t Beg = m_offsets[idx+0];
        size_t End = m_offsets[idx+1];
        return fstring(m_strpool.data() + Beg, End - Beg - m_delim_len);
    }
    byte_t* mutable_nth_data(size_t idx) {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        return m_strpool.data() + m_offsets[idx];
    }
    const byte_t* nth_data(size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        return m_strpool.data() + m_offsets[idx];
    }
    size_t  nth_size(size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        size_t Beg = m_offsets[idx+0];
        size_t End = m_offsets[idx+1];
        return End - Beg - m_delim_len;
    }
    size_t  nth_offset(size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        return m_offsets[idx];
    }
    size_t  nth_seq_id(size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        return idx;
    }
    size_t  nth_endpos(size_t idx) const {
        assert(idx+1 < m_offsets.size());
        idx = m_offsets.size() - 2 - idx;
        return m_offsets[idx+1] - m_delim_len;
    }
    fstring back() const { return (*this)[m_offsets.size()-1]; }
    void swap(RevOrdStrVecUintTpl&);
    void push_back(fstring str);
    void pop_back();
	void back_grow_no_init(size_t nGrow);
    void reverse_keys();
    void sort();
    void clear();
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring k) const { return lower_bound(0, size(), k); }
    size_t upper_bound(fstring k) const { return upper_bound(0, size(), k); }
    size_t lower_bound(size_t start, size_t end, fstring) const;
    size_t upper_bound(size_t start, size_t end, fstring) const;
    size_t max_strlen() const;
};

using VoRevOrdStrVec = RevOrdStrVec                 ; // Vo : VarWidth offset
using DoRevOrdStrVec = RevOrdStrVecUintTpl<uint32_t>; // Do : DWORD    offset
using QoRevOrdStrVec = RevOrdStrVecUintTpl<uint64_t>; // Qo : QWORD    offset

} // namespace terark
