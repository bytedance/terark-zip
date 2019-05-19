#ifndef __terark_automata_dfa_interface_hpp__
#define __terark_automata_dfa_interface_hpp__

#include <stdio.h>
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/function.hpp>
#include <boost/noncopyable.hpp>



namespace terark {

typedef uint16_t       auchar_t;
const size_t initial_state = 0;
const size_t state_not_found = size_t(-1);

struct DFA_MmapHeader; // forward declaration
class febitvec;
class SortableStrVec;

//#pragma pack(push,1)
template<class Uint, int UintSize>
struct CharTargetBase {
	enum { StateBits = sizeof(Uint)*8 };
	static const Uint nil = Uint(-1);
	auchar_t ch;
	Uint  target;
};
template<class Uint>
struct CharTargetBase<Uint, 8> {
	BOOST_STATIC_ASSERT(sizeof(Uint)==8);
	enum { StateBits = 55 };
	static const Uint nil = (Uint(1) << 55) - 1;
	Uint ch     :  9;
	Uint target : 55; // MAX_STATE_BITS
};
template<class StateID>
struct CharTarget : public CharTargetBase<StateID, sizeof(StateID)> {
	CharTarget(auchar_t c, StateID t) {
		this->ch = c;
		this->target = t;
	}
	CharTarget() {
		this->ch = auchar_t(511);
		this->target = this->nil;
	}
};
//#pragma pack(pop)
BOOST_STATIC_ASSERT(sizeof(CharTarget<size_t>)==8 || sizeof(size_t)==4);

template<class StateID>
inline
bool operator<(CharTarget<StateID> x, CharTarget<StateID> y) {
	if (x.ch != y.ch)
		return x.ch < y.ch;
	else
		return x.target < y.target;
}
template<class StateID>
inline
bool operator==(CharTarget<StateID> x, CharTarget<StateID> y) {
	return x.ch == y.ch && x.target == y.target;
}
template<class StateID>
inline
bool operator!=(CharTarget<StateID> x, CharTarget<StateID> y) {
	return x.ch != y.ch || x.target != y.target;
}

class ByteInputRange { // or named ByteInputStream ?
public:
	virtual ~ByteInputRange();
	virtual void operator++() = 0; // strict, don't return *this
	virtual byte_t operator*() = 0; // non-const
	virtual bool empty() const = 0;
};
typedef function<byte_t(byte_t)> ByteTR;

class TERARK_DLL_EXPORT BaseDFA; // forward declaration
class TERARK_DLL_EXPORT BaseDAWG;
class TERARK_DLL_EXPORT SuffixCountableDAWG;
class TERARK_DLL_EXPORT BaseAC;

struct TERARK_DLL_EXPORT ComplexMatchContext : private boost::noncopyable {
	virtual ~ComplexMatchContext();
	virtual ComplexMatchContext* clone() const = 0;
};

struct TERARK_DLL_EXPORT MatchContextBase {
	size_t pos; // matching pos of fstring, or say it "matched len"
	size_t root;
	size_t zidx;
	MatchContextBase() {
		pos = 0;
		root = 0; // initial_state;
		zidx = 0;
	}
	MatchContextBase(size_t pos1, size_t root1, size_t zidx1) {
		pos = pos1;
		root = root1;
		zidx = zidx1;
	}
};
struct TERARK_DLL_EXPORT MatchContext : MatchContextBase {
	valvec<byte_t> zbuf;
	ComplexMatchContext* complex_context; // normally NULL

	void swap(MatchContext& y) {
		std::swap(pos , y.pos );
		std::swap(root, y.root);
		std::swap(zidx, y.zidx);
		zbuf.swap(y.zbuf);
		std::swap(complex_context, y.complex_context);
	}

	~MatchContext();
	MatchContext() {
		complex_context = NULL;
	}
	MatchContext(size_t pos1, size_t root1, size_t zidx1) {
		pos = pos1;
		root = root1;
		zidx = zidx1;
		complex_context = NULL;
	}
	MatchContext(const MatchContext& y);
	MatchContext& operator=(const MatchContext& y);
	void reset();
	void reset(size_t pos1, size_t root1, size_t zidx1);
};

template<class CharT>
class TERARK_DLL_EXPORT ADFA_LexIteratorT : boost::noncopyable {
protected:
    typedef typename terark_get_uchar_type<CharT>::type uch_t;
	const BaseDFA* m_dfa;
	valvec<uch_t>  m_word;
	size_t m_curr;

public:
	typedef basic_fstring<CharT> fstr;
	ADFA_LexIteratorT(const BaseDFA* dfa);
	ADFA_LexIteratorT(valvec_no_init);
	virtual ~ADFA_LexIteratorT();
	virtual void reset(const BaseDFA* dfa, size_t root = 0) = 0;

	virtual bool incr() = 0;
	virtual bool decr() = 0;

	virtual bool seek_begin();
	virtual bool seek_end() = 0;
	virtual bool seek_lower_bound(fstr) = 0;
	bool seek_rev_lower_bound(fstr); // convenient function

	virtual size_t seek_max_prefix(fstr) = 0;

	const BaseDFA* get_dfa() const { return m_dfa; }
	fstr word() const { return fstr(m_word.data(), m_word.size()); }
	size_t word_state() const { return m_curr; }

	// for user add app data after m_word.size() and before m_word.capacity()
	// user should not add more than 16 bytes app data
	valvec<uch_t>& mutable_word() { return m_word; }
};
typedef ADFA_LexIteratorT<char    >  ADFA_LexIterator;
typedef ADFA_LexIteratorT<uint16_t>  ADFA_LexIterator16;

template<class CharT>
class TERARK_DLL_EXPORT ADFA_LexIteratorData :
                 public ADFA_LexIteratorT<CharT> {
protected:
	struct Layer {
		size_t parent;
		short  iter;
		short  size;
		int    zlen;
	};
	MatchContext   m_ctx;
	size_t         m_root;
	valvec<Layer>  m_iter;
	valvec<CharTarget<size_t> >  m_node;
	bool m_isForward;
	ADFA_LexIteratorData(const BaseDFA* dfa);
	ADFA_LexIteratorData(valvec_no_init);
	~ADFA_LexIteratorData();
};

TERARK_DLL_EXPORT BaseDFA* BaseDFA_load(fstring fname);
TERARK_DLL_EXPORT BaseDFA* BaseDFA_load(FILE*);

class TERARK_DLL_EXPORT BaseDFA { // readonly dfa interface
protected:
	BaseDFA(const BaseDFA&);
	BaseDFA& operator=(const BaseDFA&);

	virtual void finish_load_mmap(const DFA_MmapHeader*);
	virtual long prepare_save_mmap(DFA_MmapHeader*, const void**) const;
	static BaseDFA* load_mmap_fmt(const DFA_MmapHeader*);

public:
	terark_warn_unused_result
	static BaseDFA* load_from(FILE*);

	terark_warn_unused_result
	static BaseDFA* load_from(fstring fname);

	terark_warn_unused_result
	static BaseDFA* load_from_NativeInputBuffer(void* nativeInputBufferObject);

	void save_to(FILE*) const;
	void save_to(fstring fname) const;

	terark_warn_unused_result
	static BaseDFA* load_mmap(int fd);
	terark_warn_unused_result
	static BaseDFA* load_mmap(int fd, bool mmapPopulate);

	terark_warn_unused_result
	static BaseDFA* load_mmap(fstring fname);
	terark_warn_unused_result
	static BaseDFA* load_mmap(fstring fname, bool mmapPopulate);

	terark_warn_unused_result
	static BaseDFA* load_mmap_user_mem(const void* baseptr, size_t length);

	terark_warn_unused_result
	static BaseDFA* load_mmap_user_mem(fstring mem) {
		return load_mmap_user_mem(mem.data(), mem.size());
	}

	void save_mmap(function<void(const void*, size_t)> write) const;
	void save_mmap(int fd) const;
	void save_mmap(fstring fname) const;

	fstring get_mmap() const;

	BaseDFA();
	virtual ~BaseDFA();
	size_t get_sigma() const { return m_dyn_sigma; }
	void set_sigma(size_t sigma1) { m_dyn_sigma = sigma1; }

	virtual const BaseAC* get_ac() const;
	virtual const BaseDAWG* get_dawg() const;
	virtual const SuffixCountableDAWG* get_SuffixCountableDAWG() const;
	virtual bool has_freelist() const = 0;
//	virtual bool compute_is_dag() const = 0;

	virtual void dot_write_one_state(FILE* fp, size_t ls, const char* ext_attr) const;
	virtual void dot_write_one_move(FILE* fp, size_t s, size_t target, auchar_t ch) const;
	virtual void write_dot_file(FILE* fp) const;
	virtual void write_dot_file(fstring fname) const;

	void multi_root_write_dot_file(const size_t* pRoots, size_t nRoots, FILE* fp) const;
	void multi_root_write_dot_file(const size_t* pRoots, size_t nRoots, fstring fname) const;
	void multi_root_write_dot_file(const valvec<size_t>& roots, FILE* fp) const;
	void multi_root_write_dot_file(const valvec<size_t>& roots, fstring fname) const;

	void patricia_trie_write_dot_file(FILE* fp) const;
	void patricia_trie_write_dot_file(fstring fname) const;

    void write_child_stats(size_t root, fstring walkMethod, FILE*) const;
    void write_child_stats(size_t root, fstring walkMethod, fstring fname) const;

	virtual bool v_is_pzip(size_t s) const = 0;
	virtual bool v_is_term(size_t s) const = 0;
	virtual fstring v_get_zpath_data(size_t, MatchContext*) const = 0;
	virtual size_t zp_nest_level() const;
	virtual size_t v_total_states() const = 0;
	virtual size_t v_gnode_states() const = 0;
	virtual size_t v_nil_state() const = 0;
	virtual size_t v_max_state() const = 0;
	virtual size_t mem_size() const = 0;
	virtual size_t v_state_move(size_t curr, auchar_t ch) const = 0;
//	virtual size_t v_num_children(size_t) const = 0; // time: O(1) or O(n)
	virtual bool   v_has_children(size_t) const = 0; // time: O(1)

	typedef function<void(size_t child, auchar_t)> OnMove;
	typedef function<void(size_t child)> OnDest;
	virtual void v_for_each_move(size_t parent, const OnMove&) const = 0;
	virtual void v_for_each_dest(size_t parent, const OnDest&) const = 0;
	virtual void v_for_each_dest_rev(size_t parent, const OnDest&) const = 0;

	// length of dest/moves must be at least sigma
	virtual size_t get_all_dest(size_t s, size_t* dests) const = 0;
	virtual size_t get_all_move(size_t s, CharTarget<size_t>* moves) const = 0;

	void get_all_dest(size_t s, valvec<size_t>* dests) const;
	void get_all_move(size_t s, valvec<CharTarget<size_t> >* moves) const;

	void get_stat(DFA_MmapHeader*) const;
	virtual void str_stat(std::string*) const;
	std::string  str_stat() const { std::string s; str_stat(&s); return s; }

	/// output @param keys are unsorted, .seq_id is the final state for that key
	virtual void dfa_get_random_keys_append(SortableStrVec* keys, size_t max_keys) const;
	void dfa_get_random_keys(SortableStrVec* keys, size_t max_keys) const;

	size_t find_first_leaf(size_t root = initial_state) const;

	size_t pfs_put_children(size_t parent, febitvec& color, valvec<size_t>& stack, size_t* children_buf) const;

	inline size_t num_zpath_states() const { return m_zpath_states; }
	inline uint64_t total_zpath_len() const { return m_total_zpath_len; }

	inline bool is_dag() const { return m_is_dag; }

// low level operations
	inline void set_is_dag(bool val) { m_is_dag = val; }

	inline void set_kv_delim(auchar_t delim) {
		assert(delim < 512);
		m_kv_delim = delim;
	}
	inline auchar_t kv_delim() const { return m_kv_delim; }

	inline bool is_mmap() const { return NULL != mmap_base; }
	inline uint64_t adfa_total_words_len() const { return m_adfa_total_words_len; }

protected:
	void risk_swap(BaseDFA& y);
	long stat_impl(DFA_MmapHeader*, const void** dataPtrs) const;

	const DFA_MmapHeader* mmap_base;
	unsigned m_kv_delim : 9;
	unsigned m_is_dag   : 1;
	unsigned m_mmap_type: 2;
	unsigned m_dyn_sigma:10;
	size_t   m_zpath_states;
	uint64_t m_total_zpath_len;
	uint64_t m_adfa_total_words_len;
};

// The whole DFA is not required to be acyclic, only require acylic from 'root'
class TERARK_DLL_EXPORT AcyclicPathDFA : public BaseDFA {
public:

	virtual ADFA_LexIterator*   adfa_make_iter(size_t root = initial_state) const = 0;
	virtual ADFA_LexIterator16* adfa_make_iter16(size_t root = initial_state) const = 0;
};

class TERARK_DLL_EXPORT MatchingDFA : public AcyclicPathDFA {
public:
	terark_warn_unused_result static MatchingDFA* load_from(FILE*);
	terark_warn_unused_result static MatchingDFA* load_from(fstring fname);
	terark_warn_unused_result static MatchingDFA* load_mmap(int fd);
	terark_warn_unused_result static MatchingDFA* load_mmap(int fd, bool mmapPopulate);
	terark_warn_unused_result static MatchingDFA* load_mmap(fstring fname);
	terark_warn_unused_result static MatchingDFA* load_mmap(fstring fname, bool mmapPopulate);
	terark_warn_unused_result static MatchingDFA* load_mmap_user_mem(const void* baseptr, size_t length);

};

class TERARK_DLL_EXPORT DFA_MutationInterface {
protected:
	DFA_MutationInterface(const DFA_MutationInterface&);
	DFA_MutationInterface& operator=(const DFA_MutationInterface&);

public:
	DFA_MutationInterface();
	virtual ~DFA_MutationInterface();
	virtual const BaseDFA* get_BaseDFA() const = 0;

	virtual void resize_states(size_t new_states_num) = 0;
	virtual size_t new_state() = 0;
    virtual size_t clone_state(size_t source) = 0;
	virtual void del_state(size_t state_id) = 0;
	virtual void add_all_move(size_t state_id, const CharTarget<size_t>* moves, size_t n_moves) = 0;
	inline  void add_all_move(size_t state_id, const valvec<CharTarget<size_t> >& moves) {
				 add_all_move(state_id, moves.data(), moves.size()); }

	virtual void del_all_move(size_t state_id) = 0;
	virtual void del_move(size_t s, auchar_t ch);

#ifdef TERARK_AUTOMATA_ENABLE_MUTATION_VIRTUALS
    virtual bool add_word(size_t RootState, fstring key) = 0;
    bool add_word(fstring key) { return add_word(initial_state, key); }

	virtual size_t copy_from(size_t SrcRoot) = 0;
	virtual void remove_dead_states() = 0;
	virtual void remove_sub(size_t RootState) = 0;

    virtual void graph_dfa_minimize() = 0;
    virtual void trie_dfa_minimize() = 0;
    virtual void adfa_minimize() = 0;
#endif
protected:
    virtual size_t add_move_imp(size_t source, size_t target, auchar_t ch, bool OverwriteExisted) = 0;
};

struct DawgIndexIter {
	size_t index;
	size_t matchedLen;
	bool   isFound;
};

class TERARK_DLL_EXPORT BaseDAWG {
protected:
	BaseDAWG(const BaseDAWG&);
	BaseDAWG& operator=(const BaseDAWG&);

	size_t n_words;

public:
	static const size_t null_word = size_t(-1);

	BaseDAWG();
	virtual ~BaseDAWG();

	size_t num_words() const { return n_words; }

    virtual size_t index(fstring word) const;
    virtual void lower_bound(fstring word, size_t* index, size_t* dict_rank) const;
    virtual void nth_word(size_t nth, std::string* word) const;
	std::string  nth_word(size_t nth) const;

	DawgIndexIter dawg_lower_bound(fstring) const;

	typedef function<void(size_t len, size_t nth)> OnMatchDAWG;
    size_t match_dawg(fstring, const OnMatchDAWG&) const;
    size_t match_dawg(fstring, const OnMatchDAWG&, const ByteTR&) const;
    size_t match_dawg(fstring, const OnMatchDAWG&, const byte_t*) const;

    size_t match_dawg_l(fstring, const OnMatchDAWG&) const;
    size_t match_dawg_l(fstring, const OnMatchDAWG&, const ByteTR&) const;
    size_t match_dawg_l(fstring, const OnMatchDAWG&, const byte_t*) const;

	bool match_dawg_l(fstring, size_t* len, size_t* nth, const ByteTR&) const;
	bool match_dawg_l(fstring, size_t* len, size_t* nth, const byte_t*) const;
	bool match_dawg_l(fstring, size_t* len, size_t* nth) const;

	virtual size_t index(MatchContext&, fstring word) const = 0;
    virtual void lower_bound(MatchContext&, fstring word, size_t* index, size_t* dict_rank) const;
	virtual size_t v_state_to_word_id(size_t state) const;
	virtual size_t state_to_dict_rank(size_t state) const;

	/// output @param keys are unsorted, .seq_id is the word id of that key
	virtual void get_random_keys_append(SortableStrVec* keys, size_t max_keys) const;
	void get_random_keys(SortableStrVec* keys, size_t max_keys) const;

protected:
	virtual void nth_word(MatchContext&, size_t nth, std::string* word) const = 0;
	virtual DawgIndexIter dawg_lower_bound(MatchContext&, fstring) const = 0;

    virtual size_t match_dawg(MatchContext&, size_t base_nth, fstring, const OnMatchDAWG&) const = 0;
    virtual size_t match_dawg(MatchContext&, size_t base_nth, fstring, const OnMatchDAWG&, const ByteTR&) const = 0;
    virtual size_t match_dawg(MatchContext&, size_t base_nth, fstring, const OnMatchDAWG&, const byte_t*) const = 0;

	virtual bool match_dawg_l(MatchContext&, fstring, size_t* len, size_t* nth) const = 0;
	virtual bool match_dawg_l(MatchContext&, fstring, size_t* len, size_t* nth, const ByteTR&) const = 0;
	virtual bool match_dawg_l(MatchContext&, fstring, size_t* len, size_t* nth, const byte_t*) const = 0;

	friend class LazyUnionDAWG;
};

class TERARK_DLL_EXPORT SuffixCountableDAWG : public BaseDAWG {
public:
	virtual size_t suffix_cnt(size_t root) const = 0;

	///@{
	///@returns suffix_cnt of exact_prefix
	size_t suffix_cnt(fstring exact_prefix) const;
	size_t suffix_cnt(fstring exact_prefix, const ByteTR& tr) const;
	size_t suffix_cnt(fstring exact_prefix, const byte_t* tr) const;
	///@}

	///@{
	///@param[out] cnt on return
	///   cnt[i] == suffix_cnt(str.substr(0,i));
	///   cnt->size()-1 is the max matched length
	void path_suffix_cnt(fstring str, valvec<size_t>* cnt) const;
	void path_suffix_cnt(fstring str, valvec<size_t>* cnt, const ByteTR& tr) const;
	void path_suffix_cnt(fstring str, valvec<size_t>* cnt, const byte_t* tr) const;
	///@}

protected:
	virtual size_t suffix_cnt(MatchContext&, fstring) const = 0;
	virtual size_t suffix_cnt(MatchContext&, fstring, const ByteTR&) const = 0;
	virtual size_t suffix_cnt(MatchContext&, fstring, const byte_t*) const = 0;

	virtual void path_suffix_cnt(MatchContext&, fstring, valvec<size_t>*) const = 0;
	virtual void path_suffix_cnt(MatchContext&, fstring, valvec<size_t>*, const ByteTR&) const = 0;
	virtual void path_suffix_cnt(MatchContext&, fstring, valvec<size_t>*, const byte_t*) const = 0;

	friend class LazyUnionDAWG;
};

TERARK_DLL_EXPORT
size_t
dot_escape(const char* ibuf, size_t ilen, char* obuf, size_t olen);
TERARK_DLL_EXPORT
size_t
dot_escape(const auchar_t* ibuf, size_t ilen, char* obuf, size_t olen);

} // namespace terark

#endif // __terark_automata_dfa_interface_hpp__

