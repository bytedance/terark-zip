#include "fsa.hpp"
#include "dfa_mmap_header.hpp"
#include "tmplinst.hpp"

#include "x_fsa_util.hpp"

#include "forward_decl.hpp"

#include <terark/util/autoclose.hpp>
#include <terark/bitmap.hpp>
#include <terark/util/unicode_iterator.hpp>

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/num_to_str.hpp>

#include <errno.h>

// For Memory Map support
#include <fcntl.h>
#ifdef _MSC_VER
	#define WIN32_LEAN_AND_MEAN
	#define NOMINMAX
	#include <io.h>
	#include <Windows.h>
	#define munmap(base,size) UnmapViewOfFile(base)
	#define O_FORCE_BINARY _O_BINARY
#else
	#include <sys/mman.h>
	#include <unistd.h>
	#define O_FORCE_BINARY 0
#endif

#include <terark/util/stat.hpp>

#if !defined(MAP_LOCKED)
#define  MAP_LOCKED 0
#endif

#if !defined(MAP_POPULATE)
	#define  MAP_POPULATE 0
#endif

#if !defined(O_LARGEFILE)
	#define  O_LARGEFILE 0
#endif

namespace terark {

bool g_enableChecksumVerify = getEnvBool("Terark_enableChecksumVerify", true);

TERARK_DLL_EXPORT bool isChecksumVerifyEnabled() {
    return g_enableChecksumVerify;
}

TERARK_DLL_EXPORT void enableChecksumVerify(bool val) {
    g_enableChecksumVerify = val;
}

ByteInputRange::~ByteInputRange() {}

template<class CharT>
ADFA_LexIteratorT<CharT>::ADFA_LexIteratorT(const BaseDFA* dfa) {
	m_dfa = dfa;
	m_word.reserve(128);
	m_curr = size_t(-1);
}
/// do not reserve/allocate memory
template<class CharT>
ADFA_LexIteratorT<CharT>::ADFA_LexIteratorT(valvec_no_init) {
	m_dfa = NULL;
	m_curr = size_t(-1);
}
template<class CharT>
ADFA_LexIteratorT<CharT>::~ADFA_LexIteratorT() {}

template<class CharT>
void ADFA_LexIteratorT<CharT>::dispose() {
	// default is to direct delete
	delete this;
}

template<class CharT>
bool ADFA_LexIteratorT<CharT>::seek_begin() {
	return seek_lower_bound(fstr());
}

template<class CharT>
bool ADFA_LexIteratorT<CharT>::seek_rev_lower_bound(fstr str) {
    if (seek_lower_bound(str)) {
        if (word() == str)
            return true;
        return decr();
    }
    return seek_end();
}

template class ADFA_LexIteratorT<char>;
template class ADFA_LexIteratorT<uint16_t>;

template<class CharT>
ADFA_LexIteratorData<CharT>::ADFA_LexIteratorData(const BaseDFA* dfa)
  : ADFA_LexIteratorT<CharT>(dfa)
{
	m_isForward = true;
	m_root = 0;
	m_iter.reserve(128);
	m_node.reserve(512);
}

template<class CharT>
ADFA_LexIteratorData<CharT>::ADFA_LexIteratorData(valvec_no_init)
  : ADFA_LexIteratorT<CharT>(valvec_no_init())
{
	m_isForward = true;
	m_root = 0;
}

template<class CharT>
ADFA_LexIteratorData<CharT>::~ADFA_LexIteratorData()
{}

template class ADFA_LexIteratorData<char>;
template class ADFA_LexIteratorData<uint16_t>;

ComplexMatchContext::~ComplexMatchContext() {
	// do nothing
}

MatchContext::MatchContext(const MatchContext& y)
  : MatchContextBase(y), zbuf(y.zbuf)
{
	if (y.complex_context)
		complex_context = y.complex_context->clone();
	else
		complex_context = NULL;
}

MatchContext::~MatchContext() {
	delete complex_context;
}

MatchContext&
MatchContext::operator=(const MatchContext& y) {
	MatchContext(y).swap(*this);
	return *this;
}

void MatchContext::reset() {
	delete complex_context;
	pos = 0;
	root = 0; // initial_state;
	zidx = 0;
	complex_context = NULL;
}

void MatchContext::reset(size_t pos1, size_t root1, size_t zidx1) {
	delete complex_context;
	pos = pos1;
	root = root1;
	zidx = zidx1;
	complex_context = NULL;
}

struct DFA_MmapType {
	enum Enum {
		is_invalid = 0,
		is_mmap = 1,
		is_malloc = 2,
		is_user_mem = 3,
	};
};

void BaseDFADeleter::operator()(BaseDFA* p) const {
	delete p;
}
void BaseDFADeleter::operator()(MatchingDFA* p) const {
	delete p;
}
BaseDFA* BaseDFA_load(fstring fname) {
	return BaseDFA::load_from(fname);
}
BaseDFA* BaseDFA_load(FILE* fp) {
	return BaseDFA::load_from(fp);
}
MatchingDFA* MatchingDFA_load(fstring fname) {
	return MatchingDFA::load_from(fname);
}
MatchingDFA* MatchingDFA_load(FILE* fp) {
	return MatchingDFA::load_from(fp);
}

BaseDFA::BaseDFA() {
	mmap_base = NULL;
	m_mmap_type = DFA_MmapType::is_invalid;
	m_kv_delim = 256; // default use min non-byte as delim
	m_is_dag = 0;
	m_zpath_states = 0;
	m_total_zpath_len = 0;
	m_dyn_sigma = 0;
	m_adfa_total_words_len = 0;
}

#define TERARK_DEFINE_default_copy(Class) \
	Class::Class(const Class&) = default; \
	Class& Class::operator=(const Class& y) = default
TERARK_DEFINE_default_copy(BaseDFA);
TERARK_DEFINE_default_copy(DFA_MutationInterface);
TERARK_DEFINE_default_copy(BaseDAWG);

void BaseDFA::get_all_dest(size_t s, valvec<size_t>* dests) const {
	dests->resize_no_init(512); // current max possible sigma
	dests->risk_set_size(get_all_dest(s, dests->data()));
	assert(dests->size() <= 512);
}
void BaseDFA::get_all_move(size_t s, valvec<CharTarget<size_t> >* moves) const {
	moves->resize_no_init(512); // current max possible sigma
	moves->risk_set_size(get_all_move(s, moves->data()));
	assert(moves->size() <= 512);
}

#define CheckBufferOverRun(inclen) do { \
	if (q + inclen > oend) { \
		std::string msg; \
		msg += __FILE__; \
		msg += ":"; \
		msg += BOOST_STRINGIZE(__LINE__); \
		msg += ": "; \
		msg += BOOST_CURRENT_FUNCTION; \
		throw std::runtime_error(msg); \
	} } while (0)

template<class Char>
size_t
dot_escape_aux(const Char* ibuf, size_t ilen, char* obuf, size_t olen) {
	const Char* iend = ibuf + ilen;
	char* q = obuf;
	char* oend = obuf + olen - 1;
	const char* hex = "0123456789ABCDEF";
	for (const Char* p = ibuf; p < iend && q < oend; ++p) {
		Char ch = *p;
		switch (ch) {
		default:
			if (sizeof(Char) == 1 && (ch & 0xC0) == 0xC0) {
				size_t utf8_len = terark::utf8_byte_count(ch);
				CheckBufferOverRun(utf8_len);
				if (p + utf8_len <= iend) {
					memcpy(q, p, utf8_len);
					p += utf8_len - 1;
					q += utf8_len;
					break;
				}
			}
			if (isgraph(ch))
				*q++ = ch;
			else {
				CheckBufferOverRun(5);
				*q++ = '\\';
				*q++ = '\\';
				*q++ = 'x';
				if (sizeof(Char) > 1 && (ch & 0xFF00)) {
					if (ch & 0xF000)
						*q++ = hex[ch >> 12 & 0x0F];
					*q++ = hex[ch >> 8 & 0x0F];
				}
				*q++ = hex[ch >> 4 & 0x0F];
				*q++ = hex[ch >> 0 & 0x0F];
			}
			break;
		case '\\':
			CheckBufferOverRun(4);
			*q++ = '\\'; *q++ = '\\';
			*q++ = '\\'; *q++ = '\\';
			break;
		case '"':
			CheckBufferOverRun(2);
			*q++ = '\\';
			*q++ = '"';
			break;
		case '[': // meta char
		case ']': // meta char
		case '.': // meta char
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = ch;
			break;
		case '\b':
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = 'b';
			break;
		case '\v':
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = 'v';
			break;
		case '\t':
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = 't';
			break;
		case '\r':
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = 'r';
			break;
		case '\n':
			CheckBufferOverRun(3);
			*q++ = '\\'; *q++ = '\\'; *q++ = 'n';
			break;
		}
	}
	*q = '\0';
	return q - obuf;
}

size_t
dot_escape(const char* ibuf, size_t ilen, char* obuf, size_t olen) {
	return dot_escape_aux(ibuf, ilen, obuf, olen);
}
size_t
dot_escape(const auchar_t* ibuf, size_t ilen, char* obuf, size_t olen) {
	return dot_escape_aux(ibuf, ilen, obuf, olen);
}

void BaseDFA::dot_write_one_state(FILE* fp, size_t s, const char* ext_attr) const {
	long ls = s;
	if (v_is_pzip(ls)) {
		MatchContext ctx;
		fstring zs = v_get_zpath_data(ls, &ctx);
		char buf[1040];
		dot_escape(zs.data(), zs.size(), buf, sizeof(buf)-1);
		if (v_is_term(ls))
			fprintf(fp, "\tstate%ld[label=\"%ld: %s\" shape=\"doublecircle\" %s];\n", ls, ls, buf, ext_attr);
		else
			fprintf(fp, "\tstate%ld[label=\"%ld: %s\" %s];\n", ls, ls, buf, ext_attr);
	}
	else {
		if (v_is_term(ls))
			fprintf(fp, "\tstate%ld[label=\"%ld\" shape=\"doublecircle\" %s];\n", ls, ls, ext_attr);
		else
			fprintf(fp, "\tstate%ld[label=\"%ld\" %s];\n", ls, ls, ext_attr);
	}
}

void BaseDFA::dot_write_one_move(FILE* fp, size_t s, size_t t, auchar_t ch)
const {
	char buf[32];
	dot_escape(&ch, 1, buf, sizeof(buf));
	fprintf(fp, "\tstate%ld -> state%ld [label=\"%s\"];\n", long(s), long(t), buf);
}

void BaseDFA::write_dot_file(const fstring fname) const {
	terark::Auto_fclose dot(fopen(fname.p, "w"));
	if (dot) {
		write_dot_file(dot);
	} else {
		fprintf(stderr, "can not open %s for write: %s\n", fname.p, strerror(errno));
	}
}

namespace {
struct By_ch {
template<class CT> bool operator()(const CT& x, auchar_t y) const { return x.ch < y; }
template<class CT> bool operator()(auchar_t x, const CT& y) const { return x < y.ch; }
};
static
void cclabel0(const CharTarget<size_t>* p
			, const CharTarget<size_t>* q
			, std::string& buf)
{
	for(auto i = p; i < q; ) {
		auto c = i->ch;
		auto j = i + 1;
		while (j < q && j-i == intptr_t(j->ch-c)) ++j;
		if (j - i == 1) {
			buf.push_back(c);
		}
	   	else if (j - i <= 3) {
			for (auto k = i; k < j; ++k) buf.push_back(k->ch);
		}
		else {
			buf.push_back(i->ch);
			buf.push_back('-');
			buf.push_back(j[-1].ch);
		}
		i = j;
	}
}
static
void cclabel2(const CharTarget<size_t>* p
			, const CharTarget<size_t>* q
			, std::string& sbuf)
{
	for(auto i = p; i < q; ) {
		auto j = i + 1;
		auchar_t minch = i->ch;
		while (j < q && j-i == j->ch-minch) ++j;
		auchar_t maxch = j[-1].ch;
	//	fprintf(stderr, "cclabel2: j-i=%ld minch=%02X maxch=%02X\n", j-i, minch, maxch);
		if (j-i >= 4
			//	&& ((minch > '^' && maxch & 0x80) || maxch < 0x30)
				&& (0 == minch || !strchr("-^[]", minch))
				&& (maxch > 255 || !strchr("-^[]", maxch))
		 ) {
	//		fprintf(stderr, "cclabel2: 鬼鬼\n");
			char buf[32];
			auchar_t ch = minch;
			sbuf.append(buf, dot_escape(&ch, 1, buf, sizeof(buf)));
			sbuf.push_back('-');
			ch = maxch;
			sbuf.append(buf, dot_escape(&ch, 1, buf, sizeof(buf)));
		}
	   	else {
			for (auto k = i; k < j; ++k) {
				char buf[32];
				if (k->ch < 256) {
					auchar_t ch = k->ch;
					if ('-' == ch || '^' == ch)
						sbuf.append("\\\\"), sbuf.push_back(ch);
					else
						sbuf.append(buf, dot_escape(&ch, 1, buf, sizeof(buf)));
				} else {
					sprintf(buf, "\\\\%03X", (int)k->ch);
					sbuf.append(buf);
				}
			}
		}
		i = j;
	}
}
static void
print_labels(FILE* fp, size_t parent, const CharTarget<size_t>* children, size_t size) {
	std::string sbuf;
	for(auto i = children; i < children + size; ) {
		auto j = i;
		auto t = i->target;
		while (j < children + size && j->target == t) ++j;
		sbuf.resize(0);
		if (j - i == 1) { // same as BaseDFA::dot_write_one_move
			char buf[32];
			auchar_t ch = i->ch;
			sbuf.append(buf, dot_escape(&ch, 1, buf, sizeof(buf)));
		}
		else if (j - i == 256) {
			sbuf.append(".");
		}
		else if (j - i >= 236) {
			sbuf.append("[^");
			for (auchar_t c = 0; c < 512; ++c) {
				if (c != '-' && !std::binary_search(i, j, c, By_ch())) {
					char buf[32];
					auchar_t ch = c;
					sbuf.append(buf, dot_escape(&ch, 1, buf, sizeof(buf)));
				}
			}
			if (!std::binary_search(i, j, '-', By_ch()))
			 	sbuf.push_back('-');
			sbuf.append("]");
		}
		else {
			sbuf.append("[");
			auto dig0 = std::lower_bound(i, j, '0', By_ch());
			auto dig1 = std::upper_bound(i, j, '9', By_ch());
			auto upp0 = std::lower_bound(i, j, 'A', By_ch());
			auto upp1 = std::upper_bound(i, j, 'Z', By_ch());
			auto low0 = std::lower_bound(i, j, 'a', By_ch());
			auto low1 = std::upper_bound(i, j, 'z', By_ch());
			if (dig0 < dig1) cclabel0(dig0, dig1, sbuf);
			if (upp0 < upp1) cclabel0(upp0, upp1, sbuf);
			if (low0 < low1) cclabel0(low0, low1, sbuf);
			if (i    < dig0) cclabel2(i   , dig0, sbuf);
			if (dig1 < upp0) cclabel2(dig1, upp0, sbuf);
			if (upp1 < low0) cclabel2(upp1, low0, sbuf);
			if (low1 < j   ) cclabel2(low1, j   , sbuf);
			sbuf.append("]");
		}
		fprintf(fp, "\tstate%ld -> state%ld [label=\"%s\"];\n"
			, long(parent), long(t), sbuf.c_str());
		i = j;
	}
}
} // namespace

void BaseDFA::write_dot_file(FILE* fp) const {
	size_t RootState = 0;
	multi_root_write_dot_file(&RootState, 1, fp);
}

void BaseDFA::
multi_root_write_dot_file(const size_t* pRoots, size_t nRoots, FILE* fp)
const {
//	printf("%s:%d %s\n", __FILE__, __LINE__, BOOST_CURRENT_FUNCTION);
	fprintf(fp, "digraph G {\n");
	febitvec   color(v_total_states(), 0);
	febitvec   is_written(v_total_states(), 0);
	valvec<size_t> stack;
	terark::AutoFree<CharTarget<size_t> > children(m_dyn_sigma);
	for(size_t i = nRoots; i > 0; ) {
		size_t RootState = pRoots[--i];
		color.set1(RootState);
		stack.push_back(RootState);
		dot_write_one_state(fp, RootState, "color=\"green\"");
		is_written.set1(RootState);
	}
	while (!stack.empty()) {
		size_t curr = stack.back(); stack.pop_back();
		size_t size = get_all_move(curr, children);
		for(size_t i = 0; i < size; ++i) {
			size_t child = children[i].target;
			if (color.is0(child)) {
				color.set1(child);
				stack.push_back(child);
			}
		}
		if (is_written.is1(curr)) continue;
		if (size) {
			bool hasDelim = binary_search_ex_0(children.p, size,
				   	m_kv_delim, CharTarget_By_ch());
			if (hasDelim || children[size-1].ch > 255)
				dot_write_one_state(fp, curr, "color=\"blue\"");
			else
				dot_write_one_state(fp, curr, "");
		} else
			dot_write_one_state(fp, curr, "");
		is_written.set1(curr);
	}
	color.fill(0);
	for(size_t i = nRoots; i > 0; ) {
		size_t RootState = pRoots[--i];
		color.set1(RootState);
		stack.push_back(RootState);
	}
	while (!stack.empty()) {
		size_t curr = stack.back(); stack.pop_back();
		size_t size = get_all_move(curr, children);
		if (size > 1) {
			auto By_target_ch =
			[](const CharTarget<size_t>& x, const CharTarget<size_t>& y) {
				 if (x.target != y.target)
					 return x.target < y.target;
				 else
					 return x.ch < y.ch;
			};
			std::sort(children.p, children + size, By_target_ch);
			print_labels(fp, curr, children, size);
		}
		for(size_t i = 0; i < size; ++i) {
			size_t child = children[i].target;
			auchar_t ch  = children[i].ch;
			if (color.is0(child)) {
				color.set1(child);
				stack.push_back(child);
			}
			if (size == 1)
				dot_write_one_move(fp, curr, child, ch);
		}
	}
	fprintf(fp, "}\n");
}

void BaseDFA::
multi_root_write_dot_file(const size_t* pRoots, size_t nRoots, fstring fname)
const {
	terark::Auto_fclose dot(fopen(fname.p, "w"));
	if (dot) {
		multi_root_write_dot_file(pRoots, nRoots, dot);
	} else {
		fprintf(stderr, "can not open %s for write: %s\n", fname.p, strerror(errno));
	}
}

void BaseDFA::
multi_root_write_dot_file(const valvec<size_t>& roots, FILE* fp)
const {
	multi_root_write_dot_file(roots.data(), roots.size(), fp);
}

void BaseDFA::
multi_root_write_dot_file(const valvec<size_t>& roots, fstring fname)
const {
	multi_root_write_dot_file(roots.data(), roots.size(), fname);
}

void BaseDFA::patricia_trie_write_dot_file(FILE* fp) const {
	fprintf(fp, "digraph G {\n");
	terark::AutoFree<CharTarget<size_t> > children(m_dyn_sigma);
	febitvec isVisited(this->v_total_states(), false);
	valvec<size_t> stack;
	valvec<char>  patriciaRaw;
	valvec<char>  patriciaStr;
	MatchContext  ctx;
	auto writePatriciaNode = [&](size_t node) {
		fstring zstr = this->v_get_zpath_data(node, &ctx);
		patriciaRaw.append(zstr);
		patriciaStr.resize_no_init(patriciaRaw.size() * 6 + 10);
		int pstrLen = (int)
		dot_escape(patriciaRaw.data(), patriciaRaw.size(),
				   patriciaStr.data(), patriciaStr.size());
		fprintf(fp, "\tzEdge%zd[label=\"%.*s\" shape=\"box\" color=\"maroon\"];\n"
				  , node, pstrLen, patriciaStr.data());
	};
	bool alwaysDrawSuper = getEnvBool("PatriciaTrieAlwaysDrawSuper", false);
	if (v_is_pzip(initial_state)) {
		fprintf(fp, "super[style=\"dotted\"]\n");
		writePatriciaNode(initial_state);
	}
	else if (alwaysDrawSuper) {
		fprintf(fp, "super[style=\"dotted\"]\n");
	}
	stack.push_back(initial_state);
	while (!stack.empty()) {
		size_t state = stack.pop_val();
		if (isVisited[state]) {
			THROW_STD(invalid_argument,
				"ERROR: Found shared state, this DFA is not a trie");
		}
		isVisited.set1(state);
		size_t cnt = this->get_all_move(state, children);
		for(size_t i = cnt; i > 0; ) {
			size_t child = children[--i].target;
			if (v_is_pzip(child)) {
				patriciaRaw.erase_all();
				patriciaRaw.push_back(children[i].ch);
				writePatriciaNode(child);
			}
			stack.push_back(child);
		}
		if (v_is_term(state))
			fprintf(fp, "\tstate%zd[label=\"%zd\" shape=\"doublecircle\"];\n", state, state);
		else
			fprintf(fp, "\tstate%zd[label=\"%zd\"];\n", state, state);
	}
	if (v_is_pzip(initial_state)) {
		fprintf(fp, "super -> zEdge0;\n");
		fprintf(fp, "zEdge0 -> state0;\n");
	}
	else if (alwaysDrawSuper) {
		fprintf(fp, "super -> state0 [style=\"dotted\"];\n");
	}
	stack.push_back(initial_state);
	while (!stack.empty()) {
		size_t state = stack.pop_val();
		size_t cnt = this->get_all_move(state, children);
		for(size_t i = cnt; i > 0; ) {
			size_t child = children[--i].target;
			stack.push_back(child);
			if (this->v_is_pzip(child)) {
				fprintf(fp, "\tstate%zd -> zEdge%zd [color=\"maroon\"];\n", state, child);
				fprintf(fp, "\tzEdge%zd -> state%zd [color=\"maroon\"];\n", child, child);
			}
			else {
				auchar_t ch = children[i].ch;
				char label[8];
				int  len = (int)dot_escape(&ch, 1, label, sizeof(label));
				fprintf(fp, "\tstate%zd -> state%zd [label=\"%.*s\"];\n"
						  , state, child, len, label);
			}
		}
	}
	fprintf(fp, "}\n"); // digraph
}

void BaseDFA::patricia_trie_write_dot_file(fstring fname) const {
	terark::Auto_fclose dot(fopen(fname.c_str(), "w"));
	if (dot) {
		patricia_trie_write_dot_file(dot);
	} else {
		fprintf(stderr, "can not open %s for write: %s\n", fname.p, strerror(errno));
	}
}

void BaseDFA::write_child_stats(size_t root, fstring walkMethod, FILE* fp) const {
    if (strcasecmp(walkMethod.c_str(), "bfs") == 0) {
        valvec<size_t> q1(v_total_states() / 16, valvec_reserve());
        valvec<size_t> q2(v_total_states() / 16, valvec_reserve());
        q1.push_back(root);
        size_t n_parent = 0;
        size_t n_childs = 0;
        while (!q1.empty()) {
            for(size_t i = 0; i < q1.size(); ++i) {
                size_t cur = q1[i];
                size_t num = get_all_dest(cur, q2.grow_capacity(m_dyn_sigma));
                assert(num <= m_dyn_sigma);
                q2.risk_set_size(q2.size() + num);
                n_parent += 1;
                n_childs += num;
                fprintf(fp, "%zd\t%zd\t%f\n", n_parent, n_childs, double(n_childs) / n_parent);
            }
            q1.swap(q2);
            q2.erase_all();
        }
    }
    else {
        fprintf(stderr, "BaseDFA::write_child_stats: walkMethod must be \"bfs\" for now\n");
    }
}

void BaseDFA::write_child_stats(size_t root, fstring walkMethod, fstring fname) const {
	terark::Auto_fclose fp(fopen(fname.c_str(), "w"));
	if (fp) {
		write_child_stats(root, walkMethod, fp);
	} else {
		fprintf(stderr
            , "BaseDFA::write_child_stats: can not open %s for write: %s\n"
            , fname.p, strerror(errno));
	}
}


void BaseDFA::finish_load_mmap(const DFA_MmapHeader*) {
	// do nothing
}
long BaseDFA::prepare_save_mmap(DFA_MmapHeader*, const void**) const {
	// nth bit set to 1 means dataPtrs[nth] need to be free'ed
	return 0;
}
static
void free_save_mmap_ptrs(size_t blockNum, const void** dataPtrs, long mask) {
	BOOST_STATIC_ASSERT(DFA_MmapHeader::MAX_BLOCK_NUM <= 32);
	assert(blockNum <= DFA_MmapHeader::MAX_BLOCK_NUM);
	if (blockNum > DFA_MmapHeader::MAX_BLOCK_NUM) {
		THROW_STD(logic_error
			, "Fatal: blockNum = %ld, DFA_MmapHeader::MAX_BLOCK_NUM = %d"
			, long(blockNum), DFA_MmapHeader::MAX_BLOCK_NUM);
	}
	if (mask) {
		size_t max_mask = blockNum == 32 ? UINT32_MAX : ~(size_t(-1) << blockNum);
		TERARK_RT_assert(size_t(mask) <= max_mask, std::invalid_argument);
	}
	for (size_t i = 0; i < blockNum; ++i) {
		if (mask & (long(1) << i)) {
			assert(NULL != dataPtrs[i]);
			::free(const_cast<void*>(dataPtrs[i]));
		}
	}
}

/////////////////////////////////////////////////////////////////////////

BaseDFA::~BaseDFA() {
	if (mmap_base) {
		switch (m_mmap_type) {
		case DFA_MmapType::is_invalid:
			assert(0); abort();
			break;
		case DFA_MmapType::is_mmap:
			::munmap((void*)mmap_base, mmap_base->file_size);
			break;
		case DFA_MmapType::is_malloc:
			::free((void*)mmap_base);
			break;
		case DFA_MmapType::is_user_mem:
			// do nothing...
			break;
		}
	}
}

BaseDFA* BaseDFA::load_mmap(int fd) {
	bool mmapPopulate = getEnvBool("DFA_MAP_POPULATE", false);
	return load_mmap(fd, mmapPopulate);
}

BaseDFA* BaseDFA::load_mmap(int fd, bool mmapPopulate) {
	if (fd < 0) {
		THROW_STD(invalid_argument,	"fd=%d < 0", fd);
	}
#ifdef _MSC_VER
//	LARGE_INTEGER fsize;
//	if (!GetFileSizeEx((HANDLE)mmap_fd, &fsize) {
//		DWORD err = GetLastError();
//		THROW_STD(runtime_error, "GetFileSizeEx().ErrCode=%d(0x%X)", err, err);
//	}
	HANDLE hFile = (HANDLE)::_get_osfhandle(fd);
	HANDLE hMmap = CreateFileMapping(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
	if (NULL == hMmap) {
		DWORD err = GetLastError();
		THROW_STD(runtime_error, "CreateFileMapping().ErrCode=%d(0x%X)", err, err);
	}
	const DFA_MmapHeader* base = (const DFA_MmapHeader*)
		MapViewOfFile(hMmap, FILE_MAP_READ, 0, 0, 0);
	if (NULL == base) {
		DWORD err = GetLastError();
		CloseHandle(hMmap);
		THROW_STD(runtime_error, "MapViewOfFile().ErrCode=%d(0x%X)", err, err);
	}
	if (mmapPopulate) {
		WIN32_MEMORY_RANGE_ENTRY vm;
		vm.VirtualAddress = (void*)base;
		vm.NumberOfBytes  = base->file_size;
		PrefetchVirtualMemory(GetCurrentProcess(), 1, &vm, 0);
	}
	CloseHandle(hMmap);
#else
	struct stat st;
	if (::fstat(fd, &st) < 0) {
		THROW_STD(runtime_error, "fstat(fd=%d) = %s", fd, strerror(errno));
	}
	if (!S_ISREG(st.st_mode)) {
		THROW_STD(invalid_argument, "Warning: st_mode=0x%lX is not a regular file",
			(long)st.st_mode);
	}
	if (st.st_size <= (long)sizeof(DFA_MmapHeader)) {
		THROW_STD(invalid_argument, "FileSize=%ld is less than HeaderSize=%ld",
			(long)st.st_size, (long)sizeof(DFA_MmapHeader));
	}
	int flags = MAP_SHARED;
	if (mmapPopulate) {
		flags |= MAP_POPULATE;
	}
  #if MAP_LOCKED
	if (getEnvBool("DFA_MAP_LOCKED", false)) {
		flags |= MAP_LOCKED;
	}
  #endif
	const DFA_MmapHeader* base = (const DFA_MmapHeader*)
		::mmap(NULL, st.st_size, PROT_READ, flags, fd, 0);
	if (MAP_FAILED == base) {
		THROW_STD(runtime_error, "mmap(PROT_READ, fd=%d) = %s", fd, strerror(errno));
	}
	size_t fsize = st.st_size;
#endif
	if (fsize < base->file_size) {
		THROW_STD(invalid_argument, "length=%lld, header.file_size=%lld"
			, (long long)fsize, (long long)base->file_size);
	}
	BaseDFA* dfa = load_mmap_fmt(base);
	dfa->m_mmap_type = DFA_MmapType::is_mmap;
	return dfa;
}

BaseDFA* BaseDFA::load_mmap_user_mem(const void* baseptr, size_t length) {
	auto header = reinterpret_cast<const DFA_MmapHeader*>(baseptr);
	if (length < header->file_size) {
		THROW_STD(invalid_argument, "length=%lld, header.file_size=%lld"
			, (long long)length, (long long)header->file_size);
	}
	BaseDFA* dfa = load_mmap_fmt(header);
	dfa->m_mmap_type = DFA_MmapType::is_user_mem;
	return dfa;
}

BaseDFA* BaseDFA::load_mmap_fmt(const DFA_MmapHeader* base) {
	if (strcmp(base->magic, "nark-dfa-mmap") != 0) {
		THROW_STD(invalid_argument, "file is not nark-dfa-mmap");
	}
	const DFA_ClassMetaInfo* meta = DFA_ClassMetaInfo::find(base->dfa_class_name);
	if (NULL == meta) {
		TERARK_THROW(std::invalid_argument,
			": unknown dfa_class: %s", base->dfa_class_name);
	}
	if (base->crc32cLevel >= 1 && g_enableChecksumVerify) {
		uint32_t header_crc32 = Crc32c_update(0, base, sizeof(*base)-4);
		if (base->header_crc32 != header_crc32) {
			throw BadCrc32cException("BaseDFA::load_mmap_fmt(): header_crc32"
				, base->header_crc32, header_crc32);
		}
	}
	if (base->crc32cLevel >= 2 && g_enableChecksumVerify) {
		size_t  content_len = base->file_size - sizeof(*base);
		uint32_t file_crc32 = Crc32c_update(0, base+1, content_len);
		if (base->file_crc32 != file_crc32) {
			throw BadCrc32cException("BaseDFA::load_mmap_fmt(): file_crc32"
				, base->file_crc32, file_crc32);
		}
	}
	std::unique_ptr<BaseDFA> dfa(meta->create());
	dfa->finish_load_mmap(base);
	dfa->mmap_base  = base;
	dfa->m_is_dag   = base->is_dag ? 1 : 0;
	dfa->m_kv_delim = base->kv_delim;
	dfa->m_zpath_states = base->zpath_states;
	dfa->m_total_zpath_len = base->zpath_length;
	dfa->m_adfa_total_words_len = base->adfa_total_words_len;
	return dfa.release();
}

long BaseDFA::stat_impl(DFA_MmapHeader* pHeader, const void** dataPtrs) const {
	DFA_MmapHeader& header = *pHeader;
	memset(&header, 0, sizeof(header));
	const DFA_ClassMetaInfo* meta = DFA_ClassMetaInfo::find(this);
	if (NULL == meta) {
		TERARK_THROW(std::invalid_argument,
			"dfa_class=%s don't support save_mmap", typeid(*this).name());
	}
	strcpy(header.dfa_class_name, meta->class_name);
	strcpy(header.magic, "nark-dfa-mmap");
	header.header_size = sizeof(header);
	header.magic_len = 13;
	header.version  = DFA_MmapHeader::current_version;
	header.is_dag   = this->m_is_dag;
	header.kv_delim = this->m_kv_delim;
	header.gnode_states = this->v_gnode_states();
	header.total_states = this->v_total_states();
	header.zpath_states = this->m_zpath_states;
	header.zpath_length = this->m_total_zpath_len;
	long need_free_mask = prepare_save_mmap(&header, dataPtrs);
	assert(13 == header.magic_len);
	assert(sizeof(header) == header.blocks[0].offset);
	assert(header.num_blocks <= DFA_MmapHeader::MAX_BLOCK_NUM);
	if (header.num_blocks > DFA_MmapHeader::MAX_BLOCK_NUM) {
		THROW_STD(logic_error
			, "Fatal: blockNum = %ld, DFA_MmapHeader::MAX_BLOCK_NUM = %d"
			, long(header.num_blocks), DFA_MmapHeader::MAX_BLOCK_NUM);
	}
	header.file_size = align_to_64(header.blocks[header.num_blocks-1].endpos());
	return need_free_mask;
}

static
void Do_save_mmap(const BaseDFA* dfa, int fd,
				  function<void(size_t toWrite, size_t written)> Throw) {
	auto write = [&](const void* vdata, size_t size) {
		byte_t const* data = (byte_t const*)(vdata);
		size_t written = 0;
		while (written < size) {
			size_t len1 = std::min(size - written, size_t(1)<<30);
			intptr_t len2 = ::write(fd, data + written, len1);
			if (terark_likely(len2 > 0)) {
				written += len2;
			}
			else {
				Throw(len1, len2);
			}
		}
	};
	dfa->save_mmap(ref(write));
#if defined(_MSC_VER)
	if (!::FlushFileBuffers((HANDLE)_get_osfhandle(fd))) {
		DWORD err = GetLastError();
		THROW_STD(runtime_error, "FlushFileBuffers().ErrCode=%d(%X)", err, err);
	}
#else
	if (::fsync(fd) < 0) {
		THROW_STD(runtime_error, "fsync(fd=%d) = %s", fd, strerror(errno));
	}
#endif
}

void BaseDFA::save_mmap(int fd) const {
	auto Throw = [fd](size_t toWrite, size_t written) {
		THROW_STD(length_error
			, "write(fd=%d, toWrite=%zd) = %zd : %s"
			, fd, toWrite, written, strerror(errno));
	};
	Do_save_mmap(this, fd, ref(Throw));
}

void BaseDFA::save_mmap(function<void(const void*, size_t)> write)
const {
	const void* dataPtrs[DFA_MmapHeader::MAX_BLOCK_NUM] = { NULL };
	DFA_MmapHeader header;
	long need_free_mask = stat_impl(&header, dataPtrs);
	TERARK_SCOPE_EXIT(
		free_save_mmap_ptrs(header.num_blocks, dataPtrs, need_free_mask);
	);
	header.crc32cLevel = 2;
	header.file_crc32 = 0;
	for(size_t i = 0, n = header.num_blocks; i < n; ++i) {
		const byte_t* data = reinterpret_cast<const byte_t*>(dataPtrs[i]);
		const size_t  nlen = header.blocks[i].length;
		header.file_crc32 = Crc32c_update(header.file_crc32, data, nlen);
		if (nlen % 64 != 0) {
			static const char zeros[64] = {0};
			header.file_crc32 =	Crc32c_update(header.file_crc32, zeros, 64-nlen%64);
		}
	}
	header.header_crc32 = Crc32c_update(0, &header, sizeof(header)-4);
	write(&header, sizeof(header));
	for(size_t i = 0, n = header.num_blocks; i < n; ++i) {
		assert(header.blocks[i].offset % 64 == 0);
		write(dataPtrs[i], header.blocks[i].length);
		if (header.blocks[i].length % 64 != 0) {
			char zeros[64];
			memset(zeros, 0, sizeof(zeros));
			write(zeros, 64 - header.blocks[i].length % 64);
		}
	}
}

BaseDFA* BaseDFA::load_mmap(fstring fname) {
	bool populate = getEnvBool("DFA_MAP_POPULATE", false);
	return load_mmap(fname, populate);
}

BaseDFA* BaseDFA::load_mmap(fstring fstrPathName, bool mmapPopulate) {
#if defined(_MSC_VER)
	// for windows FILE_SHARE_DELETE
	bool writable = false;
	size_t fsize = 0;
	void* base = mmap_load(fstrPathName, &fsize, writable, mmapPopulate);
	BaseDFA* dfa = load_mmap_fmt((DFA_MmapHeader*)base);
	dfa->m_mmap_type = DFA_MmapType::is_mmap;
	return dfa;
#else
	const char* fname = fstrPathName.c_str();
	terark::Auto_close_fd fd(::open(fname, O_RDONLY));
	if (fd < 0) {
		THROW_STD(runtime_error, "error: open(%s, O_RDONLY) = %s", fname, strerror(errno));
	}
	return load_mmap(fd, mmapPopulate);
#endif
}

void BaseDFA::save_mmap(fstring fstrPathName) const {
	terark::Auto_close_fd fd;
	const char* fname = fstrPathName.c_str();
	fd = ::open(fname, O_CREAT|O_TRUNC|O_WRONLY|O_LARGEFILE|O_FORCE_BINARY, 0644);
	if (fd < 0) {
		TERARK_THROW(std::runtime_error,
			"error: open(%s, O_CREAT|O_TRUNC|O_WRONLY|O_LARGEFILE|O_FORCE_BINARY, 0644) = %s", fname, strerror(errno));
	}
	auto Throw = [&](size_t toWrite, size_t written) {
		THROW_STD(length_error
			, "write(file=%s, toWrite=%zd) = %zd : %s"
			, fname, toWrite, written, strerror(errno));
	};
	Do_save_mmap(this, fd, ref(Throw));
}

fstring BaseDFA::get_mmap() const {
	return fstring((byte_t*)mmap_base, mmap_base->file_size);
}

size_t BaseDFA::zp_nest_level() const {
	return 0;
}

/////////////////////////////////////////////////////////////////////////

BaseDFA* BaseDFA::load_from(fstring fname) {
	assert('\0' == *fname.end());
	{
		terark::FileStream file(fname.p, "rb");
		return load_from(file);
	}
}

BaseDFA*
BaseDFA::load_from_NativeInputBuffer(void* nativeInputBufferObject) {
	auto& dio = *(NativeDataInput<InputBuffer>*)nativeInputBufferObject;
	std::string className;
	dio >> className;
	const DFA_ClassMetaInfo* meta = DFA_ClassMetaInfo::find(className);
	if (meta) {
		std::unique_ptr<BaseDFA> dfa(meta->create());
		meta->load(dio, &*dfa);
		return dfa.release();
	}
	if (strcmp(className.c_str(), "nark-dfa-mmap") == 0) {
		BaseDFA* dfa = NULL;
		IInputStream* stream = dio.getInputStream();
		if (stream) {
			if (FileStream* fp = dynamic_cast<FileStream*>(stream)) {
			#ifdef _MSC_VER
				if (_setmode(_fileno(*fp), _O_BINARY) < 0) {
					THROW_STD(invalid_argument, "setmode(fileno(fp))");
				}
			#endif
				try {
					dfa = load_mmap(::fileno(*fp));
				}
				catch (const std::exception&) {
					fprintf(stderr, "Warning: mmap failed, trying to load by stream read\n");
				}
			}
		} else {
			fprintf(stderr, "Warning: in %s: IInputStream is not a FileStream\n"
					, BOOST_CURRENT_FUNCTION);
		}
		if (NULL == dfa) {
			terark::AutoFree<DFA_MmapHeader> base(1);
			base->magic_len = 13;
			memcpy(base->magic, "nark-dfa-mmap", base->magic_len);
			int remain_header_len = sizeof(DFA_MmapHeader) - base->magic_len - 1;
			dio.ensureRead(base->magic + base->magic_len, remain_header_len);
			DFA_MmapHeader* q = (DFA_MmapHeader*)realloc(base, base->file_size);
			if (NULL == q) {
				THROW_STD(runtime_error, "malloc FileSize=%lld (including DFA_MmapHeader)"
						, (long long)base->file_size);
			}
			base.p = q;
			dio.ensureRead(q+1, q->file_size - sizeof(DFA_MmapHeader));
			dfa = load_mmap_fmt(q);
			dfa->m_mmap_type = DFA_MmapType::is_malloc;
			base.p = NULL; // release ownership
		}
		return dfa;
	}
	TERARK_THROW(std::invalid_argument, ": unknown dfa_class: %s", className.c_str());
}

BaseDFA* BaseDFA::load_from(FILE* fp) {
	NonOwnerFileStream file(fp);
	file.disbuf();
	InputBuffer dio(&file);
	return load_from_NativeInputBuffer(&dio);
}

void BaseDFA::save_to(fstring fname) const {
	assert('\0' == *fname.end());
	terark::FileStream file(fname.p, "wb");
	save_to(file);
}
void BaseDFA::save_to(FILE* fp) const {
	NonOwnerFileStream file(fp);
	file.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&file);
	const DFA_ClassMetaInfo* meta = DFA_ClassMetaInfo::find(this);
	if (meta) {
		dio << fstring(meta->class_name);
		meta->save(dio, this);
	}
	else {
		TERARK_THROW(std::invalid_argument,
			": unknown dfa_class: %s", typeid(*this).name());
	}
}

void BaseDFA::risk_swap(BaseDFA& y) {
#define DO_SWAP(f) { auto t = f; f = y.f; y.f = t; }
   	DO_SWAP(mmap_base);
   	DO_SWAP(m_kv_delim);
   	DO_SWAP(m_is_dag);
   	DO_SWAP(m_mmap_type);
   	DO_SWAP(m_dyn_sigma);
   	DO_SWAP(m_zpath_states);
   	DO_SWAP(m_total_zpath_len);
    DO_SWAP(m_adfa_total_words_len);
#undef DO_SWAP
}

void BaseDFA::get_stat(DFA_MmapHeader* st) const {
	const void* dataPtrs[DFA_MmapHeader::MAX_BLOCK_NUM] = { NULL };
	long need_free_mask = stat_impl(st, dataPtrs);
	free_save_mmap_ptrs(st->num_blocks, dataPtrs, need_free_mask);
}

void BaseDFA::str_stat(std::string* st) const {
}

/// output @param keys are unsorted
void BaseDFA::dfa_get_random_keys_append(SortableStrVec* keys, size_t num) const {
    std::mt19937 rnd(keys->size() + keys->str_size() + num);
    assert(m_dyn_sigma >= 256);
    AutoFree<CharTarget<size_t> > children(m_dyn_sigma);
    valvec<byte_t> key(256, valvec_reserve());
    MatchContext ctx;
    // this algo is likely to select short keys, it's not uniform distributed
    for(size_t i = 0; i < num; ++i) {
        size_t s = initial_state;
        key.erase_all();
        while (true) {
            if (v_is_pzip(s)) {
                key.append(v_get_zpath_data(s, &ctx));
            }
            size_t nc = get_all_move(s, children.p);
            size_t rd = rnd();
            size_t k;
            if (v_is_term(s)) {
                if (0 == nc)
                    break; // done
                k = rd % (nc + 1);
                if (0 == k) { // select curr word
                    break; // done
                }
                k--;
            }
            else if (nc) {
                k = rd % nc;
            }
            else {
                assert(false); // when 0 == nc, state must be term
                k = 0; // to shut up compiler warn
            }
            key.push_back(byte_t(children.p[k].ch));
            s = children.p[k].target;
            assert(s < v_total_states());
        }
        keys->push_back(key);
        keys->m_index.back().seq_id = s; // 2^36, up to 64G
    }
}

void BaseDFA::dfa_get_random_keys(SortableStrVec* keys, size_t num) const {
    keys->m_index.erase_all();
    keys->m_strpool.erase_all();
    dfa_get_random_keys_append(keys, num);
}

size_t BaseDFA::find_first_leaf(size_t root) const {
	valvec<size_t> stack(512, valvec_reserve());
	terark::AutoFree<size_t> children(m_dyn_sigma);
	stack.push_back(root);
	while (!stack.empty()) {
		size_t parent = stack.back(); stack.pop_back();
		if (get_all_dest(parent, children)) {
			// has at least one child
			stack.push_back(children[0]);
		} else {
			return parent;
		}
	}
	return size_t(-1);
}

/// @returns number of children
///   children pushed to stack can be obtained by (stack->size() - oldsize)
///   in the caller
size_t BaseDFA::pfs_put_children
(size_t parent, febitvec& color, valvec<size_t>& stack, size_t* children_buf)
const {
	size_t cnt = this->get_all_dest(parent, children_buf);
	for(size_t k = cnt; k > 0; ) {
		size_t child = children_buf[--k];
		if (color.is0(child)) {
			color.set1(child);
			stack.push(child);
		}
	}
	return cnt;
}


MatchingDFA* MatchingDFA::load_from(FILE* fp) {
	BaseDFA* dfa = BaseDFA::load_from(fp);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "dfa is not a MatchingDFA");
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_from(fstring fname) {
	BaseDFA* dfa = BaseDFA::load_from(fname);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "file:%.*s is not a MatchingDFA",
				fname.ilen(), fname.data());
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_mmap(int fd) {
	BaseDFA* dfa = BaseDFA::load_mmap(fd);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "dfa is not a MatchingDFA");
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_mmap(fstring fname) {
	BaseDFA* dfa = BaseDFA::load_mmap(fname);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "file:%s is not a MatchingDFA", fname.c_str());
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_mmap(int fd, bool mmapPopulate) {
	BaseDFA* dfa = BaseDFA::load_mmap(fd, mmapPopulate);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "dfa is not a MatchingDFA");
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_mmap(fstring fname, bool mmapPopulate) {
	BaseDFA* dfa = BaseDFA::load_mmap(fname, mmapPopulate);
	if (dfa) {
		if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa))
			return p;
		else {
			delete dfa;
			THROW_STD(invalid_argument, "file:%s is not a MatchingDFA", fname.c_str());
		}
	}
	return NULL;
}

MatchingDFA* MatchingDFA::load_mmap_user_mem(const void* baseptr, size_t length) {
	BaseDFA* dfa = BaseDFA::load_mmap_user_mem(baseptr, length);
	assert(nullptr != dfa);
	if (MatchingDFA* p = dynamic_cast<MatchingDFA*>(dfa)) {
		return p;
	}
	delete dfa;
	THROW_STD(invalid_argument, "dfa is not a MatchingDFA");
}

//#include <cxxabi.h>
// __cxa_demangle(const char* __mangled_name, char* __output_buffer, size_t* __length, int* __status);

/////////////////////////////////////////////////////////
//
DFA_MutationInterface::DFA_MutationInterface() {}
DFA_MutationInterface::~DFA_MutationInterface() {}

// this all-O(n) maybe a slow implement
// a faster override maybe best-O(1) and worst-O(n)
void DFA_MutationInterface::del_move(size_t s, auchar_t ch) {
	CharTarget<size_t> moves[512]; // max possible sigma
//	printf("del_move------------: c=%c:%02X\n", ch, ch);
	size_t n = get_BaseDFA()->get_all_move(s, moves);
	size_t i = 0;
	for (size_t j = 0; j < n; ++j) {
		if (moves[j].ch != ch)
			moves[i++] = moves[j];
	}
	del_all_move(s);
	add_all_move(s, moves, i);
}

/////////////////////////////////////////////////////////
//
const BaseDAWG* BaseDFA::get_dawg() const {
	return NULL;
}

BaseDAWG::BaseDAWG() {
	n_words = 0;
}
BaseDAWG::~BaseDAWG() {}

static thread_local MatchContext g_fsa_ctx;

size_t BaseDAWG::index(fstring word) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return index(ctx, word);
}

void BaseDAWG::lower_bound(fstring word, size_t* index, size_t* dict_rank) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
    return lower_bound(ctx, word, index, dict_rank);
}

std::string
BaseDAWG::nth_word(size_t nth) const {
	if (nth >= n_words) {
		THROW_STD(out_of_range, "nth=%zd n_words=%zd", nth, n_words);
	}
	std::string word;
	nth_word(nth, &word);
	return word;
}
void
BaseDAWG::nth_word(size_t nth, std::string* word) const {
	if (nth >= n_words) {
		THROW_STD(out_of_range, "nth=%zd n_words=%zd", nth, n_words);
	}
	word->resize(0);
	MatchContext ctx;
	nth_word(ctx, nth, word);
}

DawgIndexIter BaseDAWG::dawg_lower_bound(fstring str) const {
	MatchContext ctx;
	return dawg_lower_bound(ctx, str);
}

size_t BaseDAWG::
match_dawg(fstring str, const OnMatchDAWG& f) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg(ctx, 0, str, f);
}
size_t BaseDAWG::
match_dawg(fstring str, const OnMatchDAWG& f, const ByteTR& tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg(ctx, 0, str, f, tr);
}
size_t BaseDAWG::
match_dawg(fstring str, const OnMatchDAWG& f, const byte_t* tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg(ctx, 0, str, f, tr);
}

size_t BaseDAWG::
match_dawg_l(fstring str, const OnMatchDAWG& f) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	size_t len, nth;
	if (match_dawg_l(ctx, str, &len, &nth)) f(len, nth);
	return ctx.pos;
}
size_t BaseDAWG::
match_dawg_l(fstring str, const OnMatchDAWG& f, const ByteTR& tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	size_t len, nth;
	if (match_dawg_l(ctx, str, &len, &nth, tr)) f(len, nth);
	return ctx.pos;
}
size_t BaseDAWG::
match_dawg_l(fstring str, const OnMatchDAWG& f, const byte_t* tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	size_t len, nth;
	if (match_dawg_l(ctx, str, &len, &nth, tr)) f(len, nth);
	return ctx.pos;
}

bool BaseDAWG::
match_dawg_l(fstring str, size_t* len, size_t* nth) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg_l(ctx, str, len, nth);
}
bool BaseDAWG::
match_dawg_l(fstring str, size_t* len, size_t* nth, const ByteTR& tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg_l(ctx, str, len, nth, tr);
}
bool BaseDAWG::
match_dawg_l(fstring str, size_t* len, size_t* nth, const byte_t* tr) const {
    MatchContext& ctx = g_fsa_ctx;
    ctx.reset();
	return match_dawg_l(ctx, str, len, nth, tr);
}

void BaseDAWG::lower_bound(MatchContext& ctx, fstring word, size_t* index, size_t* dict_rank) const {
    THROW_STD(invalid_argument, "this method should not be called");
}

size_t BaseDAWG::v_state_to_word_id(size_t state) const {
	THROW_STD(invalid_argument, "this method should not be called");
}

size_t BaseDAWG::state_to_dict_rank(size_t state) const {
	THROW_STD(invalid_argument, "this method should not be called");
}

void BaseDAWG::get_random_keys_append(SortableStrVec* keys, size_t num) const {
    size_t nWords = this->n_words;
    size_t seed = keys->size() + keys->str_size() + num;
    std::mt19937_64 rnd(seed);
    std::string word;
    for(size_t i = 0; i < num; ++i) {
        size_t k = rnd() % nWords;
        nth_word(k, &word);
        keys->push_back(word);
        keys->m_index.back().seq_id = k;
    }
}

void BaseDAWG::get_random_keys(SortableStrVec* keys, size_t num) const {
    keys->clear();
    get_random_keys_append(keys, num);
}


} // namespace terark
