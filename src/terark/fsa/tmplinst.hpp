#ifndef __terark_fsa_tmplinst_hpp__
#define __terark_fsa_tmplinst_hpp__

#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <boost/preprocessor/cat.hpp>
#include "fsa.hpp"

namespace terark {

class TERARK_DLL_EXPORT DFA_ClassMetaInfo {
public:
	// the class name in serialization data
	const char* class_name;
	std::string rtti_class_name;

	virtual ~DFA_ClassMetaInfo();

	virtual BaseDFA* create() const = 0;
	virtual void load(NativeDataInput<InputBuffer>& dio, BaseDFA*) const = 0;
	virtual void save(NativeDataOutput<OutputBuffer>& dio, const BaseDFA*) const = 0;

	void register_me(const char* class_name, const char* rtti_class_name);

	static const DFA_ClassMetaInfo* find(fstring class_name);
	static const DFA_ClassMetaInfo* find(const BaseDFA*);
};

template<class DFA>
class DFA_ClassMetaInfoInst : public DFA_ClassMetaInfo {
public:
	DFA_ClassMetaInfoInst(const char* class_name, const char* rtti_class_name) {
		this->register_me(class_name, rtti_class_name);
	}
	BaseDFA* create() const {
		return new DFA;
	}
	void load(NativeDataInput<InputBuffer>& dio, BaseDFA* dfa) const {
		assert(NULL != dfa);
		dio >> *static_cast<DFA*>(dfa);
	}
	void save(NativeDataOutput<OutputBuffer>& dio, const BaseDFA* dfa) const {
		assert(NULL != dfa);
		dio << *static_cast<const DFA*>(dfa);
	}
};

// sizeof(DFA class name with \0) must <= 60 (DFA_MmapHeader::dfa_class_name)
#define TMPL_INST_DFA_CLASS(DFA) \
	BOOST_STATIC_ASSERT(sizeof(BOOST_STRINGIZE(DFA)) <= 60); \
	static DFA_ClassMetaInfoInst<DFA >  \
	  terark_used_static_obj            \
		BOOST_PP_CAT(gs_dfa_tmplinst, __LINE__)(BOOST_STRINGIZE(DFA), typeid(DFA).name());

} // namespace terark

#endif // __terark_fsa_tmplinst_hpp__
