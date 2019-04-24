#ifndef __terark_fsa_forward_decl_hpp__
#define __terark_fsa_forward_decl_hpp__

#include <stdio.h>
#include <memory>
#include <utility>
#include <terark/fstring.hpp>

namespace terark {

class BaseDFA;
class MatchingDFA;

struct TERARK_DLL_EXPORT BaseDFADeleter {
	void operator()(BaseDFA*) const;
	void operator()(MatchingDFA*) const;
};

typedef std::unique_ptr<BaseDFA, BaseDFADeleter> BaseDFAPtr;
typedef std::unique_ptr<MatchingDFA, BaseDFADeleter> MatchingDFAPtr;

TERARK_DLL_EXPORT BaseDFA* BaseDFA_load(fstring fname);
TERARK_DLL_EXPORT BaseDFA* BaseDFA_load(FILE*);

TERARK_DLL_EXPORT MatchingDFA* MatchingDFA_load(fstring fname);
TERARK_DLL_EXPORT MatchingDFA* MatchingDFA_load(FILE*);

} // namespace terark

#endif // __terark_fsa_forward_decl_hpp__

