#pragma once

#include <terark/config.hpp>
#include <stdio.h>

namespace terark {

class TERARK_DLL_EXPORT FSA_Cache {
public:
	virtual ~FSA_Cache();
	virtual bool has_fsa_cache() const = 0;
	virtual bool build_fsa_cache(double cacheRatio, const char* walkMethod)=0;
	virtual void print_fsa_cache_stat(FILE*) const = 0;
};

class NTD_CacheTrie; // forward declaration

} // namespace terark

