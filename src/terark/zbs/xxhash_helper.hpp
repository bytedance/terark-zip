#pragma once
#include <terark/fstring.hpp>
#include <zstd/common/xxhash.h>

namespace terark {

class XXHash64 {
	XXH64_state_t* xxhstate;
public:
	explicit XXHash64(uint64_t seed) {
		auto xxhs = XXH64_createState();
		if (NULL == xxhs) {
			throw std::bad_alloc();
		}
		XXH64_reset(xxhs, seed);
		xxhstate = xxhs;
	}
	XXHash64(const XXHash64& y) {
		auto xxhs = XXH64_createState();
		if (NULL == xxhs) {
			throw std::bad_alloc();
		}
		XXH64_copyState(xxhs, y.xxhstate);
		xxhstate = xxhs;
	}
	XXHash64& operator=(const XXHash64& y) {
		XXH64_copyState(xxhstate, y.xxhstate);
		return *this;
	}
	~XXHash64() {
		XXH64_freeState(xxhstate);
	}
	XXHash64& reset(uint64_t seed) {
		XXH64_reset(xxhstate, seed);
		return *this;
	}
	XXHash64& update(const void* data, size_t len) {
		XXH64_update(xxhstate, data, len);
		return *this;
	}
	XXHash64& update(fstring data) {
		XXH64_update(xxhstate, data.data(), data.size());
		return *this;
	}
	uint64_t digest() const {
		return XXH64_digest(xxhstate);
	}
	uint64_t operator()(const void* data, size_t len) {
		XXH64_update(xxhstate, data, len);
		return XXH64_digest(xxhstate);
	}
	uint64_t operator()(fstring data) {
		return (*this)(data.data(), data.size());
	}
};

inline uint64_t XXH64(fstring data, uint64_t seed) {
	return ::XXH64(data.data(), data.size(), seed);
}

} // namespace terark

