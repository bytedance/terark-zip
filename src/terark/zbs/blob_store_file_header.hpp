#pragma once
#include <terark/util/function.hpp>
#include <terark/stdtypes.hpp>
#include <boost/static_assert.hpp>
#include <terark/fstring.hpp>
#include <terark/io/IStream.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace terark {

#define My_bsr_size_t TERARK_IF_WORD_BITS_64(terark_bsr_u64, terark_bsr_u32)

TERARK_DLL_EXPORT bool isChecksumVerifyEnabled();

template<size_t Align, class File>
void PadzeroForAlign(File& f, size_t offset) {
	if (offset % Align != 0) {
		const static char zeros[Align] = {0};
		f.ensureWrite(zeros, Align - offset % Align);
	}
}
template<size_t Align, class File, class Hash>
void PadzeroForAlign(File& f, Hash& h, size_t offset) {
  if (offset % Align != 0) {
    const static char zeros[Align] = { 0 };
    const size_t zeroLen = Align - offset % Align;
    h.update(zeros, zeroLen);
    f.ensureWrite(zeros, zeroLen);
  }
}

#pragma pack(push,8)
// Compatible with DFA_MmapHeader
static const char   MagicString[] = "terark-blob-store";
static const size_t MagicStrLen   = sizeof(MagicString)-1; // 17

enum ChecksumType : uint8_t {
    kCRC32C = 0,
    kCRC16C = 1
};

struct FileHeaderBase {
	uint8_t  magic_len;
	char     magic[19];
	char     className[20]; // SimpleZipBlobStore or DictZipBlobStore or MixedLenBlobStore etc.

	uint64_t fileSize;
	uint64_t unzipSize;

	uint64_t records        : 40;
	uint64_t checksumType   :  8;
	uint64_t formatVersion  : 16;

	uint64_t globalDictSize : 40;
	uint64_t pad12          : 24;

	uint64_t pad13;
};
#pragma pack(pop)

BOOST_STATIC_ASSERT(sizeof(FileHeaderBase) == 80);


struct BlobStoreFileFooter {
  uint64_t zipDataXXHash;
  uint64_t fileXXHash;
  uint64_t reserved1[5];
  uint32_t pad9;
  uint32_t footerLength; // must be the last field
  BlobStoreFileFooter() {
    reset();
  }
  void reset() {
    memset(this, 0, sizeof(BlobStoreFileFooter));
    footerLength = sizeof(BlobStoreFileFooter);
  }
};

BOOST_STATIC_ASSERT(sizeof(BlobStoreFileFooter) == 64);

class AbstractBlobStore;

class RegisterBlobStore {
public:
  typedef AbstractBlobStore* (*Factory)();
  struct RegisterFactory {
    RegisterFactory(std::initializer_list<fstring> names, Factory);
  };
#define REGISTER_BlobStore(Clazz, ...) \
	static RegisterBlobStore::RegisterFactory \
    s_reg##Clazz({__VA_ARGS__}, \
		[]() -> AbstractBlobStore* { \
			return new Clazz(); \
		})
};

class FunctionAdaptBuffer : public IOutputStream {
public:
  explicit
  FunctionAdaptBuffer(function<void(const void* data, size_t size)> f);
  size_t write(const void* vbuf, size_t length) override;
  void flush() override;
private:
  function<void(const void* data, size_t size)> f_;
};

} // namespace terark
