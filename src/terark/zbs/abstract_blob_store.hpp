#pragma once
#include "blob_store.hpp"

namespace terark {

class SortableStrVec;
class ZReorderMap;
class LruReadonlyCache;

class TERARK_DLL_EXPORT AbstractBlobStore : public BlobStore {
public:
    struct TERARK_DLL_EXPORT Builder : public RefCounter {
        static Builder* createBuilder(fstring clazz, fstring outputFileName, fstring moreConfig);
        virtual ~Builder();
        virtual Builder* getPreBuilder() const;
        virtual void addRecord(fstring rec) = 0;
        virtual void finish() = 0;
    };
protected:
	std::string m_fpath;
    bool            m_isMmapData;
    bool            m_isUserMem;
    bool            m_isDetachMeta;
    MemoryCloseType m_dictCloseType;
	int             m_checksumLevel;
	int             m_checksumType;
	const struct FileHeaderBase* m_mmapBase;

	void risk_swap(AbstractBlobStore& y);

public:
	static AbstractBlobStore* load_from_mmap(fstring fpath, bool mmapPopulate);
	static AbstractBlobStore* load_from_user_memory(fstring dataMem);
	static AbstractBlobStore* load_from_user_memory(fstring dataMem, Dictionary dict);
    virtual void save_mmap(fstring fpath) const; // has default implementation
    virtual void save_mmap(function<void(const void*, size_t)> write) const = 0;

    const char* name() const override;
	void set_fpath(fstring fpath);
	fstring get_fpath() const;
	Dictionary get_dict() const override;
	fstring get_mmap() const override;

	AbstractBlobStore();
	virtual ~AbstractBlobStore();
    virtual void reorder_zip_data(ZReorderMap& newToOld,
        function<void(const void* data, size_t size)> writeAppend,
        fstring tmpFile) const = 0;
};

TERARK_DLL_EXPORT
AbstractBlobStore*
NestLoudsTrieBlobStore_build(fstring clazz, int nestLevel, SortableStrVec&);

} // namespace terark
