#include "function.hpp"

namespace terark {

UserMemPool::UserMemPool() {}
UserMemPool::~UserMemPool() {}

void* UserMemPool::alloc(size_t len) {
    return ::malloc(len);
}

void* UserMemPool::realloc(void* ptr, size_t len) {
    return ::realloc(ptr, len);
}

void  UserMemPool::sfree(void* ptr, size_t) {
    return ::free(ptr);
}

UserMemPool* UserMemPool::SysMemPool() {
    static UserMemPool sysMem;
    return &sysMem;
}

} // namespace terark
