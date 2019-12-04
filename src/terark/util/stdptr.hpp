#pragma once
#include <memory>

namespace terark {

// for pre c++17

template<class T>
std::shared_ptr<T> as_shared_ptr(T* p) {
    return std::shared_ptr<T>(p);
}

template<class T, class D>
std::shared_ptr<T> as_shared_ptr(T* p, D d) {
    return std::shared_ptr<T>(p, d);
}

template<class T>
std::unique_ptr<T> as_unique_ptr(T* p) {
    return std::unique_ptr<T>(p);
}

template<class T, class D>
std::unique_ptr<T> as_unique_ptr(T* p, D d) {
    return std::unique_ptr<T>(p, d);
}


} // namespace terark

