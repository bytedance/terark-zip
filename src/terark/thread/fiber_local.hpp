//
// Created by leipeng on 2019-08-22.
//

#include <terark/valvec.hpp>

namespace terark {

    template<class T>
    class recycle_pool {
        valvec<T> m_free;
        static_assert(std::is_move_constructible<valvec<T> >::value, "valvec<T> must be move constructible");
        static_assert(std::is_move_constructible<T>::value, "T must be move constructible");
    public:
        T get() {
            if (m_free.size()) {
                return m_free.pop_val();
            }
            else {
                return T();
            }
        }
        void put(T&& p) {
            m_free.emplace_back(std::move(p));
        }
    };

}
