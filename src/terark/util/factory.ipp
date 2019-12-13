#include "factory.hpp"
#include <terark/hash_strmap.hpp>
#include <boost/current_function.hpp>

namespace terark {

template<class Product, class... CreatorArgs>
struct Factoryable<Product, CreatorArgs...>::AutoReg::Impl {
    typedef hash_strmap<
        function<Product*(CreatorArgs...)>,
        fstring_func::IF_SP_ALIGN(hash_align, hash),
        fstring_func::IF_SP_ALIGN(equal_align, equal),
        ValueInline,
        SafeCopy // function is not memory movable on some stdlib
    > RegMap;
    static RegMap& s_get_regmap() {
        static RegMap rmap;
        return rmap;
    }
};

template<class Product, class... CreatorArgs>
Factoryable<Product>::
AutoReg::AutoReg(fstring name, function<Product*(CreatorArgs...)> creator) {
    Impl::RegMap& rmap = Impl::s_get_regmap();
    std::pair<size_t, bool> ib = rmap.insert_i(name, creator);
    if (!ib.second) {
        fprintf(stderr, "ERROR: %s: duplicate name = %.*s\n", name.ilen(), name.p);
    }
}

template<class Product, class... CreatorArgs>
Product* Factoryable<Product>::create(fstring name, CreatorArgs... args) {
    auto& rmap = AutoReg::Impl::s_get_regmap();
    size_t i = rmap.find_i(name);
    if (rmap.end_i() != i) {
        auto& creator = rmap.val(i);
        return creator(args...);
    }
    return nullptr;
}

} // namespace terark

/// ---- user land ----

///@param Product allowing template product, such as
/// TERARK_FACTORY_INSTANTIATE(SomeProduct<T1, T2, T3>, CreatorArg1...)
#define TERARK_FACTORY_INSTANTIATE(Product, ...) \
    template class terark::Factoryable<Product, ##__VA_ARGS__ >

