#include "factory.hpp"
#include <terark/hash_strmap.hpp>
#include <boost/current_function.hpp>

namespace terark {

template<class ProductPtr, class... CreatorArgs>
struct Factoryable<ProductPtr, CreatorArgs...>::AutoReg::Impl {
    typedef hash_strmap<
        function<ProductPtr(CreatorArgs...)>,
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

template<class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::
AutoReg::AutoReg(fstring name,
		 function<ProductPtr(CreatorArgs...)> creator) {
    auto& rmap = Impl::s_get_regmap();
    std::pair<size_t, bool> ib = rmap.insert_i(name, creator);
    if (!ib.second) {
        fprintf(stderr, "ERROR: %s: duplicate name = %.*s\n", BOOST_CURRENT_FUNCTION, name.ilen(), name.p);
    }
}

template<class ProductPtr, class... CreatorArgs>
ProductPtr Factoryable<ProductPtr, CreatorArgs...>::
create(fstring name, CreatorArgs... args) {
    auto& rmap = AutoReg::Impl::s_get_regmap();
    size_t i = rmap.find_i(name);
    if (rmap.end_i() != i) {
        auto& creator = rmap.val(i);
        return creator(args...);
    }
    return nullptr;
}

template<class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::~Factoryable() {
}

} // namespace terark

/// ---- user land ----

///@param ProductPtr allowing template product, such as
/// TERARK_FACTORY_INSTANTIATE(SomeProduct<T1, T2, T3>, CreatorArg1...)
///@note this macro must be called in namespace terark
#define TERARK_FACTORY_INSTANTIATE(ProductPtr, ...) \
    template class Factoryable<ProductPtr, ##__VA_ARGS__ >

///@note this macro must be called in global namespace
#define TERARK_FACTORY_INSTANTIATE_GNS(ProductPtr, ...) \
    namespace terark { TERARK_FACTORY_INSTANTIATE(ProductPtr, ##__VA_ARGS__); }

