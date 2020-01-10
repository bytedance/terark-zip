#include "factory.hpp"
//#include <terark/hash_strmap.hpp>
#include <terark/gold_hash_map.hpp>
#include <boost/current_function.hpp>
#include <mutex>

namespace terark {

template<class ProductPtr, class... CreatorArgs>
struct Factoryable<ProductPtr, CreatorArgs...>::AutoReg::Impl {
    using NameToFuncMap = gold_hash_map_p<fstring, CreatorFunc>;
    using TypeToNameMap = gold_hash_map<std::type_index, fstring>;

    NameToFuncMap func_map;
    TypeToNameMap type_map;
    std::mutex    mtx;

    static Impl& s_singleton() { static Impl imp; return imp; }
};

//#define TERARK_FACTORY_WARN_ON_DUP_NAME

template<class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::
AutoReg::AutoReg(fstring name, CreatorFunc creator, const std::type_info& ti)
  : m_name(name), m_type_idx(ti)
{
    auto& imp = Impl::s_singleton();
    auto& func_map = imp.func_map.get_map();
    CreatorFunc* pFunc = new CreatorFunc(std::move(creator));
    imp.mtx.lock();
    std::pair<size_t, bool> ib = func_map.insert_i(name, pFunc);
    if (!ib.second) {
        fprintf(stderr, "FATAL: %s: duplicate name = %.*s\n"
            , BOOST_CURRENT_FUNCTION, name.ilen(), name.p);
        abort();
    }
    ib = imp.type_map.insert_i(ti, name);
    if (!ib.second) {
#if defined(TERARK_FACTORY_WARN_ON_DUP_NAME)
        fstring oldname = imp.type_map.val(ib.first);
        fprintf(stderr
            , "WARN: %s: dup name: {old=\"%.*s\", new=\"%.*s\"} "
                "for type: %s, new name ignored\n"
            , BOOST_CURRENT_FUNCTION
            , oldname.ilen(), oldname.p, name.ilen(), name.p
            , ti.name());
#endif
    }
    imp.mtx.unlock();
}

template<class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::
AutoReg::~AutoReg() {
    auto& imp = Impl::s_singleton();
    imp.mtx.lock();
    size_t cnt1 = imp.func_map.erase(m_name);
    size_t cnt2 = imp.type_map.erase(m_type_idx);
    if (0 == cnt1) {
        fprintf(stderr, "FATAL: %s: name = %.*s to creator not found\n"
            , BOOST_CURRENT_FUNCTION, DOT_STAR_S(m_name));
        abort();
    }
    if (0 == cnt2) {
#if defined(TERARK_FACTORY_WARN_ON_DUP_NAME)
        fprintf(stderr, "WARN: %s: type = %s to name not found, ignored\n"
            , BOOST_CURRENT_FUNCTION, m_type_idx.name());
#endif
    }
    imp.mtx.unlock();
}

template<class ProductPtr, class... CreatorArgs>
ProductPtr Factoryable<ProductPtr, CreatorArgs...>::
create(fstring name, CreatorArgs... args) {
    auto& imp = AutoReg::Impl::s_singleton();
    auto& func_map = imp.func_map.get_map();
    imp.mtx.lock();
    size_t i = func_map.find_i(name);
    if (func_map.end_i() != i) {
        typename AutoReg::CreatorFunc* creator = func_map.val(i);
        imp.mtx.unlock();
        return (*creator)(args...);
    }
    else {
        imp.mtx.unlock();
        return nullptr;
    }
}

template<class ProductPtr, class... CreatorArgs>
fstring Factoryable<ProductPtr, CreatorArgs...>::reg_name() const {
    auto& imp = AutoReg::Impl::s_singleton();
    fstring name;
    imp.mtx.lock();
    size_t idx = imp.type_map.find_i(std::type_index(typeid(*this)));
    if (imp.type_map.end_i() != idx) {
        name = imp.type_map.val(idx);
    }
    imp.mtx.unlock();
    return name;
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

