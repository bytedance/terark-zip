// created by leipeng 2019-12-13

#pragma once
#include "function.hpp"
#include <terark/fstring.hpp>
#include <terark/preproc.hpp>

namespace terark {

///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being factoryable:
/// class SomeFactory : public Factoyable<SomeFactory> {
///    ...
/// };
template<class Product, class... CreatorArgs>
class Factoryable {
public:
    virtual ~Factoryable();
    static Product* create(fstring name, CreatorArgs...);
    struct AutoReg {
        AutoReg(fstring name, function<Product*(CreatorArgs...)> creator);
        struct Impl;
    };
};

///@param VarID var identifier
///@param Name string of factory name
///@param Creator
///@param Product ... can be template such as SomeProduct<T1, T2, T3>
///@note if Name has some non-var char, such as "-,." ...
///         must use TERARK_FACTORY_REGISTER_EX to set an VarID
#define TERARK_FACTORY_REGISTER_EX(VarID, Name, Creator, Product, ...) \
  TERARK_PP_IDENTITY(Product,##__VA_ARGS__)::AutoReg \
    TERARK_PP_CAT(g_reg_factory_, VarID, __LINE__)(Name, Creator)

#define TERARK_FACTORY_REGISTER(Name, Creator) \
        TERARK_FACTORY_REGISTER_EX(Name, TERARK_PP_STR(Name), Creator, Name)

} // namespace terark
